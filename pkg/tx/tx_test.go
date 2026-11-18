package tx_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	container "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	. "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/lang"
	. "github.com/BeamFoundry/rays/pkg/testing"
)

type customLabeledError struct {
	label string
}

func (e *customLabeledError) Error() string {
	return "custom labeled error"
}

func (e *customLabeledError) HasErrorLabel(label string) bool {
	return e.label == label
}

type mockCommitErrSession struct {
	mongo.Session
	commitErr error
}

func (m *mockCommitErrSession) CommitTransaction(context.Context) error {
	return m.commitErr
}

type dummyLogSink struct{}

func (d *dummyLogSink) Info(int, string, ...any)    {}
func (d *dummyLogSink) Error(error, string, ...any) {}

type testTxLogger struct {
	Logging
	errors []string
	traces []string
}

func (l *testTxLogger) Error(msg string, args ...any) {
	l.errors = append(l.errors, fmt.Sprint(msg, args))
}

func (l *testTxLogger) Trace(msg string, args ...any) {
	l.traces = append(l.traces, fmt.Sprint(msg, args))
}

var _ = Describe("Transaction and ClientBuilder", Ordered, func() {
	var (
		mongodbContainer *container.MongoDBContainer
		mongoHost        string
		mongoPort        int
		mongoURI         string
		client           *mongo.Client
	)

	BeforeAll(func() {
		ctx := context.Background()
		var err error
		mongodbContainer, err = container.Run(
			ctx,
			"mongo:8.2.7",
			container.WithReplicaSet("rs0"),
			testcontainers.WithLogger(GetLogger()),
		)
		Expect(err).NotTo(HaveOccurred())

		ip, err := mongodbContainer.Host(ctx)
		Expect(err).NotTo(HaveOccurred())
		port, err := mongodbContainer.MappedPort(ctx, "27017")
		Expect(err).NotTo(HaveOccurred())

		mongoHost = ip
		mongoPort = int(port.Num())
		mongoURI, err = mongodbContainer.ConnectionString(ctx)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterAll(func() {
		if mongodbContainer != nil {
			testcontainers.CleanupContainer(GinkgoTB(), mongodbContainer)
		}
	})

	BeforeEach(func() {
		var err Error
		client, err = NewMongoClientBuilder().WithURI(mongoURI).Build()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if client != nil {
			_ = client.Disconnect(context.Background())
		}
	})

	It("covers MongoClientBuilder URI construction, timeouts, and options", func() {
		// Default builder and empty host/port fallback
		b1 := NewMongoClientBuilder().WithHost("").WithPort(0)
		opts1 := b1.BuildOptions()
		Expect(opts1).NotTo(BeNil())
		Expect(opts1.GetURI()).To(Equal("mongodb://127.0.0.1:27017/"))

		// IPv6 host normalization with brackets
		bIPv6 := NewMongoClientBuilder().WithHost("[::1]").WithPort(27018)
		optsIPv6 := bIPv6.BuildOptions()
		Expect(optsIPv6.GetURI()).To(Equal("mongodb://[::1]:27018/"))

		// IPv6 host normalization without brackets
		bIPv6Raw := NewMongoClientBuilder().WithHost("::1").WithPort(27019)
		optsIPv6Raw := bIPv6Raw.BuildOptions()
		Expect(optsIPv6Raw.GetURI()).To(Equal("mongodb://[::1]:27019/"))

		// WithCredentials, DirectConnection(false), Min/MaxPoolSize, customizers
		customizerCalled := false
		bFull := NewMongoClientBuilder().
			WithHost(mongoHost).
			WithPort(mongoPort).
			WithCredentials("user", "pass", "admin").
			WithDirectConnection(false).
			WithMaxPoolSize(50).
			WithMinPoolSize(10).
			WithConnectTimeout(5 * time.Second).
			WithServerSelectionTimeout(5 * time.Second).
			WithLogSink(&dummyLogSink{}).
			WithCommandLogging(true).
			WithOptions(func(_ *options.ClientOptions) {
				customizerCalled = true
			})
		optsFull := bFull.BuildOptions()
		Expect(customizerCalled).To(BeTrue())
		Expect(optsFull.Auth).NotTo(BeNil())
		Expect(optsFull.Auth.Username).To(Equal("user"))
		Expect(optsFull.Direct).NotTo(BeNil())
		Expect(*optsFull.Direct).To(BeFalse())
		Expect(*optsFull.MaxPoolSize).To(Equal(uint64(50)))
		Expect(*optsFull.MinPoolSize).To(Equal(uint64(10)))
		Expect(optsFull.LoggerOptions).NotTo(BeNil())
		Expect(optsFull.LoggerOptions.ComponentLevels[options.LogComponentCommand]).To(Equal(options.LogLevelDebug))

		// Command logging disabled
		bCmdOff := NewMongoClientBuilder().WithLogSink(&dummyLogSink{}).WithCommandLogging(false)
		optsCmdOff := bCmdOff.BuildOptions()
		Expect(optsCmdOff.LoggerOptions.ComponentLevels[options.LogComponentCommand]).To(Equal(options.LogLevel(0)))

		// Options with nil customizer is safe
		bNilOpt := NewMongoClientBuilder().WithOptions(nil)
		Expect(bNilOpt).NotTo(BeNil())

		// URI with connectTimeoutMS preserves URI timeout
		bURI := NewMongoClientBuilder().WithURI("mongodb://127.0.0.1:27017/?connectTimeoutMS=2500")
		optsURI := bURI.BuildOptions()
		Expect(*optsURI.ConnectTimeout).To(Equal(2500 * time.Millisecond))
		Expect(NewMongoClientBuilder().ExportResolvedConnectTimeout(&options.ClientOptions{})).To(Equal(10 * time.Second))

		// Build with custom context
		customCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cCustom, err := NewMongoClientBuilder().WithURI(mongoURI).Build(customCtx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cCustom.Disconnect(context.Background())).NotTo(HaveOccurred())

		// BuildDatabase success and failure
		db, err := NewMongoClientBuilder().WithURI(mongoURI).BuildDatabase("test_tx_db")
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Name()).To(Equal("test_tx_db"))
		Expect(db.Client().Disconnect(context.Background())).NotTo(HaveOccurred())

		_, err = NewMongoClientBuilder().WithURI("://invalid-uri").BuildDatabase("bad")
		Expect(err).To(HaveOccurred())

		// Ping failure and disconnect cleanup
		unreachableURI := "mongodb://127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=50"
		_, err = NewMongoClientBuilder().
			WithURI(unreachableURI).
			WithConnectTimeout(50 * time.Millisecond).
			Build()
		Expect(err).To(HaveOccurred())
	})

	It("covers isUnknownTransactionCommitResult branches", func() {
		Expect(ExportIsUnknownTransactionCommitResult(nil)).To(BeFalse())
		Expect(ExportIsUnknownTransactionCommitResult(errors.New("other error"))).To(BeFalse())
		Expect(ExportIsUnknownTransactionCommitResult(errors.New("something UnknownTransactionCommitResult happened"))).To(BeTrue())

		cmdErr := mongo.CommandError{Labels: []string{"UnknownTransactionCommitResult"}}
		Expect(ExportIsUnknownTransactionCommitResult(cmdErr)).To(BeTrue())

		labeledErr := &customLabeledError{label: "UnknownTransactionCommitResult"}
		Expect(ExportIsUnknownTransactionCommitResult(labeledErr)).To(BeTrue())
	})

	It("covers MongoSession methods, context implementation, and lifecycle", func() {
		// Nil session receiver
		var nilSess *MongoSession
		Expect(nilSess.Context()).NotTo(BeNil())
		Expect(nilSess.Session()).To(BeNil())
		Expect(nilSess.Error()).NotTo(HaveOccurred())
		Expect(nilSess.Validate()).NotTo(HaveOccurred())
		Expect(nilSess.Commit()).To(HaveOccurred())
		Expect(nilSess.Rollback()).To(HaveOccurred())
		Expect(nilSess.Abort()).To(HaveOccurred())
		nilSess.End()

		// Ambient session (no driver session)
		var nilCtx context.Context
		ambient := NewMongoSession(nilCtx, nil)
		Expect(ambient.Context()).NotTo(BeNil())
		Expect(ambient.Session()).To(BeNil())
		Expect(ambient.Error()).NotTo(HaveOccurred())
		Expect(ambient.Validate()).NotTo(HaveOccurred())
		Expect(ambient.Commit()).To(HaveOccurred())
		Expect(ambient.Rollback()).To(HaveOccurred())
		ambient.End()

		// Context forwarding methods
		type testCtxKey string
		const testKey testCtxKey = "testKey"
		baseCtx, cancel := context.WithTimeout(context.WithValue(context.Background(), testKey, "testVal"), 5*time.Second)
		defer cancel()
		sCtx := NewMongoSession(baseCtx, nil)
		d, ok := sCtx.Deadline()
		Expect(ok).To(BeTrue())
		Expect(d).NotTo(BeZero())
		Expect(sCtx.Done()).NotTo(BeNil())
		Expect(sCtx.Err()).NotTo(HaveOccurred())
		Expect(sCtx.Value(testKey)).To(Equal("testVal"))

		// Session with driver session
		driverSess, err := client.StartSession()
		Expect(err).NotTo(HaveOccurred())
		ms := NewMongoSession(context.Background(), driverSess)
		Expect(ms.Session()).To(Equal(driverSess))
		// Rollback fails when transaction was not started
		Expect(ms.Rollback()).To(HaveOccurred())
		ms.End()

		// Session with started transaction: rollback and already finalized
		driverSess2, err := client.StartSession()
		Expect(err).NotTo(HaveOccurred())
		Expect(driverSess2.StartTransaction()).NotTo(HaveOccurred())
		ms2 := NewMongoSession(context.Background(), driverSess2)
		Expect(ms2.Rollback()).NotTo(HaveOccurred())
		Expect(ms2.Validate()).To(HaveOccurred()) // Already rolled back
		Expect(ms2.Commit()).To(HaveOccurred())   // Already finalized
		Expect(ms2.Rollback()).To(HaveOccurred()) // Already finalized
		ms2.End()

		// Session with started transaction: commit and already finalized
		driverSess3, err := client.StartSession()
		Expect(err).NotTo(HaveOccurred())
		Expect(driverSess3.StartTransaction()).NotTo(HaveOccurred())
		ms3 := NewMongoSession(context.Background(), driverSess3)
		Expect(ms3.Commit()).NotTo(HaveOccurred())
		Expect(ms3.Validate()).To(HaveOccurred()) // Already committed
		Expect(ms3.Commit()).To(HaveOccurred())   // Already finalized
		ms3.End()

		// SetError and Validate
		sSetErr := NewMongoSession(context.Background(), nil)
		sSetErr.SetError(NewError("CustomErr", "failed"))
		Expect(sSetErr.Validate()).To(HaveOccurred())

		// Mock session with UnknownTransactionCommitResult (retryable, not cached)
		rawSess, err := client.StartSession()
		Expect(err).NotTo(HaveOccurred())
		mockSess := &mockCommitErrSession{Session: rawSess, commitErr: errors.New("UnknownTransactionCommitResult")}
		msMock := NewMongoSession(context.Background(), mockSess)
		Expect(msMock.Commit()).To(HaveOccurred())
		Expect(msMock.Error()).NotTo(HaveOccurred()) // Not cached, retry allowed
		msMock.End()

		// Mock session with terminal non-retryable error (cached)
		rawSess2, err := client.StartSession()
		Expect(err).NotTo(HaveOccurred())
		mockSessTerminal := &mockCommitErrSession{Session: rawSess2, commitErr: errors.New("terminal commit error")}
		msMockTerminal := NewMongoSession(context.Background(), mockSessTerminal)
		Expect(msMockTerminal.Commit()).To(HaveOccurred())
		Expect(msMockTerminal.Error()).To(HaveOccurred())  // Cached
		Expect(msMockTerminal.Commit()).To(HaveOccurred()) // Returns cached error
		msMockTerminal.End()

		// Session with initial error
		disClient := &mongo.Client{}
		disMgr := NewMongoTransactionalManager(disClient, nil)
		disSess := disMgr.Start(context.Background())
		Expect(disSess.Error()).To(HaveOccurred())
		Expect(disSess.Validate()).To(HaveOccurred())
		Expect(disSess.Commit()).To(HaveOccurred())
		Expect(disSess.Rollback()).To(HaveOccurred())
		disMgr.End()
	})

	It("covers MongoTransactionalManager Start, Get, End, Transactional, Execute, and ExecuteWithResult", func() {
		logger := &testTxLogger{}
		mgr := NewMongoTransactionalManager(client, logger)

		// 1. Successful Transactional
		var nilCtx context.Context
		mgr.Transactional(nilCtx, func(sess *MongoSession) Error {
			Expect(sess).NotTo(BeNil())
			Expect(mgr.Get()).To(Equal(sess))
			coll := client.Database("test_tx_db").Collection("docs")
			_, err := coll.InsertOne(sess, bson.M{"msg": "success_tx"})
			Expect(err).NotTo(HaveOccurred())
			return nil
		})

		// 1b. Transactional with early rollback and action error (covers rollbackErr.LogError)
		mgr.Transactional(context.Background(), func(sess *MongoSession) Error {
			Expect(sess.Rollback()).NotTo(HaveOccurred())
			return NewError("ActionError", "failed after rollback")
		})

		// 1c. Transactional with early rollback and nil error (covers commitErr.LogError)
		mgr.Transactional(context.Background(), func(sess *MongoSession) Error {
			Expect(sess.Rollback()).NotTo(HaveOccurred())
			return nil
		})

		// 2. Transactional with callback error (rollback)
		mgr.Transactional(context.Background(), func(sess *MongoSession) Error {
			coll := client.Database("test_tx_db").Collection("docs")
			_, err := coll.InsertOne(sess, bson.M{"msg": "rollback_tx"})
			Expect(err).NotTo(HaveOccurred())
			return NewError("ActionError", "rollback requested")
		})

		// 3. Transactional with panic (rollback)
		Expect(func() {
			mgr.Transactional(context.Background(), func(sess *MongoSession) Error {
				coll := client.Database("test_tx_db").Collection("docs")
				_, insertErr := coll.InsertOne(sess, bson.M{"msg": "panic_tx"})
				Expect(insertErr).NotTo(HaveOccurred())
				panic("transactional callback panic")
			})
		}).To(Panic())

		// Verify neither rolled-back doc persisted
		coll := client.Database("test_tx_db").Collection("docs")
		count, countErr := coll.CountDocuments(context.Background(), bson.M{"msg": "rollback_tx"})
		Expect(countErr).NotTo(HaveOccurred())
		Expect(count).To(Equal(int64(0)))
		countPanic, countPanicErr := coll.CountDocuments(context.Background(), bson.M{"msg": "panic_tx"})
		Expect(countPanicErr).NotTo(HaveOccurred())
		Expect(countPanic).To(Equal(int64(0)))

		// 4. Execute success, error, panic
		execErr := mgr.Execute(context.Background(), func(_ *MongoSession) Error {
			return nil
		})
		Expect(execErr).NotTo(HaveOccurred())

		execActionErr := mgr.Execute(context.Background(), func(_ *MongoSession) Error {
			return NewError("ActionError", "execute action error")
		})
		Expect(execActionErr).To(HaveOccurred())
		Expect(execActionErr.Error()).To(ContainSubstring("execute action error"))

		// Execute with early rollback and action error (covers rollback failure logging)
		exRollbackErr := mgr.Execute(context.Background(), func(sess *MongoSession) Error {
			Expect(sess.Rollback()).NotTo(HaveOccurred())
			return NewError("ActionError", "rollback failed")
		})
		Expect(exRollbackErr).To(HaveOccurred())

		// Execute with early rollback and nil error (covers commitErr return)
		exCommitFailErr := mgr.Execute(context.Background(), func(sess *MongoSession) Error {
			Expect(sess.Rollback()).NotTo(HaveOccurred())
			return nil
		})
		Expect(exCommitFailErr).To(HaveOccurred())

		execPanicErr := mgr.Execute(context.Background(), func(_ *MongoSession) Error {
			panic("execute panic")
		})
		Expect(execPanicErr).To(HaveOccurred())
		Expect(execPanicErr.Error()).To(ContainSubstring("execute panic"))

		// 5. ExecuteWithResult success, error, panic, nil manager
		res, resErr := ExecuteWithResult(context.Background(), mgr, func(_ *MongoSession) (string, Error) {
			return "result_ok", nil
		})
		Expect(resErr).NotTo(HaveOccurred())
		Expect(res).To(Equal("result_ok"))

		_, resActionErr := ExecuteWithResult(context.Background(), mgr, func(_ *MongoSession) (string, Error) {
			return "", NewError("ActionError", "result action error")
		})
		Expect(resActionErr).To(HaveOccurred())

		// ExecuteWithResult with early rollback and action error
		_, exResRollbackErr := ExecuteWithResult(context.Background(), mgr, func(sess *MongoSession) (int, Error) {
			Expect(sess.Rollback()).NotTo(HaveOccurred())
			return 0, NewError("ActionError", "rollback failed")
		})
		Expect(exResRollbackErr).To(HaveOccurred())

		// ExecuteWithResult with early rollback and nil error
		_, exResCommitFailErr := ExecuteWithResult(context.Background(), mgr, func(sess *MongoSession) (int, Error) {
			Expect(sess.Rollback()).NotTo(HaveOccurred())
			return 0, nil
		})
		Expect(exResCommitFailErr).To(HaveOccurred())

		_, resPanicErr := ExecuteWithResult(context.Background(), mgr, func(_ *MongoSession) (string, Error) {
			panic("result panic")
		})
		Expect(resPanicErr).To(HaveOccurred())
		Expect(resPanicErr.Error()).To(ContainSubstring("result panic"))

		_, nilMgrErr := ExecuteWithResult[string](context.Background(), nil, func(_ *MongoSession) (string, Error) {
			return "", nil
		})
		Expect(nilMgrErr).To(HaveOccurred())

		// 6. Nested transaction stacking
		sOuter := mgr.Start(context.Background())
		Expect(mgr.Get()).To(Equal(sOuter))
		sInner := mgr.Start(context.Background())
		Expect(mgr.Get()).To(Equal(sInner))
		mgr.End()
		Expect(mgr.Get()).To(Equal(sOuter))
		mgr.End()

		// 7. Ambient session when no active session
		ambient := mgr.Get()
		Expect(ambient.Session()).To(BeNil())

		// 8. Manager without logger
		mgrNoLogger := NewMongoTransactionalManager(client, nil)
		mgrNoLogger.End() // empty stack
		s := mgrNoLogger.Start(context.Background())
		Expect(s).NotTo(BeNil())
		mgrNoLogger.End()

		// 9. Startup error propagation for Transactional, Execute, ExecuteWithResult
		disClient2 := &mongo.Client{}
		badMgr := NewMongoTransactionalManager(disClient2, nil)
		badMgr.Transactional(context.Background(), func(_ *MongoSession) Error {
			Fail("should not run on startup failure")
			return nil
		})
		Expect(badMgr.Execute(context.Background(), func(_ *MongoSession) Error {
			Fail("should not run on startup failure")
			return nil
		})).To(HaveOccurred())
		_, errWithResult := ExecuteWithResult(context.Background(), badMgr, func(_ *MongoSession) (int, Error) {
			Fail("should not run on startup failure")
			return 0, nil
		})
		Expect(errWithResult).To(HaveOccurred())
	})
})
