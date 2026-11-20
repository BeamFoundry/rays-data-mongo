package tx_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	raystx "github.com/BeamFoundry/rays/pkg/tx"
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
	commitErr   error
	commitCalls int
}

func (m *mockCommitErrSession) CommitTransaction(context.Context) error {
	m.commitCalls++
	return m.commitErr
}

type mockSeqCommitSession struct {
	mongo.Session
	errs        []error
	commitCalls int
}

func (m *mockSeqCommitSession) CommitTransaction(context.Context) error {
	m.commitCalls++
	if len(m.errs) > 0 {
		err := m.errs[0]
		m.errs = m.errs[1:]
		return err
	}
	return nil
}

type recordingLogSink struct {
	messages []string
	mu       sync.Mutex
}

func (r *recordingLogSink) Info(_ int, msg string, _ ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, msg)
}

func (r *recordingLogSink) Error(_ error, msg string, _ ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, msg)
}

func (r *recordingLogSink) Snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make([]string, len(r.messages))
	copy(copied, r.messages)
	return copied
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
		if strings.Contains(mongoURI, "?") {
			mongoURI += "&directConnection=true"
		} else {
			mongoURI += "?directConnection=true"
		}
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

		// Zero connect timeout boundary test
		zeroDuration := time.Duration(0)
		Expect(NewMongoClientBuilder().ExportResolvedConnectTimeout(&options.ClientOptions{ConnectTimeout: &zeroDuration})).To(Equal(time.Duration(0)))
		cZero, errZero := NewMongoClientBuilder().WithURI(mongoURI).WithConnectTimeout(0).Build()
		Expect(errZero).NotTo(HaveOccurred())
		Expect(cZero.Disconnect(context.Background())).NotTo(HaveOccurred())

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

		// Command logging suppression test with MONGODB_LOG_COMMAND and MONGODB_LOG_ALL set in environment
		GinkgoT().Setenv("MONGODB_LOG_COMMAND", "debug")
		GinkgoT().Setenv("MONGODB_LOG_ALL", "debug")

		recSink := &recordingLogSink{}
		cNoCmd, err := NewMongoClientBuilder().
			WithURI(mongoURI).
			WithLogSink(recSink).
			WithCommandLogging(false).
			Build()
		Expect(err).NotTo(HaveOccurred())
		Expect(cNoCmd.Database("test_tx_db").RunCommand(context.Background(), bson.D{{Key: "ping", Value: 1}}).Err()).NotTo(HaveOccurred())
		Expect(cNoCmd.Disconnect(context.Background())).NotTo(HaveOccurred())

		snapshot := recSink.Snapshot()
		for _, msg := range snapshot {
			Expect(strings.Contains(msg, "Command started")).To(BeFalse())
			Expect(strings.Contains(msg, "Command succeeded")).To(BeFalse())
		}

		Expect(NewMongoClientBuilder().ExportResolvedConnectTimeout(nil)).To(Equal(10 * time.Second))

		// Ping failure and disconnect cleanup
		unreachableURI := "mongodb://127.0.0.1:1/?directConnection=true&serverSelectionTimeoutMS=50"
		_, err = NewMongoClientBuilder().
			WithURI(unreachableURI).
			WithConnectTimeout(50 * time.Millisecond).
			Build()
		Expect(err).To(HaveOccurred())

		// Ping failure with zero connect timeout (covers no-timeout disconnect cleanup)
		_, err = NewMongoClientBuilder().
			WithURI(unreachableURI).
			WithConnectTimeout(0).
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
		Expect(sCtx.Value((*MongoSession)(nil))).To(Equal(sCtx))

		// Ended session methods return error
		endedSess := NewMongoSession(context.Background(), nil)
		endedSess.End()
		Expect(endedSess.Validate()).To(HaveOccurred())
		Expect(endedSess.Commit()).To(HaveOccurred())
		Expect(endedSess.Rollback()).To(HaveOccurred())
		Expect(endedSess.Abort()).To(HaveOccurred())

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
		Expect(mockSess.commitCalls).To(Equal(1))
		Expect(msMock.Validate()).To(HaveOccurred()) // commitPending is true
		Expect(msMock.Error()).NotTo(HaveOccurred()) // Not cached, retry allowed
		Expect(msMock.Rollback()).To(HaveOccurred()) // Cannot rollback with pending commit result
		mockSess.commitErr = nil
		Expect(msMock.Commit()).NotTo(HaveOccurred()) // Retry succeeds
		Expect(mockSess.commitCalls).To(Equal(2))
		msMock.End()

		// Mock session with expired context retrying on UnknownTransactionCommitResult
		cancCtx, cancFn := context.WithCancel(context.Background())
		cancFn()
		rawSessRetry, err := client.StartSession()
		Expect(err).NotTo(HaveOccurred())
		mockSessRetry := &mockCommitErrSession{Session: rawSessRetry, commitErr: errors.New("UnknownTransactionCommitResult")}
		msMockRetry := NewMongoSession(cancCtx, mockSessRetry)
		Expect(msMockRetry.Commit()).To(HaveOccurred())
		Expect(mockSessRetry.commitCalls).To(Equal(2))
		msMockRetry.End()

		// Mock session with unknown commit result followed by server selection timeout on retry,
		// preserving commitPending without caching terminal error, followed by successful subsequent Commit.
		cancCtx2, cancFn2 := context.WithCancel(context.Background())
		cancFn2()
		rawSessSeq, err := client.StartSession()
		Expect(err).NotTo(HaveOccurred())
		seqMock := &mockSeqCommitSession{
			Session: rawSessSeq,
			errs: []error{
				errors.New("UnknownTransactionCommitResult"),
				errors.New("server selection timeout"),
			},
		}
		msSeq := NewMongoSession(cancCtx2, seqMock)
		commitErr := msSeq.Commit()
		Expect(commitErr).To(HaveOccurred())
		Expect(seqMock.commitCalls).To(Equal(2))
		Expect(msSeq.Validate()).To(HaveOccurred()) // commitPending remains true
		Expect(msSeq.Error()).NotTo(HaveOccurred()) // inconclusive error is not cached as terminal

		// Subsequent Commit on the same session succeeds
		Expect(msSeq.Commit()).NotTo(HaveOccurred())
		Expect(seqMock.commitCalls).To(Equal(3))
		Expect(msSeq.Error()).NotTo(HaveOccurred())
		msSeq.End()

		// Mock session where retry of CommitTransaction succeeds immediately
		cancCtx3, cancFn3 := context.WithCancel(context.Background())
		cancFn3()
		rawSessSuccess, err := client.StartSession()
		Expect(err).NotTo(HaveOccurred())
		seqMockSuccess := &mockSeqCommitSession{
			Session: rawSessSuccess,
			errs: []error{
				errors.New("UnknownTransactionCommitResult"),
			},
		}
		msSeqSuccess := NewMongoSession(cancCtx3, seqMockSuccess)
		Expect(msSeqSuccess.Commit()).NotTo(HaveOccurred())
		Expect(seqMockSuccess.commitCalls).To(Equal(2))
		msSeqSuccess.End()

		Expect(ExportIsServerSelectionError(nil)).To(BeFalse())
		Expect(ExportIsServerSelectionError(errors.New("server selection timeout"))).To(BeTrue())
		Expect(ExportIsServerSelectionError(errors.New("other error"))).To(BeFalse())

		// Mock session with terminal non-retryable error (cached)
		rawSess2, err := client.StartSession()
		Expect(err).NotTo(HaveOccurred())
		mockSessTerminal := &mockCommitErrSession{Session: rawSess2, commitErr: errors.New("terminal commit error")}
		msMockTerminal := NewMongoSession(context.Background(), mockSessTerminal)
		Expect(msMockTerminal.Commit()).To(HaveOccurred())
		Expect(mockSessTerminal.commitCalls).To(Equal(1))
		Expect(msMockTerminal.Error()).To(HaveOccurred())  // Cached
		Expect(msMockTerminal.Commit()).To(HaveOccurred()) // Returns cached error
		Expect(mockSessTerminal.commitCalls).To(Equal(1))  // Terminal error does not call mock on second Commit
		msMockTerminal.End()

		// Session with initial error
		disClient := &mongo.Client{}
		disMgr := NewMongoTransactionalManager(disClient, nil)
		Expect(disMgr.Start()).To(HaveOccurred())
		disSess := disMgr.Get()
		Expect(disSess.Error()).To(HaveOccurred())
		Expect(disSess.Validate()).To(HaveOccurred())
		Expect(disSess.Commit()).To(HaveOccurred())
		Expect(disSess.Rollback()).To(HaveOccurred())
		disMgr.End()
	})

	It("covers MongoTransactionalManager Start, Commit, Rollback, Get, End, SetError, Error, and TransactionManager coordination", func() {
		logger := &testTxLogger{}
		txManager := raystx.DefaultTransactionManager{}.New()
		mgr := NewMongoTransactionalManager(client, logger).WithTransactionManager(txManager)

		// 1. Coordinated Transactional commit
		txManager.SetContext(context.Background())
		Expect(txManager.Transactional(func() Error {
			sess := mgr.Get()
			Expect(sess).NotTo(BeNil())
			Expect(mgr.Get()).To(Equal(sess))
			coll := client.Database("test_tx_db").Collection("docs")
			_, err := coll.InsertOne(sess, bson.M{"msg": "success_tx"})
			Expect(err).NotTo(HaveOccurred())
			return nil
		})).NotTo(HaveOccurred())
		txManager.ClearContext()

		// 2. Coordinated Transactional with callback error (rollback)
		Expect(txManager.Transactional(func() Error {
			sess := mgr.Get()
			coll := client.Database("test_tx_db").Collection("docs")
			_, err := coll.InsertOne(sess, bson.M{"msg": "rollback_tx"})
			Expect(err).NotTo(HaveOccurred())
			return NewError("ActionError", "rollback requested")
		})).To(HaveOccurred())

		// 3. Coordinated Transactional with SetError (immediate rollback)
		Expect(txManager.Transactional(func() Error {
			sess := mgr.Get()
			coll := client.Database("test_tx_db").Collection("docs")
			_, err := coll.InsertOne(sess, bson.M{"msg": "seterror_tx"})
			Expect(err).NotTo(HaveOccurred())
			mgr.SetError(nil) // nil is no-op
			Expect(mgr.Error()).NotTo(HaveOccurred())
			mgr.SetError(NewError("RepoError", "repository failure"))
			Expect(mgr.Error()).To(HaveOccurred())
			return nil
		})).To(HaveOccurred())

		// 4. Coordinated Transactional with panic (rollback)
		Expect(txManager.Transactional(func() Error {
			sess := mgr.Get()
			coll := client.Database("test_tx_db").Collection("docs")
			_, insertErr := coll.InsertOne(sess, bson.M{"msg": "panic_tx"})
			Expect(insertErr).NotTo(HaveOccurred())
			panic("transactional callback panic")
		})).To(HaveOccurred())

		// Verify neither rolled-back doc persisted, and successful doc persisted
		coll := client.Database("test_tx_db").Collection("docs")
		countSuccess, countSuccessErr := coll.CountDocuments(context.Background(), bson.M{"msg": "success_tx"})
		Expect(countSuccessErr).NotTo(HaveOccurred())
		Expect(countSuccess).To(Equal(int64(1)))

		count, countErr := coll.CountDocuments(context.Background(), bson.M{"msg": "rollback_tx"})
		Expect(countErr).NotTo(HaveOccurred())
		Expect(count).To(Equal(int64(0)))
		countSetErr, countSetErrErr := coll.CountDocuments(context.Background(), bson.M{"msg": "seterror_tx"})
		Expect(countSetErrErr).NotTo(HaveOccurred())
		Expect(countSetErr).To(Equal(int64(0)))
		countPanic, countPanicErr := coll.CountDocuments(context.Background(), bson.M{"msg": "panic_tx"})
		Expect(countPanicErr).NotTo(HaveOccurred())
		Expect(countPanic).To(Equal(int64(0)))

		// 5. Enlistment failure when txManager already has an error before mgr.Get() is called
		Expect(txManager.Transactional(func() Error {
			txManager.SetError(NewError("PreExistingError", "already failed"))
			sess := mgr.Get()
			Expect(sess.Error()).To(HaveOccurred())
			Expect(sess.Error().Name()).To(Equal("PreExistingError"))
			return nil
		})).To(HaveOccurred())

		// 6. Enlistment failure when client fails Start() during mgr.Get()
		disClient := &mongo.Client{}
		badMgr := NewMongoTransactionalManager(disClient, logger).WithTransactionManager(txManager)
		Expect(txManager.Transactional(func() Error {
			sess := badMgr.Get()
			Expect(sess.Error()).To(HaveOccurred())
			return nil
		})).To(HaveOccurred())

		// 7. Standalone manager: Commit/Rollback without active session, SetError/Error, nested stacking, nil logger
		standalone := NewMongoTransactionalManager(client, nil)
		Expect(standalone.Commit()).To(HaveOccurred())
		Expect(standalone.Rollback()).To(HaveOccurred())
		Expect(mgr.Commit()).To(HaveOccurred())
		Expect(mgr.Rollback()).To(HaveOccurred())
		Expect(standalone.Error()).NotTo(HaveOccurred())
		standalone.SetError(NewError("NoSessErr", "ignored when no session"))
		standalone.End() // empty stack

		Expect(standalone.Start()).NotTo(HaveOccurred())
		sOuter := standalone.Get()
		Expect(sOuter.Session()).NotTo(BeNil())
		Expect(standalone.Start()).NotTo(HaveOccurred())
		sInner := standalone.Get()
		Expect(sInner).NotTo(Equal(sOuter))
		standalone.SetError(NewError("InnerErr", "inner failed"))
		Expect(standalone.Error()).To(HaveOccurred())
		Expect(standalone.Error().Name()).To(Equal("InnerErr"))
		// Second SetError preserves initial session error
		standalone.SetError(NewError("SecondErr", "second ignored"))
		Expect(standalone.Error().Name()).To(Equal("InnerErr"))
		standalone.End()
		Expect(standalone.Get()).To(Equal(sOuter))
		Expect(standalone.Commit()).NotTo(HaveOccurred())
		standalone.End()

		// 8. Ambient session when no active session
		ambient := mgr.Get()
		Expect(ambient.Session()).To(BeNil())
		Expect(mgr.Error()).NotTo(HaveOccurred())

		// 9. New(), WithDatabase(), Database(), and resolvedClient() fallbacks (database client and nil client)
		dbBackedMgr := MongoTransactionalManager{}.New().WithDatabase(client.Database("test_tx_db"))
		Expect(dbBackedMgr.Database()).NotTo(BeNil())
		Expect(dbBackedMgr.Database().Name()).To(Equal("test_tx_db"))
		Expect(dbBackedMgr.Start()).NotTo(HaveOccurred())
		Expect(dbBackedMgr.Get().Session()).NotTo(BeNil())
		dbBackedMgr.End()

		nilClientMgr := MongoTransactionalManager{}.New()
		Expect(nilClientMgr.Database()).To(BeNil())
		Expect(nilClientMgr.Start()).To(HaveOccurred())
		nilClientMgr.End()
	})
})
