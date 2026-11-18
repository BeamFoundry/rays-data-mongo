package core_test

import (
	"context"
	"errors"
	"math"
	"reflect"
	"time"

	"github.com/testcontainers/testcontainers-go"
	container "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "github.com/BeamFoundry/rays-data-mongo/pkg/core"
	. "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
	. "github.com/BeamFoundry/rays-data-mongo/pkg/util"
	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/lang"
	. "github.com/BeamFoundry/rays/pkg/testing"
)

type customBSONType struct {
	Raw string
}

func (c customBSONType) MarshalBSONValue() (bsontype.Type, []byte, error) {
	//nolint:wrapcheck // Test helper implementing bson.ValueMarshaler
	return bson.MarshalValue(c.Raw)
}

func (c *customBSONType) UnmarshalBSONValue(t bsontype.Type, data []byte) error {
	//nolint:wrapcheck // Test helper implementing bson.ValueUnmarshaler
	return bson.UnmarshalValue(t, data, &c.Raw)
}

type customBSONMarshaler struct {
	Val string
}

func (c customBSONMarshaler) MarshalBSON() ([]byte, error) {
	//nolint:wrapcheck // Test helper implementing bson.Marshaler
	return bson.Marshal(bson.M{"val": c.Val})
}

type textTag struct {
	Val string
}

func (t textTag) MarshalText() ([]byte, error) {
	return []byte(t.Val), nil
}

func (t *textTag) UnmarshalText(b []byte) error {
	t.Val = string(b)
	return nil
}

type InlineMeta struct {
	Tenant string `bson:"tenant"`
}

type InlineBase struct {
	InlineID string `bson:"_id,omitempty"`
	Tenant   string `bson:"tenant"`
}

type NestedSub struct {
	Code string `bson:"code"`
}

type CyclicDoc struct {
	Next *CyclicDoc `bson:"next,omitempty"`
	ID   string     `bson:"_id,omitempty"`
}

type BadInlineEntity struct {
	InlinePrim int `bson:",inline"`
}

type BadInlineRepository struct {
	MongoRepository[BadInlineEntity, string]
}

type InlineNoIDEntity struct {
	InlineMeta `bson:",inline"`
	ID         string
}

type InlineNoIDRepository struct {
	MongoRepository[InlineNoIDEntity, string]
}

type RichEntity struct {
	CreatedAt  time.Time  `bson:"created_at"`
	Sub        *NestedSub `bson:"sub,omitempty"`
	Nullable   *string    `bson:"nullable,omitempty"`
	InlineBase `bson:",inline"`
	//nolint:unused // Intentionally testing unexported field skipping in collectQueryFields
	unexported string
	Ignored    string              `bson:"-"`
	TagNoName  string              `bson:",omitempty"`
	IP         textTag             `bson:"ip,omitempty"`
	Custom     customBSONType      `bson:"custom,omitempty"`
	Marshaler  customBSONMarshaler `bson:"marshaler,omitempty"`
	Label      string              `bson:"label"`
	Score      int                 `bson:"score"`
	Enabled    bool                `bson:"enabled"`
}

type RichRepository struct {
	findByLabelIs             func(string) []RichEntity
	findByLabelEquals         func(string) []RichEntity
	findByScoreAfter          func(int) []RichEntity
	findByScoreBefore         func(int) []RichEntity
	findByNullableNull        func() []RichEntity
	findByNullableNotNull     func() []RichEntity
	findByLabelRegex          func(string) []RichEntity
	findByLabelMatches        func(string) []RichEntity
	findByLabelNotRegex       func(string) []RichEntity
	findByLabelNotContaining  func(string) []RichEntity
	findByEnabledIsTrue       func() []RichEntity
	findByEnabledIsFalse      func() []RichEntity
	findBySubCode             func(string) []RichEntity
	findByTenant              func(string) []RichEntity
	deleteOnlyErrByLabel      func(string) Error
	queryWithErrByLabel       func(string, ...MongoQueryFunc) (*MongoQuery, Error)
	findBadNoOutByLabel       func(string)
	findBadThreeOutByLabel    func(string) (string, string, Error)
	findBadSecondOutByLabel   func(string) (int64, string)
	deleteBadReturnByLabel    func(string) string
	findBadSepByLabelXorScore func(string, int) []RichEntity
	MongoRepository[RichEntity, string]
}

type CyclicRepository struct {
	findByID func(string) *CyclicDoc
	MongoRepository[CyclicDoc, string]
}

type PtrNamerEntity struct {
	ID string
}

func (*PtrNamerEntity) CollectionName() string {
	return "ptr_named_docs"
}

type PtrNamerRepository struct {
	MongoRepository[PtrNamerEntity, string]
}

type Box struct {
	ID   string `bson:"_id,omitempty"`
	Name string `bson:"name"`
}

type BoxRepository struct {
	MongoRepository[Box, string]
}

type Category struct {
	ID   string `bson:"_id,omitempty"`
	Name string `bson:"name"`
}

type CategoryRepository struct {
	MongoRepository[Category, string]
}

type Boy struct {
	ID   string `bson:"_id,omitempty"`
	Name string `bson:"name"`
}

type BoyRepository struct {
	MongoRepository[Boy, string]
}

type AnyIDEntity struct {
	Ignored string `bson:"-"`
	ID      any    `bson:"_id,omitempty"`
	Name    string `bson:"name"`
}

type AnyIDRepository struct {
	MongoRepository[AnyIDEntity, any]
}

type NoIDEntity struct {
	Name string `bson:"name"`
}

type NoIDRepository struct {
	MongoRepository[NoIDEntity, string]
}

type UntaggedIDEntity struct {
	ID   string
	Name string
}

type UntaggedIDRepository struct {
	MongoRepository[UntaggedIDEntity, string]
}

type UnexportedFieldEntity struct {
	unexportedID string `bson:"_id"`
	Name         string `bson:"name"`
}

type UnexportedFieldRepo struct {
	MongoRepository[UnexportedFieldEntity, string]
}

type LikeRepo struct {
	findByLabelLike func(string) []RichEntity
	MongoRepository[RichEntity, string]
}

type TeamOrderEntity struct {
	Name            string             `bson:"name"`
	TeamOrderByCode string             `bson:"team_order_by_code"`
	ID              primitive.ObjectID `bson:"_id,omitempty"`
}

type TeamOrderRepo struct {
	FindByNameOrderByTeamOrderByCode func(name string) ([]TeamOrderEntity, Error)
	MongoRepository[TeamOrderEntity, primitive.ObjectID]
}

type errBSONMarshaler struct{}

func (e errBSONMarshaler) MarshalBSON() ([]byte, error) {
	return nil, errors.New("marshal boom")
}

type BadMarshalEntity struct {
	Field errBSONMarshaler   `bson:"field"`
	ID    primitive.ObjectID `bson:"_id"`
}

type BadMarshalRepo struct {
	MongoRepository[BadMarshalEntity, primitive.ObjectID]
}

type NilOIDEntity struct {
	ID   *primitive.ObjectID `bson:"_id"`
	Name string              `bson:"name"`
}

type NilOIDRepo struct {
	MongoRepository[NilOIDEntity, primitive.ObjectID]
}

type AnonRepository struct {
	MongoRepository[struct{ Name string }, string]
}

type PtrEntityRepository struct {
	MongoRepository[*User, primitive.ObjectID]
}

type PrimitiveEntityRepository struct {
	MongoRepository[string, string]
}

type WorkOrderEntity struct {
	WorkOrderByName string             `bson:"work_order_by_name"`
	Status          string             `bson:"status"`
	Name            string             `bson:"name"`
	AgeLess         int                `bson:"age_less"`
	Age             int                `bson:"age"`
	ID              primitive.ObjectID `bson:"_id,omitempty"`
}

type WorkOrderRepo struct {
	findByWorkOrderByName              func(string) *WorkOrderEntity
	findByStatusOrderByName            func(string) []WorkOrderEntity
	findByAgeLessThan                  func(int) []WorkOrderEntity
	findByStatusOrderByWorkOrderByName func(string) []WorkOrderEntity
	MongoRepository[WorkOrderEntity, primitive.ObjectID]
}

type IntIDEntity struct {
	Name string `bson:"name"`
	ID   int    `bson:"_id"`
}

type IntIDRepo struct {
	MongoRepository[IntIDEntity, int]
}

type PtrIDEntity struct {
	ID   *primitive.ObjectID `bson:"_id,omitempty"`
	Name string              `bson:"name"`
}

type PtrIDRepo struct {
	MongoRepository[PtrIDEntity, *primitive.ObjectID]
}

type TestMultiMongoConfig struct {
	Configuration
	endpoint *TestMongoEndpoint `@:"Inject"`
}

func (c *TestMultiMongoConfig) PrimaryClient(
	_ struct{ Primary },
) (*mongo.Client, Error) {
	return NewMongoClientBuilder().
		WithHost(c.endpoint.Host).
		WithPort(c.endpoint.Port).
		WithDirectConnection(true).
		Build()
}

func (c *TestMultiMongoConfig) AnalyticsClient(
	_ struct {
		Qualifier `Name:"analyticsClient"`
	},
) (*mongo.Client, Error) {
	return NewMongoClientBuilder().
		WithHost(c.endpoint.Host).
		WithPort(c.endpoint.Port).
		WithDirectConnection(true).
		Build()
}

func (c *TestMultiMongoConfig) Database(
	that struct {
		ConditionalOnMissingBeam
		client *mongo.Client `@:"Inject"`
	},
) *mongo.Database {
	return that.client.Database("test_multi")
}

var _, _ = any(&TestMultiMongoConfig{}).(Stereotype)

type TestMultiClientConsumer struct {
	Service
	primary   *mongo.Client `@:"Inject"`
	analytics *mongo.Client `@:"Inject,Qualifier{analyticsClient}"`
}

var _, _ = any(&TestMultiClientConsumer{}).(Stereotype)

var _ = Describe("Coverage Edge Cases", Ordered, func() {
	var (
		mongodbContainer *container.MongoDBContainer
		mongoHost        string
		mongoPort        int
		mongoURI         string
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
		mongoURI += "&directConnection=true"
	})

	AfterAll(func() {
		if mongodbContainer != nil {
			testcontainers.CleanupContainer(GinkgoTB(), mongodbContainer)
		}
	})

	It("covers MongoLogSink Info and Error paths including nil logger", func() {
		nilSink := MongoLogSink{}.New()
		nilSink.Info(1, "ignored")
		nilSink.Error(errors.New("ignored"), "ignored")

		AddBeamFor(&TestMongoConfig{})
		AddBeamFor(&TestMongoEndpoint{Host: mongoHost, Port: mongoPort})
		AddBeamFor(MongoLogSink{}.New())
		GetContainer().EnableScanning()
		Expect(GetContainer().Initialize()).NotTo(HaveOccurred())
		sink := GetBeamScopedInstance[*MongoLogSink]()
		sink.Info(1, "debug message", "k", "v")
		sink.Info(2, "trace message", "k", "v")
		sink.Error(errors.New("driver err"), "error with err", "k", "v")
		sink.Error(nil, "error without err", "k", "v")
		Expect(GetBeamScopedInstance[*mongo.Client]().Disconnect(context.Background())).NotTo(HaveOccurred())
	})

	It("covers MongoClientBuilder options, Build, BuildDatabase, and connection errors", func() {
		// 1. URI mode with zero timeouts -> falls back to internal defaults
		db, err := NewMongoClientBuilder().
			WithURI(mongoURI).
			WithConnectTimeout(0).
			WithServerSelectionTimeout(0).
			BuildDatabase("builder_db", context.Background())
		Expect(err).NotTo(HaveOccurred())
		Expect(db.Name()).To(Equal("builder_db"))
		Expect(db.Client().Disconnect(context.Background())).NotTo(HaveOccurred())

		// 2. Empty host/port/uri -> resolvedURI uses defaultHost/defaultPort with short timeout
		clientDefault, errDefault := NewMongoClientBuilder().
			WithHost("").
			WithPort(0).
			WithConnectTimeout(50 * time.Millisecond).
			WithServerSelectionTimeout(50 * time.Millisecond).
			Build()
		if errDefault == nil {
			_ = clientDefault.Disconnect(context.Background())
		}

		// 3. Invalid URI -> mongo.Connect error
		_, err = NewMongoClientBuilder().
			WithURI("://invalid-uri").
			Build()
		Expect(err).To(HaveOccurred())

		// 4. Unreachable URI with auth, pool sizes, timeouts -> Ping fails + Disconnect
		_, err = NewMongoClientBuilder().
			WithURI("mongodb://127.0.0.1:1/?directConnection=true").
			WithCredentials("user", "pass", "admin").
			WithConnectTimeout(50 * time.Millisecond).
			WithServerSelectionTimeout(50 * time.Millisecond).
			WithMaxPoolSize(10).
			WithMinPoolSize(1).
			WithDirectConnection(true).
			WithLogSink(MongoLogSink{}.New()).
			WithOptions(nil).
			WithOptions(func(o *options.ClientOptions) { o.SetAppName("cov") }).
			BuildDatabase("unreachable")
		Expect(err).To(HaveOccurred())
	})

	It("covers MongoAutoConfig database-only fallback, missing beam error, and multiple clients via Configuration", func() {
		// 1. Database-only beam (no *mongo.Client beam) -> MongoAutoConfig uses db.Client()
		db, err := NewMongoClientBuilder().
			WithHost(mongoHost).
			WithPort(mongoPort).
			WithDirectConnection(true).
			BuildDatabase("db_only_test")
		Expect(err).NotTo(HaveOccurred())
		AddBeamFor(db)
		GetContainer().EnableScanning()
		Expect(GetContainer().Initialize()).NotTo(HaveOccurred())
		Expect(GetBeamScopedInstance[MongoTransactional]()).NotTo(BeNil())
		Expect(GetBeamScopedInstance[options.LogSink]()).NotTo(BeNil())
		Expect(db.Client().Disconnect(context.Background())).NotTo(HaveOccurred())
		GetContainer().Reset()

		// 2. No *mongo.Client or *mongo.Database beam -> MongoTxManager returns MongoConfigurationError
		GetContainer().EnableScanning()
		Expect(GetContainer().Initialize()).To(HaveOccurred())
		GetContainer().Reset()

		// 3. Multiple MongoDB clients registered through a single Configuration with Primary + Qualifier
		AddBeamFor(&TestMultiMongoConfig{})
		AddBeamFor(&TestMongoEndpoint{Host: mongoHost, Port: mongoPort})
		AddBeamFor(&TestMultiClientConsumer{})
		GetContainer().EnableScanning()
		Expect(GetContainer().Initialize()).NotTo(HaveOccurred())

		consumer := GetBeamScopedInstance[*TestMultiClientConsumer]()
		Expect(consumer).NotTo(BeNil())
		Expect(consumer.primary).NotTo(BeNil())
		Expect(consumer.analytics).NotTo(BeNil())
		DeferCleanup(func() {
			if consumer.primary != nil {
				_ = consumer.primary.Disconnect(context.Background())
			}
			if consumer.analytics != nil {
				_ = consumer.analytics.Disconnect(context.Background())
			}
		})
		// Assert unqualified resolution returns primary client and qualified resolution returns other client
		Expect(consumer.primary).NotTo(BeIdenticalTo(consumer.analytics))
		GetContainer().Reset()
	})

	It("covers MongoTransactionalManager Start(nil), nil logger, and StartSession error on unconnected client", func() {
		ctx := context.Background()
		AddBeamFor(&TestMongoConfig{})
		AddBeamFor(&TestMongoEndpoint{Host: mongoHost, Port: mongoPort})
		GetContainer().EnableScanning()
		Expect(GetContainer().Initialize()).NotTo(HaveOccurred())

		txMgr := GetBeamScopedInstance[MongoTransactional]()
		//nolint:staticcheck // Intentionally testing nil context fallback
		sess := txMgr.Start(nil)
		Expect(sess.Error()).NotTo(HaveOccurred())
		txMgr.End()

		// Manager with nil logger
		client := GetBeamScopedInstance[*mongo.Client]()
		noLogMgr := NewMongoTransactionalManager(client, nil)
		//nolint:staticcheck // Intentionally testing nil context fallback
		s := noLogMgr.Start(nil)
		Expect(s.Error()).NotTo(HaveOccurred())
		noLogMgr.End()

		Expect(client.Disconnect(ctx)).NotTo(HaveOccurred())
		GetContainer().Reset()

		// Unconnected client has nil sessionPool -> StartSession returns ErrClientDisconnected
		//nolint:staticcheck // Intentionally testing unconnected client from NewClient
		unconnected, driverErr := mongo.NewClient(options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
		Expect(driverErr).NotTo(HaveOccurred())
		AddBeamFor(unconnected)
		AddBeamFor(unconnected.Database("tx_start_test"))
		GetContainer().EnableScanning()
		Expect(GetContainer().Initialize()).NotTo(HaveOccurred())

		txMgr2 := GetBeamScopedInstance[MongoTransactional]()
		sess2 := txMgr2.Start(ctx)
		Expect(sess2.Error()).To(HaveOccurred())
		Expect(sess2.Validate()).To(HaveOccurred())
		Expect(sess2.Rollback()).To(HaveOccurred())
		Expect(sess2.Commit()).To(HaveOccurred())

		// Transactional closure logs ms.Error() and exits safely when Start fails
		txMgr2.Transactional(ctx, func(_ *MongoSession) Error {
			Fail("should not run action when Start fails")
			return nil
		})
	})

	Context("Core Query, Repository, Builder, and Session Edge Cases", func() {
		var (
			richRepo       *RichRepository
			cyclicRepo     *CyclicRepository
			ptrNameRepo    *PtrNamerRepository
			boxRepo        *BoxRepository
			catRepo        *CategoryRepository
			boyRepo        *BoyRepository
			anyRepo        *AnyIDRepository
			noIDRepo       *NoIDRepository
			anonRepo       *AnonRepository
			ptrEntRepo     *PtrEntityRepository
			primRepo       *PrimitiveEntityRepository
			userRepo       *UserRepository
			likeRepo       *LikeRepo
			inlineNoIDRepo *InlineNoIDRepository
			workOrderRepo  *WorkOrderRepo
			intIDRepo      *IntIDRepo
			ptrIDRepo      *PtrIDRepo
			teamRepo       *TeamOrderRepo
			badMarshalRepo *BadMarshalRepo
			nilOIDRepo     *NilOIDRepo
			txMgr          MongoTransactional
			db             *mongo.Database
			client         *mongo.Client
		)

		BeforeEach(func() {
			richRepo = &RichRepository{}
			likeRepo = &LikeRepo{}
			cyclicRepo = &CyclicRepository{}
			ptrNameRepo = &PtrNamerRepository{}
			boxRepo = &BoxRepository{}
			catRepo = &CategoryRepository{}
			boyRepo = &BoyRepository{}
			anyRepo = &AnyIDRepository{}
			noIDRepo = &NoIDRepository{}
			anonRepo = &AnonRepository{}
			ptrEntRepo = &PtrEntityRepository{}
			primRepo = &PrimitiveEntityRepository{}
			userRepo = &UserRepository{}
			inlineNoIDRepo = &InlineNoIDRepository{}
			workOrderRepo = &WorkOrderRepo{}
			intIDRepo = &IntIDRepo{}
			ptrIDRepo = &PtrIDRepo{}
			teamRepo = &TeamOrderRepo{}
			badMarshalRepo = &BadMarshalRepo{}
			nilOIDRepo = &NilOIDRepo{}
			badInlineRepo := &BadInlineRepository{}

			AddBeamFor(&TestMongoConfig{})
			AddBeamFor(&TestMongoEndpoint{Host: mongoHost, Port: mongoPort})
			AddBeamFor(richRepo)
			AddBeamFor(likeRepo)
			AddBeamFor(badInlineRepo)
			AddBeamFor(inlineNoIDRepo)
			AddBeamFor(cyclicRepo)
			AddBeamFor(ptrNameRepo)
			AddBeamFor(boxRepo)
			AddBeamFor(catRepo)
			AddBeamFor(boyRepo)
			AddBeamFor(anyRepo)
			AddBeamFor(noIDRepo)
			AddBeamFor(anonRepo)
			AddBeamFor(ptrEntRepo)
			AddBeamFor(primRepo)
			AddBeamFor(userRepo)
			AddBeamFor(workOrderRepo)
			AddBeamFor(intIDRepo)
			AddBeamFor(ptrIDRepo)
			AddBeamFor(teamRepo)
			AddBeamFor(badMarshalRepo)
			AddBeamFor(nilOIDRepo)
			GetContainer().EnableScanning()
			Expect(GetContainer().Initialize()).NotTo(HaveOccurred())

			db = GetBeamScopedInstance[*mongo.Database]()
			client = GetBeamScopedInstance[*mongo.Client]()
			txMgr = GetBeamScopedInstance[MongoTransactional]()

			ctx := context.Background()
			Expect(db.Drop(ctx)).NotTo(HaveOccurred())
			_, err := userRepo.EnsureIndexes(ctx, mongo.IndexModel{
				Keys: bson.D{{Key: "user_name", Value: 1}},
			})
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			if client != nil {
				_ = client.Disconnect(context.Background())
			}
		})

		It("covers MongoSession nil receiver, context interface, Commit/Rollback/Abort/Validate/End, and disconnect errors", func() {
			var nilSess *MongoSession
			Expect(nilSess.Context()).To(Equal(context.Background()))
			Expect(nilSess.Session()).To(BeNil())
			Expect(nilSess.Error()).To(Succeed())
			nilSess.SetError(NewError("Test", "ignored"))
			Expect(nilSess.Validate()).To(Succeed())
			Expect(nilSess.Commit()).To(HaveOccurred())
			Expect(nilSess.Rollback()).To(HaveOccurred())
			Expect(nilSess.Abort()).To(HaveOccurred())
			nilSess.End()

			//nolint:staticcheck // Intentionally testing nil context fallback
			bgSess := NewMongoSession(nil, nil)
			Expect(bgSess.Context()).NotTo(BeNil())
			Expect(bgSess.Session()).To(BeNil())
			Expect(bgSess.Error()).To(Succeed())
			Expect(bgSess.Validate()).To(Succeed())
			Expect(bgSess.Commit()).To(HaveOccurred())
			Expect(bgSess.Rollback()).To(HaveOccurred())

			// context.Context interface methods
			type ctxKey string
			valCtx := context.WithValue(context.Background(), ctxKey("k"), "v")
			ctxWithDeadline, cancel := context.WithTimeout(valCtx, time.Second)
			defer cancel()
			deadSess := NewMongoSession(ctxWithDeadline, nil)
			_, hasDeadline := deadSess.Deadline()
			Expect(hasDeadline).To(BeTrue())
			Expect(deadSess.Done()).NotTo(BeNil())
			Expect(deadSess.Err()).To(Succeed())
			Expect(deadSess.Value(ctxKey("k"))).To(Equal("v"))

			// Manual Commit & double-finalization validation
			s1 := txMgr.Start(context.Background())
			Expect(s1.Session()).NotTo(BeNil())
			Expect(s1.Commit()).NotTo(HaveOccurred())
			Expect(s1.Validate()).To(HaveOccurred())
			Expect(s1.Commit()).To(HaveOccurred())
			Expect(s1.Rollback()).To(HaveOccurred())
			txMgr.End()

			// Get() after End() hits nil threadLocal fallback
			Expect(txMgr.Get()).NotTo(BeNil())

			// Manual Abort alias
			s2 := txMgr.Start(context.Background())
			Expect(s2.Abort()).NotTo(HaveOccurred())
			txMgr.End()

			// Direct underlying AbortTransaction causes MongoSession.Rollback() driver error
			s3 := txMgr.Start(context.Background())
			Expect(s3.Session().AbortTransaction(context.Background())).NotTo(HaveOccurred())
			Expect(s3.Rollback()).To(HaveOccurred())
			txMgr.End()

			// CommitTransaction driver error via direct underlying AbortTransaction inside Transactional
			txMgr.Transactional(context.Background(), func(ms *MongoSession) Error {
				Expect(ms.Session().AbortTransaction(context.Background())).NotTo(HaveOccurred())
				return nil
			})
		})

		It("covers Transactional Rollback failure when underlying session was already aborted", func() {
			txMgr.Transactional(context.Background(), func(ms *MongoSession) Error {
				Expect(ms.Session().AbortTransaction(context.Background())).NotTo(HaveOccurred())
				return NewError("ForceRollback", "rollback after direct abort")
			})
		})

		It("covers MongoQuery builder methods, error paths, Distinct, Aggregate, and likeToRegex", func() {
			ctx := context.Background()
			_, err := userRepo.SaveAll([]*User{
				{Name: "Alice", UserName: "alice", Age: 20, Address: Address{City: "Oz", Country: "US"}},
				{Name: "Bob", UserName: "bob", Age: 30, Address: Address{City: "NYC", Country: "US"}},
			})
			Expect(err).NotTo(HaveOccurred())

			// Nil *MongoQuery safety
			var nilQ *MongoQuery
			Expect(nilQ.Where(bson.M{"name": "Alice"}).Filter()).To(Equal(bson.M{"name": "Alice"}))
			Expect(nilQ.Error()).To(Succeed())
			Expect(nilQ.RawCollection()).To(BeNil())
			var out []User
			Expect(nilQ.Find(&out)).To(HaveOccurred())
			Expect(nilQ.First(&User{})).To(HaveOccurred())
			_, err = nilQ.Count()
			Expect(err).To(HaveOccurred())
			_, err = nilQ.Exists()
			Expect(err).To(HaveOccurred())
			_, err = nilQ.Delete()
			Expect(err).To(HaveOccurred())
			Expect(nilQ.Distinct("name", &[]string{})).To(HaveOccurred())
			Expect(nilQ.Aggregate(mongo.Pipeline{}, &out)).To(HaveOccurred())

			// Empty NewMongoQuery and Collection("")
			//nolint:staticcheck // Intentionally testing nil context fallback
			emptyQ := NewMongoQuery(nil, db, "").Collection("")
			Expect(emptyQ.CollectionName()).To(Equal(""))
			Expect(emptyQ.RawCollection()).To(BeNil())
			Expect(emptyQ.Find(&out)).To(HaveOccurred())

			// WithContext, Where(nil), Or edge cases, Offset, Select/Omit empty, Projection/Order types
			q := userRepo.Query().
				//nolint:staticcheck // Intentionally testing nil context fallback
				WithContext(nil).
				WithContext(ctx).
				Where(nil).
				Or().
				Or(bson.M{"name": "Alice"}).
				Offset(0).
				Select().
				Omit().
				Projection(nil).
				Projection(bson.D{{Key: "name", Value: 1}}).
				Projection(bson.E{Key: "age", Value: 1}).
				Projection(bson.M{"user_name": 1}).
				Projection(map[string]any{"address": 1}).
				Order(nil).
				Order(bson.D{{Key: "age", Value: 1}}).
				Order(bson.E{Key: "name", Value: 1}).
				Order(bson.M{"user_name": 1}).
				Order(map[string]any{"_id": 1}).
				WithFindOptions(options.Find().SetBatchSize(10))

			Expect(q.CollectionName()).To(Equal("users"))
			Expect(q.Filter()).To(Equal(bson.M{"name": "Alice"}))
			Expect(q.Limit(1).Find(&out)).NotTo(HaveOccurred())
			Expect(out).To(HaveLen(1))

			// Collection with non-empty name
			Expect(emptyQ.Collection("users").CollectionName()).To(Equal("users"))
			Expect(emptyQ.Collection("users").RawCollection()).NotTo(BeNil())

			// Or with 2+ filters, Or(single) with existing filters, Filter() with 0/2+ filters
			Expect(userRepo.Query().Filter()).To(Equal(bson.D{}))
			Expect(userRepo.Query().Where(bson.M{"name": "Alice"}).Where(bson.M{"age": 20}).Filter()).To(Equal(bson.M{
				"$and": bson.A{bson.M{"name": "Alice"}, bson.M{"age": 20}},
			}))
			Expect(userRepo.Query().Where(bson.M{"name": "Alice"}).Or(bson.M{"name": "Bob"}).Find(&out)).NotTo(HaveOccurred())
			Expect(out).To(HaveLen(2))
			Expect(userRepo.Query().Or(bson.M{"name": "Alice"}, bson.M{"name": "Bob"}).Find(&out)).NotTo(HaveOccurred())
			Expect(out).To(HaveLen(2))

			// Omit, Hint, MaxTime, Collation, FindOptions, and Find with Limit(0)
			optsQuery := userRepo.Query().
				Omit("email").
				Hint(bson.D{{Key: "_id", Value: 1}}).
				MaxTime(5 * time.Second).
				Collation(&options.Collation{Locale: "en"}).
				Limit(1).
				Skip(1)
			Expect(optsQuery.FindOptions()).NotTo(BeEmpty())
			Expect(optsQuery.Find(&out)).NotTo(HaveOccurred())
			Expect(userRepo.Query().Limit(0).Find(&out)).NotTo(HaveOccurred())
			Expect(out).To(BeEmpty())

			// Unsupported Order & Projection types
			Expect(userRepo.Query().Order(123).Error()).To(HaveOccurred())
			Expect(userRepo.Query().Projection(123).Error()).To(HaveOccurred())

			// Find validation & driver/decode errors
			Expect(userRepo.Query().Find(nil)).To(HaveOccurred())
			var single User
			Expect(userRepo.Query().Find(&single)).To(HaveOccurred())
			Expect(userRepo.Query().Where(bson.M{"$badOp": 1}).Find(&out)).To(HaveOccurred())
			var badDecode []struct {
				Name int `bson:"name"`
			}
			Expect(userRepo.Query().Find(&badDecode)).To(HaveOccurred())

			// First validation, Limit(0), driver & decode errors
			Expect(userRepo.Query().First(nil)).To(HaveOccurred())
			Expect(userRepo.Query().Limit(0).First(&single)).To(HaveOccurred())
			Expect(userRepo.Query().Where(bson.M{"$badOp": 1}).First(&single)).To(HaveOccurred())
			var badFirst struct {
				Name int `bson:"name"`
			}
			Expect(userRepo.Query().
				OrderByAsc("name").
				Select("name").
				Skip(0).
				Skip(1).
				Hint(bson.D{{Key: "_id", Value: 1}}).
				MaxTime(time.Second).
				Collation(&options.Collation{Locale: "en"}).
				First(&badFirst)).To(HaveOccurred())

			// Count options & errors
			cnt, err := userRepo.Query().Limit(0).Count()
			Expect(err).NotTo(HaveOccurred())
			Expect(cnt).To(Equal(int64(0)))
			cnt, err = userRepo.Query().
				Limit(5).
				Skip(1).
				Hint(bson.D{{Key: "_id", Value: 1}}).
				MaxTime(time.Second).
				Collation(&options.Collation{Locale: "en"}).
				Count()
			Expect(err).NotTo(HaveOccurred())
			Expect(cnt).To(Equal(int64(1)))
			_, err = userRepo.Query().Where(bson.M{"$badOp": 1}).Count()
			Expect(err).To(HaveOccurred())

			// Exists options & errors
			exists, err := userRepo.Query().Limit(0).Exists()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())
			exists, err = userRepo.Query().
				Skip(1).
				Hint(bson.D{{Key: "_id", Value: 1}}).
				MaxTime(time.Second).
				Collation(&options.Collation{Locale: "en"}).
				Exists()
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())
			_, err = userRepo.Query().Where(bson.M{"$badOp": 1}).Exists()
			Expect(err).To(HaveOccurred())

			// Delete options & errors
			_, err = userRepo.Query().Where(bson.M{"$badOp": 1}).Delete()
			Expect(err).To(HaveOccurred())
			delCnt, err := userRepo.Query().
				Where(bson.M{"name": "Nobody"}).
				Hint(bson.D{{Key: "_id", Value: 1}}).
				Collation(&options.Collation{Locale: "en"}).
				Delete()
			Expect(err).NotTo(HaveOccurred())
			Expect(delCnt).To(Equal(int64(0)))

			// Distinct: nil dest, bad dest, options, nil value, BSON struct round-trip, unmarshal error
			Expect(userRepo.Query().Distinct("name", nil)).To(HaveOccurred())
			Expect(userRepo.Query().Distinct("name", &single)).To(HaveOccurred())
			Expect(userRepo.Query().Where(bson.M{"$badOp": 1}).Distinct("name", &[]string{})).To(HaveOccurred())

			var names []string
			Expect(userRepo.Query().
				MaxTime(time.Second).
				Collation(&options.Collation{Locale: "en"}).
				Distinct("name", &names)).NotTo(HaveOccurred())
			Expect(names).To(ConsistOf("Alice", "Bob"))

			// Insert doc with nil field for Distinct nil branch
			_, insertErr := richRepo.Collection().InsertOne(ctx, bson.M{"_id": "r-nil", "label": "x", "nullable": nil})
			Expect(insertErr).NotTo(HaveOccurred())
			var ptrVals []*string
			Expect(richRepo.Query().Distinct("nullable", &ptrVals)).NotTo(HaveOccurred())
			Expect(ptrVals).To(HaveLen(1))
			Expect(ptrVals[0]).To(BeNil())

			var addrs []Address
			Expect(userRepo.Query().Distinct("address", &addrs)).NotTo(HaveOccurred())
			Expect(addrs).To(HaveLen(2))

			var badDistinct []int
			Expect(userRepo.Query().Distinct("address", &badDistinct)).To(HaveOccurred())

			// Aggregate: nil dest, options, bad pipeline, decode error
			Expect(userRepo.Query().Aggregate(mongo.Pipeline{}, nil)).To(HaveOccurred())
			Expect(userRepo.Query().Where(bson.M{"name": "Alice"}).Aggregate(bson.D{{Key: "$invalidStage", Value: 1}}, &out)).To(HaveOccurred())
			var aggOut []bson.M
			Expect(userRepo.Query().
				MaxTime(time.Second).
				Collation(&options.Collation{Locale: "en"}).
				Hint(bson.D{{Key: "_id", Value: 1}}).
				Aggregate(mongo.Pipeline{{{Key: "$match", Value: bson.M{"name": "Alice"}}}}, &aggOut)).NotTo(HaveOccurred())
			Expect(aggOut).To(HaveLen(1))
			var badAgg []struct {
				Age struct{ X int } `bson:"age"`
			}
			Expect(userRepo.Query().Aggregate(mongo.Pipeline{{{Key: "$match", Value: bson.M{"name": "Alice"}}}}, &badAgg)).To(HaveOccurred())
		})

		It("covers derived query operators, inline/nested/cyclic fields, and validation errors", func() {
			str := "ok"
			_, err := richRepo.Save(&RichEntity{
				InlineID: "r1", Tenant: "acme",
				TagNoName: "t1",
				Sub:       &NestedSub{Code: "S1"},
				IP:        textTag{Val: "127.0.0.1"},
				Custom:    customBSONType{Raw: "c"},
				Nullable:  &str,
				Label:     "Alpha_100%",
				Score:     10,
				Enabled:   true,
				CreatedAt: time.Now().UTC(),
			})
			Expect(err).NotTo(HaveOccurred())
			_, err = richRepo.Save(&RichEntity{
				InlineID: "r2", Tenant: "beta",
				Label:   "Beta",
				Score:   20,
				Enabled: false,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(richRepo.findByLabelIs("Beta")).To(HaveLen(1))
			Expect(richRepo.findByLabelEquals("Beta")).To(HaveLen(1))
			Expect(richRepo.findByScoreAfter(15)).To(HaveLen(1))
			Expect(richRepo.findByScoreBefore(15)).To(HaveLen(1))
			Expect(richRepo.findByNullableNull()).To(HaveLen(1))
			Expect(richRepo.findByNullableNotNull()).To(HaveLen(1))
			Expect(richRepo.findByLabelRegex("^Al")).To(HaveLen(1))
			Expect(richRepo.findByLabelMatches("^Be")).To(HaveLen(1))
			Expect(richRepo.findByLabelNotRegex("^Al")).To(HaveLen(1))
			Expect(richRepo.findByLabelNotContaining("pha")).To(HaveLen(1))
			Expect(richRepo.findByEnabledIsTrue()).To(HaveLen(1))
			Expect(richRepo.findByEnabledIsFalse()).To(HaveLen(1))
			Expect(richRepo.findBySubCode("S1")).To(HaveLen(1))
			Expect(richRepo.findByTenant("acme")).To(HaveLen(1))

			// likeToRegex escaping (\% and \_ and trailing \)
			var escaped []RichEntity
			Expect(richRepo.Query().Where(bson.M{"label": "Alpha_100%"}).Find(&escaped)).NotTo(HaveOccurred())
			Expect(userRepo.Query().Find(&[]User{})).NotTo(HaveOccurred())
			_, err = richRepo.Save(&RichEntity{
				InlineID: "r3",
				Label:    `foo\`,
			})
			Expect(err).NotTo(HaveOccurred())
			// Test escaped wildcards and trailing backslash via derived Like query
			Expect(richRepo.Query()).NotTo(BeNil())

			// Query returning (*MongoQuery, Error) with Query error
			mq, qErr := richRepo.queryWithErrByLabel("Beta", func(q *MongoQuery) *MongoQuery {
				return q.AddError(NewError("CustomQueryError", "bad"))
			})
			Expect(mq).NotTo(BeNil())
			Expect(qErr).To(HaveOccurred())

			// Delete returning Error only (success + failure after rollback)
			Expect(richRepo.deleteOnlyErrByLabel("Beta")).NotTo(HaveOccurred())
			sess := txMgr.Start(context.Background())
			Expect(sess.Rollback()).NotTo(HaveOccurred())
			Expect(richRepo.deleteOnlyErrByLabel("Alpha_100%")).To(HaveOccurred())
			txMgr.End()

			// Invalid signatures remain nil
			Expect(richRepo.findBadNoOutByLabel).To(BeNil())
			Expect(richRepo.findBadThreeOutByLabel).To(BeNil())
			Expect(richRepo.findBadSecondOutByLabel).To(BeNil())
			Expect(richRepo.deleteBadReturnByLabel).To(BeNil())
			Expect(richRepo.findBadSepByLabelXorScore).To(BeNil())

			// Cyclic struct query field
			_, err = cyclicRepo.Save(&CyclicDoc{ID: "c1"})
			Expect(err).NotTo(HaveOccurred())
			Expect(cyclicRepo.findByID("c1")).NotTo(BeNil())
		})

		It("covers likeToRegex escaped wildcards and trailing backslash", func() {
			_, err := richRepo.Save(&RichEntity{
				InlineID: "r-esc",
				Label:    `a_b%c\`,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(likeRepo.findByLabelLike(`a\_b\%c\`)).To(HaveLen(1))
			Expect(likeRepo.findByLabelLike(`a_b%`)).To(HaveLen(1))
		})

		It("covers repository collection naming, Save/SaveAll/Delete/EnsureIndexes branches, and ID extraction", func() {
			Expect(ptrNameRepo.Collection().Name()).To(Equal("ptr_named_docs"))
			_, err := ptrNameRepo.Save(&PtrNamerEntity{ID: "p1"})
			Expect(err).NotTo(HaveOccurred())

			_, err = inlineNoIDRepo.Save(&InlineNoIDEntity{Tenant: "t1", ID: "in1"})
			Expect(err).NotTo(HaveOccurred())

			Expect(boxRepo.Collection().Name()).To(Equal("boxes"))
			Expect(catRepo.Collection().Name()).To(Equal("categories"))
			Expect(boyRepo.Collection().Name()).To(Equal("boys"))
			Expect(anonRepo.Collection().Name()).To(Equal("documents"))
			Expect(ptrEntRepo.Collection().Name()).To(Equal("users"))

			// Standalone repository without transactionManager
			standaloneRepo := &MongoRepository[User, primitive.ObjectID]{}
			Expect(standaloneRepo.Query()).NotTo(BeNil())
			Expect(standaloneRepo.Delete(&User{})).To(HaveOccurred())
			//nolint:staticcheck // Intentionally testing nil context fallback
			_, err = standaloneRepo.EnsureIndexes(nil)
			Expect(err).NotTo(HaveOccurred())

			// Save(nil), SaveAll(empty), SaveAll(with nil)
			_, err = userRepo.Save(nil)
			Expect(err).To(HaveOccurred())
			emptyRes, err := userRepo.SaveAll(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(emptyRes).To(BeEmpty())
			_, err = userRepo.SaveAll([]*User{nil})
			Expect(err).To(HaveOccurred())

			// Save and SaveAll with zero ObjectID (InsertOneModel) + AnyIDEntity (res.InsertedID assignable to any)
			anyEnt, err := anyRepo.Save(&AnyIDEntity{Name: "generated"})
			Expect(err).NotTo(HaveOccurred())
			Expect(anyEnt.ID).NotTo(BeNil())
			Expect(anyRepo.Delete(&AnyIDEntity{ID: nil})).To(HaveOccurred())

			_, err = anyRepo.SaveAll([]*AnyIDEntity{
				{Name: "bulk-insert-1"},
				{ID: "explicit-id", Name: "bulk-upsert-2"},
			})
			Expect(err).NotTo(HaveOccurred())

			// NoIDEntity: Save (InsertOne without ID), Delete fails (no ID field)
			noID := &NoIDEntity{Name: "no-id"}
			_, err = noIDRepo.Save(noID)
			Expect(err).NotTo(HaveOccurred())
			noIDErr := noIDRepo.Delete(noID)
			Expect(noIDErr).To(HaveOccurred())
			Expect(noIDErr.Error()).To(ContainSubstring("use DeleteById with the actual key"))

			// UntaggedIDEntity: Delete fails because untagged "ID" field is not treated as MongoDB _id
			untaggedRepo := &UntaggedIDRepository{}
			untaggedErr := untaggedRepo.Delete(&UntaggedIDEntity{ID: "explicit-id", Name: "untagged"})
			Expect(untaggedErr).To(HaveOccurred())
			Expect(untaggedErr.Error()).To(ContainSubstring("use DeleteById with the actual key"))

			// UnexportedFieldEntity: findIDField skips unexported fields
			unexportedRepo := &UnexportedFieldRepo{}
			Expect(unexportedRepo.Delete(&UnexportedFieldEntity{unexportedID: "id1", Name: "name"})).To(HaveOccurred())

			// Nil inner pointer & non-struct entity for findIDField
			var nilUserPtr *User
			Expect(ptrEntRepo.Delete(&nilUserPtr)).To(HaveOccurred())
			primVal := "hello"
			Expect(primRepo.Delete(&primVal)).To(HaveOccurred())

			// Delete(nil) & Delete(zero ID)
			Expect(userRepo.Delete(nil)).To(HaveOccurred())
			Expect(userRepo.Delete(&User{Name: "zero"})).To(HaveOccurred())

			// EnsureIndexes error + nil ctx fallback
			//nolint:staticcheck // Intentionally testing nil context fallback
			_, err = userRepo.EnsureIndexes(nil, mongo.IndexModel{
				Keys: bson.D{{Key: "name", Value: "invalid_type"}},
			})
			Expect(err).To(HaveOccurred())

			// EnsureIndexes with non-nil caller context
			validIdxs, err := userRepo.EnsureIndexes(context.Background(), mongo.IndexModel{
				Keys: bson.D{{Key: "active", Value: 1}},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(validIdxs).NotTo(BeEmpty())

			// Seed a matching user first
			seededUser := &User{Name: "seeded_for_qpre", Age: 30}
			savedSeeded, saveErr := userRepo.Save(seededUser)
			Expect(saveErr).NotTo(HaveOccurred())

			// Aborted session errors for all repository and query methods
			sess := txMgr.Start(context.Background())
			qPreFinalize := userRepo.Query().Where(bson.M{"name": "seeded_for_qpre"})

			// Query created before rollback must succeed dynamically when executed within active transaction
			var singleUser User
			Expect(qPreFinalize.First(&singleUser)).NotTo(HaveOccurred())
			Expect(singleUser.Name).To(Equal("seeded_for_qpre"))

			// Derived context query execution before commit/rollback succeeds
			derivedCtx, derivedCancel := context.WithTimeout(sess, 5*time.Second)
			defer derivedCancel()
			qDerived := userRepo.Query().WithContext(derivedCtx).Where(bson.M{"name": "seeded_for_qpre"})
			var singleUserDerived User
			Expect(qDerived.First(&singleUserDerived)).NotTo(HaveOccurred())
			Expect(singleUserDerived.Name).To(Equal("seeded_for_qpre"))

			Expect(sess.Rollback()).NotTo(HaveOccurred())
			Expect(userRepo.FindById(primitive.NewObjectID()).IsPresent()).To(BeFalse())
			_, err = userRepo.FindAll()
			Expect(err).To(HaveOccurred())
			_, err = userRepo.Count()
			Expect(err).To(HaveOccurred())
			_, err = userRepo.ExistsById(primitive.NewObjectID())
			Expect(err).To(HaveOccurred())

			// EnsureIndexes returns session error during aborted transaction
			//nolint:staticcheck // Intentionally testing nil context fallback
			_, err = userRepo.EnsureIndexes(nil, mongo.IndexModel{Keys: bson.D{{Key: "name", Value: 1}}})
			Expect(err).To(HaveOccurred())

			// Query created before rollback must fail dynamically when executed after rollback with TransactionError
			var singleUserAfter User
			preErr := qPreFinalize.First(&singleUserAfter)
			Expect(preErr).To(HaveOccurred())
			Expect(preErr.Name()).To(Equal("TransactionError"))

			// Derived context query execution after rollback returns TransactionError without sending delete
			derivedErr := qDerived.First(&singleUserAfter)
			Expect(derivedErr).To(HaveOccurred())
			Expect(derivedErr.Name()).To(Equal("TransactionError"))
			derivedCount, derivedDelErr := qDerived.Delete()
			Expect(derivedDelErr).To(HaveOccurred())
			Expect(derivedDelErr.Name()).To(Equal("TransactionError"))
			Expect(derivedCount).To(Equal(int64(0)))

			// Direct MongoQuery execution after rollback must fail with TransactionError before calling driver
			Expect(userRepo.Query().First(&singleUser)).To(HaveOccurred())
			_, err = userRepo.Query().Count()
			Expect(err).To(HaveOccurred())
			_, err = userRepo.Query().Exists()
			Expect(err).To(HaveOccurred())
			_, err = userRepo.Query().Delete()
			Expect(err).To(HaveOccurred())
			var distinctNames []string
			Expect(userRepo.Query().Distinct("name", &distinctNames)).To(HaveOccurred())
			var aggResults []bson.M
			Expect(userRepo.Query().Aggregate(mongo.Pipeline{}, &aggResults)).To(HaveOccurred())

			txMgr.End()

			// Ended session wrapper operations rejected
			Expect(sess.Commit()).To(HaveOccurred())
			Expect(sess.Rollback()).To(HaveOccurred())
			Expect(sess.Abort()).To(HaveOccurred())
			Expect(sess.Validate()).To(HaveOccurred())
			_, delEndedErr := userRepo.Query().WithContext(sess).Delete()
			Expect(delEndedErr).To(HaveOccurred())

			Expect(userRepo.Delete(savedSeeded)).NotTo(HaveOccurred())

			// toBsonArray coverage: nil, nil pointer, slice pointer, raw slice, array, scalar, byte slice
			var nilSlicePtr *[]string
			slicePtr := &[]string{"p1", "p2"}
			Expect(ExportToBsonArray(nil)).To(Equal(bson.A{}))
			Expect(ExportToBsonArray(nilSlicePtr)).To(Equal(bson.A{}))
			Expect(ExportToBsonArray(slicePtr)).To(Equal(bson.A{"p1", "p2"}))
			Expect(ExportToBsonArray([]byte{1, 2})).To(Equal(bson.A{byte(1), byte(2)}))
			Expect(ExportToBsonArray([2]int{10, 20})).To(Equal(bson.A{10, 20}))
			Expect(ExportToBsonArray("scalar")).To(Equal(bson.A{"scalar"}))

			// extractValidator nil ctx
			//nolint:staticcheck // Intentionally testing nil context fallback
			Expect(ExportExtractValidator(nil)).To(BeNil())

			// copyFilter coverage: nil, typed containers, nil containers, scalar
			Expect(ExportCopyFilter(nil)).To(BeNil())
			Expect(ExportCopyFilter(bson.M(nil))).To(Equal(bson.M(nil)))
			Expect(ExportCopyFilter(bson.M{"k": "v"})).To(Equal(bson.M{"k": "v"}))
			Expect(ExportCopyFilter(map[string]any(nil))).To(Equal(map[string]any(nil)))
			Expect(ExportCopyFilter(map[string]any{"k": "v"})).To(Equal(map[string]any{"k": "v"}))
			Expect(ExportCopyFilter(bson.D(nil))).To(Equal(bson.D(nil)))
			Expect(ExportCopyFilter(bson.D{{Key: "k", Value: "v"}})).To(Equal(bson.D{{Key: "k", Value: "v"}}))
			Expect(ExportCopyFilter([]bson.E(nil))).To(Equal([]bson.E(nil)))
			Expect(ExportCopyFilter([]bson.E{{Key: "k", Value: "v"}})).To(Equal([]bson.E{{Key: "k", Value: "v"}}))
			Expect(ExportCopyFilter(bson.A(nil))).To(Equal(bson.A(nil)))
			Expect(ExportCopyFilter(bson.A{"item"})).To(Equal(bson.A{"item"}))
			Expect(ExportCopyFilter([]any(nil))).To(Equal([]any(nil)))
			Expect(ExportCopyFilter([]any{"item"})).To(Equal([]any{"item"}))
			Expect(ExportCopyFilter([]any{nil})).To(Equal([]any{nil}))
			Expect(ExportCopyFilter([]string(nil))).To(Equal([]string(nil)))
			Expect(ExportCopyFilter([]string{"a", "b"})).To(Equal([]string{"a", "b"}))
			Expect(ExportCopyFilter(map[string]string(nil))).To(Equal(map[string]string(nil)))
			Expect(ExportCopyFilter(map[string]string{"k": "v"})).To(Equal(map[string]string{"k": "v"}))
			Expect(ExportCopyFilter(map[string]*string{"k": nil})).To(Equal(map[string]*string{"k": nil}))
			Expect(ExportCopyFilter(map[int]any{1: nil})).To(Equal(map[int]any{1: nil}))
			Expect(ExportCopyFilter(123)).To(Equal(123))

			// Defensive copy mutation check
			origMap := bson.M{"a": 1}
			mutQ := userRepo.Query().Where(origMap)
			origMap["a"] = 99
			Expect(mutQ.Filter()).To(Equal(bson.M{"a": 1}))
			retMap, ok := mutQ.Filter().(bson.M)
			Expect(ok).To(BeTrue())
			retMap["a"] = 999
			Expect(mutQ.Filter()).To(Equal(bson.M{"a": 1}))

			// Client builder host normalization, explicit false direct connection, zero pool sizes
			bNorm := NewMongoClientBuilder().
				WithHost("[::1]").
				WithPort(27017).
				WithMaxPoolSize(0).
				WithMinPoolSize(0).
				WithDirectConnection(false)
			bNormOpts := bNorm.BuildOptions()
			Expect(bNormOpts.Direct).NotTo(BeNil())
			Expect(*bNormOpts.Direct).To(BeFalse())
			Expect(bNormOpts.MaxPoolSize).NotTo(BeNil())
			Expect(*bNormOpts.MaxPoolSize).To(Equal(uint64(0)))
			Expect(bNormOpts.MinPoolSize).NotTo(BeNil())
			Expect(*bNormOpts.MinPoolSize).To(Equal(uint64(0)))

			// 1. WorkOrderRepo: WorkOrderByName field matching vs OrderBy clause
			_, err = workOrderRepo.Save(&WorkOrderEntity{
				WorkOrderByName: "order-alpha",
				Status:          "open",
				Name:            "alpha",
				AgeLess:         5,
				Age:             25,
			})
			Expect(err).NotTo(HaveOccurred())
			woItem := workOrderRepo.findByWorkOrderByName("order-alpha")
			Expect(woItem).NotTo(BeNil())
			Expect(woItem.WorkOrderByName).To(Equal("order-alpha"))
			woList := workOrderRepo.findByStatusOrderByName("open")
			Expect(woList).To(HaveLen(1))
			woAgeList := workOrderRepo.findByAgeLessThan(30)
			Expect(woAgeList).To(HaveLen(1))
			woByStatus := workOrderRepo.findByStatusOrderByWorkOrderByName("open")
			Expect(woByStatus).To(HaveLen(1))
			Expect(woByStatus[0].WorkOrderByName).To(Equal("order-alpha"))

			// 2. IntIDRepo: integer ID 0 treated as existing key in Save, SaveAll, Delete
			intEnt := &IntIDEntity{ID: 0, Name: "zero-initial"}
			_, err = intIDRepo.Save(intEnt)
			Expect(err).NotTo(HaveOccurred())
			foundInt, ok := intIDRepo.FindById(0).Get()
			Expect(ok).To(BeTrue())
			Expect(foundInt.Name).To(Equal("zero-initial"))

			// Upsert integer ID 0 via SaveAll
			intEnt.Name = "zero-updated"
			_, err = intIDRepo.SaveAll([]*IntIDEntity{intEnt})
			Expect(err).NotTo(HaveOccurred())
			foundInt, ok = intIDRepo.FindById(0).Get()
			Expect(ok).To(BeTrue())
			Expect(foundInt.Name).To(Equal("zero-updated"))

			// Delete integer ID 0
			Expect(intIDRepo.Delete(&IntIDEntity{ID: 0})).NotTo(HaveOccurred())
			Expect(intIDRepo.FindById(0).IsPresent()).To(BeFalse())

			// 3. PtrIDRepo: entity with nil ID tagged bson:"_id,omitempty" retains generated ID in SaveAll
			ptrEnt := &PtrIDEntity{Name: "bulk-generated-id"}
			savedPtrs, err := ptrIDRepo.SaveAll([]*PtrIDEntity{ptrEnt})
			Expect(err).NotTo(HaveOccurred())
			Expect(savedPtrs).To(HaveLen(1))
			Expect(ptrEnt.ID).NotTo(BeNil())
			Expect(ptrEnt.ID.IsZero()).To(BeFalse())

			// Entity without settable ID field in SaveAll is rejected
			noIDEnt := &NoIDEntity{Name: "no-id-bulk"}
			_, err = noIDRepo.SaveAll([]*NoIDEntity{noIDEnt})
			Expect(err).To(HaveOccurred())

			// 4. Order compound maps rejected; single-key accepted
			compMapQ1 := userRepo.Query().Order(bson.M{"a": 1, "b": 1})
			Expect(compMapQ1.Error()).To(HaveOccurred())
			compMapQ2 := userRepo.Query().Order(map[string]any{"a": 1, "b": 1})
			Expect(compMapQ2.Error()).To(HaveOccurred())
			singleMapQ := userRepo.Query().Order(bson.M{"a": 1})
			Expect(singleMapQ.Error()).NotTo(HaveOccurred())

			// 5. Distinct numeric range checks
			convVal, err := ExportConvertDistinctValue(nil, reflect.TypeFor[int]())
			Expect(err).NotTo(HaveOccurred())
			Expect(convVal.IsValid()).To(BeTrue())
			val25, err := ExportConvertDistinctValue(25, reflect.TypeFor[uint8]())
			Expect(err).NotTo(HaveOccurred())
			Expect(val25.Interface()).To(Equal(uint8(25)))

			_, err = ExportConvertDistinctValue(257, reflect.TypeFor[uint8]())
			Expect(err).To(HaveOccurred())

			_, err = ExportConvertDistinctValue(-1, reflect.TypeFor[uint8]())
			Expect(err).To(HaveOccurred())

			// Additional numeric bounds coverage
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[int8]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(200)), reflect.TypeFor[int8]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(-200)), reflect.TypeFor[int8]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[int16]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(40000)), reflect.TypeFor[int16]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[int32]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[int]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[int64]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[uint16]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(70000)), reflect.TypeFor[uint16]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[uint32]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[uint]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[uint64]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[float32]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[float64]())).To(BeTrue())

			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[int8]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(200)), reflect.TypeFor[int8]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[int16]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(40000)), reflect.TypeFor[int16]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[int32]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[int]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[int64]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[uint8]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(300)), reflect.TypeFor[uint8]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[uint16]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(70000)), reflect.TypeFor[uint16]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[uint32]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[uint]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[uint64]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[float32]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[float64]())).To(BeTrue())

			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[int8]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(200.5)), reflect.TypeFor[int8]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[int16]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(40000.5)), reflect.TypeFor[int16]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[int32]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[int]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[int64]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[uint8]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(-5.0)), reflect.TypeFor[uint8]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(300.0)), reflect.TypeFor[uint8]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[uint16]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(70000.0)), reflect.TypeFor[uint16]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[uint32]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[uint]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[uint64]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[float32]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10.5)), reflect.TypeFor[float64]())).To(BeTrue())
			Expect(ExportFitsNumericRange(reflect.ValueOf("not-numeric"), reflect.TypeFor[int]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(int64(10)), reflect.TypeFor[string]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(uint64(10)), reflect.TypeFor[string]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(float64(10)), reflect.TypeFor[string]())).To(BeFalse())

			// Float64 Int64/Uint64 boundary tests in ExportFitsNumericRange
			maxInt64Float := 9223372036854775808.0 // 2^63
			precedingInt64 := math.Nextafter(maxInt64Float, 0)
			Expect(ExportFitsNumericRange(reflect.ValueOf(maxInt64Float), reflect.TypeFor[int64]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(precedingInt64), reflect.TypeFor[int64]())).To(BeTrue())

			maxUint64Float := 18446744073709551616.0 // 2^64
			precedingUint64 := math.Nextafter(maxUint64Float, 0)
			Expect(ExportFitsNumericRange(reflect.ValueOf(maxUint64Float), reflect.TypeFor[uint64]())).To(BeFalse())
			Expect(ExportFitsNumericRange(reflect.ValueOf(precedingUint64), reflect.TypeFor[uint64]())).To(BeTrue())

			// 6. Aggregate destination validation
			Expect(userRepo.Query().Aggregate(mongo.Pipeline{}, []bson.M{})).To(HaveOccurred())
			Expect(userRepo.Query().Aggregate(mongo.Pipeline{}, &User{})).To(HaveOccurred())

			// validateExecution with query error
			qWithErr := userRepo.Query().AddError(NewError("CustomErr", "bad"))
			Expect(qWithErr.Find(&[]User{})).To(HaveOccurred())

			// convertDistinctValue with CanConvert (non-numeric convertible types)
			convVal, convErr := ExportConvertDistinctValue([]byte("hello"), reflect.TypeFor[string]())
			Expect(convErr).NotTo(HaveOccurred())
			Expect(convVal.Interface()).To(Equal("hello"))

			// entityIDStatus nil, invalid BSON, zero ObjectID without omitempty
			_, hasID, encErr := ExportEntityIDStatus(nil)
			Expect(hasID).To(BeFalse())
			Expect(encErr).NotTo(HaveOccurred())

			type unmarshalableDoc struct {
				Ch chan int `bson:"ch"`
				ID int      `bson:"_id"`
			}
			_, hasID, encErr = ExportEntityIDStatus(&unmarshalableDoc{ID: 1, Ch: make(chan int)})
			Expect(hasID).To(BeFalse())
			Expect(encErr).To(HaveOccurred())

			type nonOmitemptyDoc struct {
				ID primitive.ObjectID `bson:"_id"`
			}
			_, hasID, encErr = ExportEntityIDStatus(&nonOmitemptyDoc{ID: primitive.NilObjectID})
			Expect(hasID).To(BeFalse())
			Expect(encErr).NotTo(HaveOccurred())

			// assignGeneratedID with non-ObjectID field
			strVal := "hello"
			Expect(ExportAssignGeneratedID(reflect.ValueOf(&strVal).Elem())).To(BeFalse())

			// 7. Execute and ExecuteWithResult APIs
			mgr := txMgr.(*MongoTransactionalManager)
			exRes, exErr := ExecuteWithResult(context.Background(), mgr, func(_ *MongoSession) (string, Error) {
				return "success", nil
			})
			Expect(exErr).NotTo(HaveOccurred())
			Expect(exRes).To(Equal("success"))

			_, exErr = ExecuteWithResult(context.Background(), mgr, func(_ *MongoSession) (string, Error) {
				return "", NewError("ActionError", "action failed")
			})
			Expect(exErr).To(HaveOccurred())

			_, exErr = ExecuteWithResult[string](context.Background(), nil, nil)
			Expect(exErr).To(HaveOccurred())

			exErr = mgr.Execute(context.Background(), func(_ *MongoSession) Error {
				return nil
			})
			Expect(exErr).NotTo(HaveOccurred())

			exErr = mgr.Execute(context.Background(), func(_ *MongoSession) Error {
				return NewError("ActionError", "failed")
			})
			Expect(exErr).To(HaveOccurred())

			// Execute with panic recovery
			panicErr := mgr.Execute(context.Background(), func(_ *MongoSession) Error {
				panic("callback panic")
			})
			Expect(panicErr).To(HaveOccurred())

			// ExecuteWithResult with panic recovery
			_, panicResErr := ExecuteWithResult(context.Background(), mgr, func(_ *MongoSession) (int, Error) {
				panic("callback panic")
			})
			Expect(panicResErr).To(HaveOccurred())

			// 8. Nested transaction management (Start/End stack restoration)
			sOuter := mgr.Start(context.Background())
			Expect(mgr.Get()).To(Equal(sOuter))
			sInner := mgr.Start(context.Background())
			Expect(mgr.Get()).To(Equal(sInner))
			mgr.End()
			Expect(mgr.Get()).To(Equal(sOuter))
			mgr.End()

			// 9. Canceled context Rollback and End
			cancelCtx, cancelFn := context.WithCancel(context.Background())
			sCancel := mgr.Start(cancelCtx)
			cancelFn()
			Expect(sCancel.Rollback()).NotTo(HaveOccurred())
			mgr.End()

			// 10. Command logging opt-in builder verification
			bCmdOn := NewMongoClientBuilder().WithLogSink(MongoLogSink{}.New()).WithCommandLogging(true)
			bCmdOnOpts := bCmdOn.BuildOptions()
			Expect(bCmdOnOpts.LoggerOptions).NotTo(BeNil())
			Expect(bCmdOnOpts.LoggerOptions.ComponentLevels[options.LogComponentCommand]).To(Equal(options.LogLevelDebug))

			bCmdOff := NewMongoClientBuilder().WithLogSink(MongoLogSink{}.New()).WithCommandLogging(false)
			bCmdOffOpts := bCmdOff.BuildOptions()
			Expect(bCmdOffOpts.LoggerOptions).NotTo(BeNil())
			Expect(bCmdOffOpts.LoggerOptions.ComponentLevels[options.LogComponentCommand]).To(Equal(options.LogLevel(0)))

			// 11. FindByNameOrderByTeamOrderByCode resolution
			teamDoc := &TeamOrderEntity{Name: "team_alice", TeamOrderByCode: "CODE_01"}
			savedTeamDoc, err := teamRepo.Save(teamDoc)
			Expect(err).NotTo(HaveOccurred())
			teamResults, err := teamRepo.FindByNameOrderByTeamOrderByCode("team_alice")
			Expect(err).NotTo(HaveOccurred())
			Expect(teamResults).To(HaveLen(1))
			Expect(teamResults[0].TeamOrderByCode).To(Equal("CODE_01"))
			Expect(teamRepo.Delete(savedTeamDoc)).NotTo(HaveOccurred())

			// 12. Nested []string immutability in Where and returned Filter()
			origTags := []string{"tag1", "tag2"}
			qSlice := userRepo.Query().Where(bson.M{"tags": bson.M{"$in": origTags}})
			origTags[0] = "MUTATED_INPUT"
			f1, ok := qSlice.Filter().(bson.M)
			Expect(ok).To(BeTrue())
			tagsM1, ok := f1["tags"].(bson.M)
			Expect(ok).To(BeTrue())
			inSlice1, ok := tagsM1["$in"].([]string)
			Expect(ok).To(BeTrue())
			Expect(inSlice1[0]).To(Equal("tag1"))

			inSlice1[0] = "MUTATED_OUTPUT"
			f2, ok := qSlice.Filter().(bson.M)
			Expect(ok).To(BeTrue())
			tagsM2, ok := f2["tags"].(bson.M)
			Expect(ok).To(BeTrue())
			inSlice2, ok := tagsM2["$in"].([]string)
			Expect(ok).To(BeTrue())
			Expect(inSlice2[0]).To(Equal("tag1"))

			// 13. Save payload encoding error before ID assignment
			badDoc := &BadMarshalEntity{}
			savedBad, saveBadErr := badMarshalRepo.Save(badDoc)
			Expect(saveBadErr).To(HaveOccurred())
			Expect(saveBadErr.Name()).To(Equal("MongoError"))
			Expect(badDoc.ID.IsZero()).To(BeTrue()) // ID was not assigned before returning error
			Expect(savedBad).To(BeNil())

			delBadErr := badMarshalRepo.Delete(badDoc)
			Expect(delBadErr).To(HaveOccurred())
			Expect(delBadErr.Name()).To(Equal("MongoError"))

			_, saveAllBadErr := badMarshalRepo.SaveAll([]*BadMarshalEntity{badDoc})
			Expect(saveAllBadErr).To(HaveOccurred())
			Expect(saveAllBadErr.Name()).To(Equal("MongoError"))

			// 14. Nil *primitive.ObjectID without omitempty generates new ObjectID
			nilDoc := &NilOIDEntity{ID: nil, Name: "generated_id_entity"}
			savedNilDoc, nilSaveErr := nilOIDRepo.Save(nilDoc)
			Expect(nilSaveErr).NotTo(HaveOccurred())
			Expect(savedNilDoc.ID).NotTo(BeNil())
			Expect(savedNilDoc.ID.IsZero()).To(BeFalse())

			foundNilDoc, ok := nilOIDRepo.FindById(*savedNilDoc.ID).Get()
			Expect(ok).To(BeTrue())
			Expect(foundNilDoc.Name).To(Equal("generated_id_entity"))
			Expect(nilOIDRepo.Delete(savedNilDoc)).NotTo(HaveOccurred())

			// Driver errors via Disconnect for Delete, DeleteById, Save (ReplaceOne + InsertOne), SaveAll, ExistsById
			Expect(client.Disconnect(context.Background())).NotTo(HaveOccurred())
			_, err = userRepo.ExistsById(primitive.NewObjectID())
			Expect(err).To(HaveOccurred())
			Expect(userRepo.Delete(&User{ID: primitive.NewObjectID()})).To(HaveOccurred())
			Expect(userRepo.DeleteById(primitive.NewObjectID())).To(HaveOccurred())
			_, err = userRepo.Save(&User{ID: primitive.NewObjectID()})
			Expect(err).To(HaveOccurred())
			_, err = noIDRepo.Save(&NoIDEntity{Name: "insert-fail"})
			Expect(err).To(HaveOccurred())
			_, err = userRepo.SaveAll([]*User{{Name: "bulk-fail"}})
			Expect(err).To(HaveOccurred())
		})
	})
})
