package autoconfig

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/lang"
	. "github.com/BeamFoundry/rays/pkg/testing"
)

func TestAutoconfig(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal/Autoconfig Suite")
}

type customSink struct{}

func (c *customSink) Info(int, string, ...any)    {}
func (c *customSink) Error(error, string, ...any) {}

type customSinkConfig struct {
	Configuration
}

func (c *customSinkConfig) CustomSink() options.LogSink {
	return &customSink{}
}

var _, _ = any(&customSinkConfig{}).(Stereotype)

type sinkConsumer struct {
	Component
	sink options.LogSink `@:"Inject"`
}

var _, _ = any(&sinkConsumer{}).(Stereotype)

var _ = Describe("MongoAutoConfig", func() {
	It("covers MongoLogSink and MongoTxManager database preference, client fallback, and missing beam error", func() {
		cfg := &MongoAutoConfig{}

		// 1. MongoLogSink
		sink := cfg.MongoLogSink(struct{ ConditionalOnMissingBeam }{})
		Expect(sink).NotTo(BeNil())

		// 2. Both database and client provided -> prefers database client over distinct fallback client
		fallbackClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = fallbackClient.Disconnect(context.Background()) })

		dbClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = dbClient.Disconnect(context.Background()) })

		db := dbClient.Database("test_db")

		mgrWithDB, err := cfg.MongoTxManager(struct {
			ConditionalOnMissingBeam
			client   Optional[*mongo.Client]   `@:"Inject"`
			database Optional[*mongo.Database] `@:"Inject"`
		}{
			client:   OptionalOf(fallbackClient),
			database: OptionalOf(db),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgrWithDB).NotTo(BeNil())
		sDB := mgrWithDB.Start(context.Background())
		Expect(sDB.Session()).NotTo(BeNil())
		Expect(sDB.Session().Client()).To(Equal(dbClient))
		mgrWithDB.End()

		// 3. Only client provided -> falls back to client
		mgrWithClient, err := cfg.MongoTxManager(struct {
			ConditionalOnMissingBeam
			client   Optional[*mongo.Client]   `@:"Inject"`
			database Optional[*mongo.Database] `@:"Inject"`
		}{
			client: OptionalOf(fallbackClient),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgrWithClient).NotTo(BeNil())
		sClient := mgrWithClient.Start(context.Background())
		Expect(sClient.Session()).NotTo(BeNil())
		Expect(sClient.Session().Client()).To(Equal(fallbackClient))
		mgrWithClient.End()

		// 4. Neither provided -> returns MongoConfigurationError
		_, err = cfg.MongoTxManager(struct {
			ConditionalOnMissingBeam
			client   Optional[*mongo.Client]   `@:"Inject"`
			database Optional[*mongo.Database] `@:"Inject"`
		}{})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("No *mongo.Client or *mongo.Database Beam available"))
	})

	It("preserves custom options.LogSink when registered and resolves injected options.LogSink without error", func() {
		client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = client.Disconnect(context.Background()) })

		consumer := &sinkConsumer{}
		AddBeamFor(client)
		AddBeamFor(&MongoAutoConfig{})
		AddBeamFor(&customSinkConfig{})
		AddBeamFor(consumer)
		GetContainer().EnableScanning()
		Expect(GetContainer().Initialize()).NotTo(HaveOccurred())
		Expect(consumer.sink).NotTo(BeNil())
		Expect(consumer.sink).To(BeAssignableToTypeOf(&customSink{}))
		GetContainer().Reset()
	})
})
