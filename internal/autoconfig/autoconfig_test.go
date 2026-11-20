package autoconfig

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	. "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
	. "github.com/BeamFoundry/rays/pkg/core"
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
	sink  options.LogSink    `@:"Inject"`
	txMgr MongoTransactional `@:"Inject"`
}

var _, _ = any(&sinkConsumer{}).(Stereotype)

var _ = Describe("MongoAutoConfig", func() {
	It("covers MongoLogSink and Prototype MongoTxManager construction", func() {
		cfg := &MongoAutoConfig{}

		// 1. MongoLogSink
		sink := cfg.MongoLogSink(struct{ ConditionalOnMissingBeam }{})
		Expect(sink).NotTo(BeNil())

		// 2. MongoTxManager returns a Prototype MongoTransactionalManager
		mgr := cfg.MongoTxManager(struct{ ConditionalOnMissingBeam }{}, context.Background())
		Expect(mgr).NotTo(BeNil())
	})

	It("preserves custom options.LogSink when registered and resolves Prototype MongoTransactional with *mongo.Database", func() {
		client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = client.Disconnect(context.Background()) })
		db := client.Database("test_db")

		consumer := &sinkConsumer{}
		AddBeamFor(db)
		AddBeamFor(&MongoAutoConfig{})
		AddBeamFor(&customSinkConfig{})
		AddBeamFor(consumer)
		GetContainer().EnableScanning()
		Expect(GetContainer().Initialize()).NotTo(HaveOccurred())
		Expect(consumer.sink).NotTo(BeNil())
		Expect(consumer.sink).To(BeAssignableToTypeOf(&customSink{}))
		Expect(consumer.txMgr).NotTo(BeNil())
		Expect(consumer.txMgr.Database()).To(Equal(db))
		GetContainer().Reset()
	})
})
