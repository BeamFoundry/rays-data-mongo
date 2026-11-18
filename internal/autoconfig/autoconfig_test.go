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
)

func TestAutoconfig(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Internal/Autoconfig Suite")
}

var _ = Describe("MongoAutoConfig", func() {
	It("covers MongoLogSink and MongoTxManager database preference, client fallback, and missing beam error", func() {
		cfg := &MongoAutoConfig{}

		// 1. MongoLogSink
		sink := cfg.MongoLogSink(struct{ ConditionalOnMissingBeam }{})
		Expect(sink).NotTo(BeNil())

		// 2. Both database and client provided -> prefers database
		client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
		Expect(err).NotTo(HaveOccurred())
		db := client.Database("test_db")

		mgrWithDB, err := cfg.MongoTxManager(struct {
			ConditionalOnMissingBeam
			client   Optional[*mongo.Client]   `@:"Inject"`
			database Optional[*mongo.Database] `@:"Inject"`
		}{
			client:   OptionalOf(client),
			database: OptionalOf(db),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgrWithDB).NotTo(BeNil())

		// 3. Only client provided -> falls back to client
		mgrWithClient, err := cfg.MongoTxManager(struct {
			ConditionalOnMissingBeam
			client   Optional[*mongo.Client]   `@:"Inject"`
			database Optional[*mongo.Database] `@:"Inject"`
		}{
			client: OptionalOf(client),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(mgrWithClient).NotTo(BeNil())

		// 4. Neither provided -> returns MongoConfigurationError
		_, err = cfg.MongoTxManager(struct {
			ConditionalOnMissingBeam
			client   Optional[*mongo.Client]   `@:"Inject"`
			database Optional[*mongo.Database] `@:"Inject"`
		}{})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("No *mongo.Client or *mongo.Database Beam available"))
	})
})
