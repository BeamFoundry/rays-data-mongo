package autoconfig

import (
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	. "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
	. "github.com/BeamFoundry/rays-data-mongo/pkg/util"
	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/lang"
)

// MongoAutoConfig registers default [options.LogSink] and [MongoTransactional] Beams
// when no custom instances are provided.
type MongoAutoConfig struct {
	Configuration
	logger Logging `@:"Inject"`
}

var _, _ = any(&MongoAutoConfig{}).(Stereotype)

// MongoLogSink provides the default [options.LogSink] Beam backed by [MongoLogSink].
func (this *MongoAutoConfig) MongoLogSink(
	_ struct {
		ConditionalOnMissingBeam
	},
) options.LogSink {
	return MongoLogSink{}.New()
}

// MongoTxManager provides the default [MongoTransactional] Beam backed by [MongoTransactionalManager].
func (this *MongoAutoConfig) MongoTxManager(
	that struct {
		ConditionalOnMissingBeam
		client   Optional[*mongo.Client]   `@:"Inject"`
		database Optional[*mongo.Database] `@:"Inject"`
	},
) (MongoTransactional, Error) {
	if c, ok := that.client.Get(); ok && c != nil {
		return NewMongoTransactionalManager(c, this.logger), nil
	}
	if db, ok := that.database.Get(); ok && db != nil {
		return NewMongoTransactionalManager(db.Client(), this.logger), nil
	}
	return nil, NewError("MongoConfigurationError", "No *mongo.Client or *mongo.Database Beam available to configure MongoTransactional")
}
