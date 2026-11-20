package autoconfig

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo/options"

	_ "github.com/BeamFoundry/rays" // rays

	. "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
	. "github.com/BeamFoundry/rays-data-mongo/pkg/util"
	. "github.com/BeamFoundry/rays/pkg/core"
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
	return MongoLogSink{}.New().WithLogger(this.logger)
}

// MongoTxManager provides the default [MongoTransactional] Prototype Beam backed by [MongoTransactionalManager].
//
//nolint:revive // Rays Configuration methods require the anonymous condition struct as first argument
func (this *MongoAutoConfig) MongoTxManager(
	_ struct {
		ConditionalOnMissingBeam
	},
	_ context.Context,
) MongoTransactional {
	return MongoTransactionalManager{}.New()
}
