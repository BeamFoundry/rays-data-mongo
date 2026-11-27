// Package otel provides OpenTelemetry auto-configuration for MongoDB clients
// managed by rays-data-mongo.
//
// Underscore-importing this package registers [MongoOtelConfiguration], which
// provides a [*event.CommandMonitor] beam backed by
// [go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo]
// using the container's [go.opentelemetry.io/otel/trace.TracerProvider] when available.
package otel

import (
	_ "github.com/BeamFoundry/rays" // rays
	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/lang"

	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// MongoOtelCustomizer can be implemented by any Beam to customize the
// [otelmongo.Option] slice used when building the MongoDB OpenTelemetry monitor.
type MongoOtelCustomizer interface {
	CustomizeMongoOtel(opts []otelmongo.Option) []otelmongo.Option
}

type mongoOtelConfig struct {
	commandAttributeDisabled bool
}

// MongoOtelConfiguration provides the default OpenTelemetry [*event.CommandMonitor]
// beam for MongoDB clients.
type MongoOtelConfiguration struct {
	Configuration
	env            Environment                        `@:"Inject"`
	logger         Logging                            `@:"Inject"`
	tracerProvider Optional[oteltrace.TracerProvider] `@:"Inject"`
	customizers    []MongoOtelCustomizer              `@:"Inject"`
}

var _, _ = any(&MongoOtelConfiguration{}).(Stereotype)

// CommandMonitor provides the default [*event.CommandMonitor] Beam backed by
// [otelmongo.NewMonitor], configured from rays.data.mongo.otel, the container's
// [oteltrace.TracerProvider] (when present), and any [MongoOtelCustomizer] beans.
func (this *MongoOtelConfiguration) CommandMonitor(
	_ struct {
		ConditionalOnMissingBeam
	},
) (*event.CommandMonitor, Error) {
	var cfg mongoOtelConfig
	if err := this.env.ConfigUnmarshall("rays.data.mongo.otel", &cfg); err != nil && err.Name() != "KeyNotFoundError" {
		return nil, err
	}

	var opts []otelmongo.Option
	if tp, ok := this.tracerProvider.Get(); ok {
		opts = append(opts, otelmongo.WithTracerProvider(tp))
	}
	if cfg.commandAttributeDisabled {
		opts = append(opts, otelmongo.WithCommandAttributeDisabled(true))
	}
	for _, customizer := range this.customizers {
		opts = customizer.CustomizeMongoOtel(opts)
	}

	this.logger.Debug("Configured OpenTelemetry MongoDB CommandMonitor")
	return otelmongo.NewMonitor(opts...), nil
}

// ClientOptionsCustomizer returns a [tx.MongoClientBuilder.WithOptions] callback
// that installs the given OpenTelemetry [*event.CommandMonitor] on [*options.ClientOptions].
func ClientOptionsCustomizer(monitor *event.CommandMonitor) func(*options.ClientOptions) {
	return func(opts *options.ClientOptions) {
		if opts != nil && monitor != nil {
			opts.SetMonitor(monitor)
		}
	}
}
