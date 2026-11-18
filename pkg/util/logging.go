package util

import (
	"go.mongodb.org/mongo-driver/mongo/options"

	. "github.com/BeamFoundry/rays/pkg/core"
)

// MongoLogSink adapts [Logging] to the MongoDB Go driver's [options.LogSink] interface.
type MongoLogSink struct {
	logger Logging `@:"Inject"`
}

var _ options.LogSink = &MongoLogSink{}

// New creates a new [*MongoLogSink] instance from the receiver value.
func (this MongoLogSink) New() *MongoLogSink {
	return &this
}

// WithLogger initializes or updates the [Logging] instance on the sink.
func (this *MongoLogSink) WithLogger(logger Logging) *MongoLogSink {
	this.logger = logger
	return this
}

// Info logs a non-error driver message mapped from the driver's verbosity level to Rays logging levels.
func (this *MongoLogSink) Info(level int, message string, keysAndValues ...any) {
	if this.logger == nil {
		return
	}
	if level <= 1 {
		this.logger.Debug(message, keysAndValues...)
		return
	}
	this.logger.Trace(message, keysAndValues...)
}

// Error logs a driver error message through the Rays [Logging] instance.
func (this *MongoLogSink) Error(err error, message string, keysAndValues ...any) {
	if this.logger == nil {
		return
	}
	args := make([]any, 0, len(keysAndValues)+2)
	args = append(args, keysAndValues...)
	if err != nil {
		args = append(args, "error", err)
	}
	this.logger.Error(message, args...)
}
