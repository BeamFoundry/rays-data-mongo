---
title: "6. Logging Integration"
weight: 6
---

# Logging Integration

The core Rays framework uses a centralized `slog`-based `Logging` engine for structured output and trace
context propagation. Rays Data Mongo bridges MongoDB Go driver command and error logs directly into Rays
via `MongoLogSink` in `pkg/util` (with a default `options.LogSink` Beam registered automatically by `internal/autoconfig`).
Because Rays controls log levels per injected `Logging` instance, you configure MongoDB log verbosity through
standard Rays logging configuration rather than on `MongoClientBuilder` directly.

## Using `MongoClientBuilder.WithLogSink`

Inject the auto-configured `options.LogSink` Beam (backed by `MongoLogSink` in `pkg/util`) and pass it to `WithLogSink`:

```go
package config

import (
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"

    . "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
    . "github.com/BeamFoundry/rays/pkg/core"
    . "github.com/BeamFoundry/rays/pkg/lang"
)

type MongoConfig struct {
    Configuration
    logSink options.LogSink `@:"Inject"`
}

func (c *MongoConfig) Client() (*mongo.Client, Error) {
    return NewMongoClientBuilder().
        WithURI("mongodb://localhost:27017").
        WithLogSink(c.logSink).
        Build()
}

var _, _ = any(&MongoConfig{}).(Stereotype)
```

## Bridging Custom `*mongo.Client` Beams Directly

If you construct `*options.ClientOptions` manually, inject `options.LogSink` (or register `MongoLogSink{}.New()` from `pkg/util` as a Beam) and pass it to `options.Logger()`:

```go
loggerOpts := options.Logger().
    SetSink(c.logSink).
    SetComponentLevel(options.LogComponentCommand, options.LogLevelDebug)

opts := options.Client().
    ApplyURI("mongodb://localhost:27017").
    SetLoggerOptions(loggerOpts)
```
