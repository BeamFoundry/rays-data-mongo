---
title: "3. Client Builder & Multi-Database"
weight: 3
---

# Client Builder & Multi-Database Configuration

Rays Data Mongo provides `MongoClientBuilder` in `pkg/tx` so you can construct one or more `*mongo.Client`
and `*mongo.Database` Beams inside your `Configuration` structs without global `ConfigurationProperties`.

## `MongoClientBuilder` Reference

Create a builder via `NewMongoClientBuilder()`, which defaults to `127.0.0.1:27017` and `10s` connect/server-selection timeouts:

| Method | Description |
|---|---|
| `WithURI(uri string)` | Sets a full MongoDB connection URI (overrides `WithHost` and `WithPort`). |
| `WithHost(host string)` | Sets the MongoDB hostname (default `127.0.0.1`). |
| `WithPort(port int)` | Sets the MongoDB port (default `27017`). |
| `WithCredentials(user, pass, authSource string)` | Sets authentication credentials via `options.Credential`. |
| `WithConnectTimeout(d time.Duration)` | Sets connection and startup ping timeout (default `10s`). |
| `WithServerSelectionTimeout(d time.Duration)` | Sets server selection timeout (default `10s`). |
| `WithMaxPoolSize(size uint64)` | Sets maximum connection pool size. |
| `WithMinPoolSize(size uint64)` | Sets minimum connection pool size. |
| `WithDirectConnection(direct bool)` | Enables direct single-host connection mode (useful for single-node replica sets). |
| `WithLogSink(sink options.LogSink)` | Sets a custom `options.LogSink` (such as the auto-configured `options.LogSink` Beam). |
| `WithOptions(fn func(*options.ClientOptions))` | Escape hatch to customize any `*options.ClientOptions` field prior to connecting. |
| `Build(ctx ...context.Context) (*mongo.Client, Error)` | Connects to MongoDB, verifies with `Ping`, and returns `*mongo.Client`. |
| `BuildDatabase(name string, ctx ...context.Context) (*mongo.Database, Error)` | Connects and returns `*mongo.Database` directly. |

## Registering Multiple MongoDB Drivers

Because `MongoClientBuilder` is local to each `Configuration` method and `MongoTransactionalManager` has no Stereotype,
you can register multiple MongoDB clients, databases, and transaction managers using `Primary` and `Qualifier`:

```go
package config

import (
    "go.mongodb.org/mongo-driver/mongo"

    . "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
    . "github.com/BeamFoundry/rays/pkg/core"
    . "github.com/BeamFoundry/rays/pkg/lang"
)

type MultiMongoConfig struct {
    Configuration
    logSink options.LogSink `@:"Inject"`
}

func (c *MultiMongoConfig) PrimaryClient(
    _ struct{ Primary },
) (*mongo.Client, Error) {
    return NewMongoClientBuilder().
        WithURI("mongodb://primary-mongo:27017").
        WithLogSink(c.logSink).
        Build()
}

func (c *MultiMongoConfig) PrimaryDatabase(
    that struct {
        Primary
        client *mongo.Client `@:"Inject"`
    },
) *mongo.Database {
    return that.client.Database("app_primary")
}

// Registered with the default qualifier "analyticsClient" (the method name with a lowercase first letter).
func (c *MultiMongoConfig) AnalyticsClient() (*mongo.Client, Error) {
    return NewMongoClientBuilder().
        WithURI("mongodb://analytics-mongo:27017").
        WithLogSink(c.logSink).
        Build()
}

func (c *MultiMongoConfig) AnalyticsTxManager(
    that struct {
        Qualifier `Name:"analyticsTx"`
        client    *mongo.Client `@:"Inject,Qualifier{analyticsClient}"`
        logger    Logging       `@:"Inject"`
    },
) MongoTransactional {
    return NewMongoTransactionalManager(that.client, that.logger)
}

var _, _ = any(&MultiMongoConfig{}).(Stereotype)
```
