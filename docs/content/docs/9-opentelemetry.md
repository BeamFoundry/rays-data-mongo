---
title: "9. OpenTelemetry Integration"
weight: 9
---

# OpenTelemetry Integration

Rays Data Mongo integrates with `rays-otel` to trace MongoDB commands and propagate active spans across repositories and transactions.

## Context Propagation

`MongoTransactionalManager` automatically binds `TransactionManager.GetContext()` to every `MongoSession` (both outside and inside active transactions). When combined with `rays-fiber` or `rays-otel`, MongoDB command spans created by `otelmongo` automatically become children of the active HTTP or service span.

## Automatic Monitor Bean (`pkg/contrib/otel`)

The optional `github.com/BeamFoundry/rays-data-mongo/pkg/contrib/otel` sub-module provides `MongoOtelConfiguration`, which registers a `*event.CommandMonitor` beam backed by `otelmongo.NewMonitor` using the container's `trace.TracerProvider` when available.

### 📦 Installation

```bash
go get github.com/BeamFoundry/rays-data-mongo/pkg/contrib/otel
```

### Usage

Underscore-import `github.com/BeamFoundry/rays-data-mongo/pkg/contrib/otel` in your `main.go` and pass the injected `*event.CommandMonitor` to `MongoClientBuilder.WithOptions`:

```go
package config

import (
    "go.mongodb.org/mongo-driver/event"
    "go.mongodb.org/mongo-driver/mongo"

    _ "github.com/BeamFoundry/rays-data-mongo"
    mongoOtel "github.com/BeamFoundry/rays-data-mongo/pkg/contrib/otel"
    . "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
    . "github.com/BeamFoundry/rays/pkg/core"
    . "github.com/BeamFoundry/rays/pkg/lang"
)

type MongoConfig struct {
    Configuration
    monitor Optional[*event.CommandMonitor] `@:"Inject"`
}

func (this *MongoConfig) Database() (*mongo.Database, Error) {
    builder := NewMongoClientBuilder().WithHost("127.0.0.1")
    if m, ok := this.monitor.Get(); ok {
        builder.WithOptions(mongoOtel.ClientOptionsCustomizer(m))
    }
    return builder.BuildDatabase("app")
}

var _, _ = any(&MongoConfig{}).(Stereotype)
```

### Configuration (`rays.data.mongo.otel`)

You can configure the monitor in your `application.yaml`:

```yaml
rays:
  data:
    mongo:
      otel:
        commandAttributeDisabled: false
```

### Customizing Options (`MongoOtelCustomizer`)

Any Beam implementing `MongoOtelCustomizer` can append or override `otelmongo.Option` values:

```go
type CustomMongoOtel struct {
    Service
}

func (this *CustomMongoOtel) CustomizeMongoOtel(options []otelmongo.Option) []otelmongo.Option {
    return append(options, otelmongo.WithCommandAttributeDisabled(true))
}
```
