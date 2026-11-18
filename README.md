# BeamFoundry Rays Data Mongo

[![Go Version](https://img.shields.io/badge/Go-1.27+-00ADD8?style=flat&logo=go)](https://golang.org)
[![codecov](https://codecov.io/gh/BeamFoundry/rays-data-mongo/graph/badge.svg?token=V4ZX90ORTR)](https://codecov.io/gh/BeamFoundry/rays-data-mongo)

**This is an extract of the full documentation available in [`docs/`](./docs) (`https://rays.bfdy.dev/rays-data-mongo/`).**

**Rays Data Mongo** is the official MongoDB integration package for the **BeamFoundry Rays** framework,
bringing Spring Data MongoDB-style repositories, a fluent client builder, and ThreadLocal transactions to Go
using the official [MongoDB Go Driver](https://www.mongodb.com/docs/drivers/go/current/).

## Core Architectural Features

1. **Fluent `MongoClientBuilder` & Multi-Database `Configuration` (`pkg/tx`):** Construct one or more
   `*mongo.Client` and `*mongo.Database` Beams inside standard Rays `Configuration` structs using `NewMongoClientBuilder()`.
2. **Auto-Generated Repository Methods (`pkg/core`):** Embed `MongoRepository[T, K]` and declare unexported `func` fields like
   `findByName func(string) *User`, `findByAddressCity func(string) []User`, `countByStatus func(string) (int64, Error)`,
   or `streamByAgeGreaterThan func(uint) (Stream[User], Error)`. Rays Data Mongo binds the BSON query implementation automatically.
3. **ThreadLocal Transactions (`pkg/tx`):** Using `github.com/timandy/routine`, `MongoTransactionalManager` (registered
   automatically by `internal/autoconfig` with no Stereotype of its own) propagates active `mongo.SessionContext`
   transactions across goroutine calls without polluting repository method signatures.
4. **Injection-Safe BSON Queries & Composable Helpers (`pkg/util`):** Derived queries build structured `bson.M` / `bson.D` filters
   and support variadic `...MongoQueryFunc` helpers (`Paginate`, `WithLimit`, `WithSkip`, `OrderByAsc`, `OrderByDesc`,
   `WithSelect`, `WithOmit`, `WithWhere`, `Combine`, etc.).
5. **Centralized `slog` Observability (`pkg/util`):** Bridges driver command and error logs into Rays `Logging` via `MongoLogSink` (with a default `options.LogSink` Beam registered by `internal/autoconfig`).

## Quickstart

### 1. Configuration (`Configuration` + `NewMongoClientBuilder`)

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
        WithHost("127.0.0.1").
        WithPort(27017).
        WithLogSink(c.logSink).
        Build()
}

func (c *MongoConfig) Database(
    that struct {
        client *mongo.Client `@:"Inject"`
    },
) *mongo.Database {
    return that.client.Database("myapp")
}

var _, _ = any(&MongoConfig{}).(Stereotype)
```

### 2. Model & Repository

```go
package repositories

import (
    "go.mongodb.org/mongo-driver/bson/primitive"
    . "github.com/BeamFoundry/rays-data-mongo/pkg/core"
    . "github.com/BeamFoundry/rays/pkg/core"
    . "github.com/BeamFoundry/rays/pkg/lang"
)

type User struct {
    ID       primitive.ObjectID `bson:"_id,omitempty"`
    Name     string             `bson:"name"`
    UserName string             `bson:"user_name"`
    Age      uint               `bson:"age"`
}

type UserRepository struct {
    MongoRepository[User, primitive.ObjectID]
    findByUserName func(string) Optional[*User]
}

func (r *UserRepository) FindByUserName(userName string) Optional[*User] {
    return r.findByUserName(userName)
}

var _, _ = any(&UserRepository{}).(Stereotype)
```

### 3. Entrypoint

```go
package main

import (
    "os"

    "github.com/BeamFoundry/rays"
    _ "github.com/BeamFoundry/rays-data-mongo"
    _ "myapp/config"
    _ "myapp/repositories"
)

func main() {
    ctx := rays.ApplicationContext()
    if err := ctx.Initialize(); err != nil {
        err.LogErrorWithStackTrace()
        os.Exit(1)
    }
    ctx.Run()
}
```

## Documentation & Testing

- **Documentation (Hugo):** Built with Hugo + `hugo-book` in [`docs/`](./docs).
- **Tests:** Run with Ginkgo + Testcontainers + `github.com/BeamFoundry/rays/pkg/testing`:

```bash
GOWORK=off golangci-lint run ./...
GOWORK=off go test -v ./...
```
