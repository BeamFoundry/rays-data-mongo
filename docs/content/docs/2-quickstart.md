---
title: "2. Quickstart & Setup"
weight: 2
---

# Quickstart & Setup

To use **Rays Data Mongo**, define a Rays `Configuration` struct that provides a `*mongo.Client` and `*mongo.Database`
using `NewMongoClientBuilder()`, and underscore-import `github.com/BeamFoundry/rays-data-mongo` in your entrypoint.

## 1. Installation

```bash
go get github.com/BeamFoundry/rays-data-mongo
```

## 2. Defining the MongoDB Beams

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

## 3. Define a Model and Repository

```go
package repositories

import (
    "context"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/bson/primitive"
    "go.mongodb.org/mongo-driver/mongo"

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

// Optional: ensure MongoDB indexes on container initialization
func (r *UserRepository) PostConstruct() Error {
    _, err := r.EnsureIndexes(context.Background(), mongo.IndexModel{
        Keys: bson.D{{Key: "user_name", Value: 1}},
    })
    return err
}

func (r *UserRepository) FindByUserName(userName string) Optional[*User] {
    return r.findByUserName(userName)
}

var _, _ = any(&UserRepository{}).(Stereotype)
```

## 4. The Entrypoint

Underscore-import `github.com/BeamFoundry/rays-data-mongo` along with your application packages (`myapp/config`, `myapp/repositories`)
in your `main.go` so their Beams are registered and repository wiring is activated:

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
