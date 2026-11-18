---
title: "7. Testing & Environments"
weight: 7
---

# Testing & Environments

Using `github.com/BeamFoundry/rays/pkg/testing` alongside `testcontainers-go/modules/mongodb`, you can
spin up real ephemeral MongoDB instances (including replica sets for ACID transaction testing) with full
Rays logging and `NewMongoClientBuilder()` integration.

## 1. Environment Configuration via Profiles

Switch MongoDB targets cleanly between local, test, and production environments using Rays `ConditionalOnProfile`
and `ConditionalOnMissingBeam` on your `Configuration` methods:

```go
func (c *MongoConfig) ProdClient(
    _ struct{ ConditionalOnProfile `Name:"prod"` },
) (*mongo.Client, Error) {
    return NewMongoClientBuilder().
        WithURI("mongodb+srv://cluster.example.com/?retryWrites=true&w=majority").
        WithLogSink(c.logSink).
        Build()
}

func (c *MongoConfig) DevClient(
    _ struct{ ConditionalOnMissingBeam },
) (*mongo.Client, Error) {
    return NewMongoClientBuilder().
        WithHost("127.0.0.1").
        WithPort(27017).
        WithLogSink(c.logSink).
        Build()
}
```

## 2. Ginkgo + Testcontainers Integration

Dot-import `. "github.com/BeamFoundry/rays/pkg/testing"` to integrate `GetLogger()` with Testcontainers
and automatically reset the Rays container between specs, and underscore-import `_ "github.com/BeamFoundry/rays-data-mongo"`
to activate `MongoTransactional` auto-registration:

```go
package repositories_test

import (
    "context"
    "testing"

    "github.com/testcontainers/testcontainers-go"
    container "github.com/testcontainers/testcontainers-go/modules/mongodb"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"

    . "github.com/onsi/ginkgo/v2"
    . "github.com/onsi/gomega"

    _ "github.com/BeamFoundry/rays-data-mongo"
    . "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
    . "github.com/BeamFoundry/rays/pkg/core"
    . "github.com/BeamFoundry/rays/pkg/lang"
    . "github.com/BeamFoundry/rays/pkg/testing"
)

func TestRepositories(t *testing.T) {
    RegisterFailHandler(Fail)
    RunSpecs(t, "Repositories Suite")
}

type TestMongoConfig struct {
    Configuration
    logSink options.LogSink `@:"Inject"`
    host    string
    port    int
}

func (t *TestMongoConfig) Client() (*mongo.Client, Error) {
    return NewMongoClientBuilder().
        WithHost(t.host).
        WithPort(t.port).
        WithDirectConnection(true).
        WithLogSink(t.logSink).
        Build()
}

func (t *TestMongoConfig) Database(
    that struct {
        client *mongo.Client `@:"Inject"`
    },
) *mongo.Database {
    return that.client.Database("test_db")
}

var _ = Describe("UserRepository", Ordered, func() {
    var (
        mongodbContainer *container.MongoDBContainer
        mongoHost        string
        mongoPort        int
    )

    BeforeAll(func() {
        ctx := context.Background()
        var err error
        mongodbContainer, err = container.Run(
            ctx,
            "mongo:latest",
            container.WithReplicaSet("rs0"),
            testcontainers.WithLogger(GetLogger()),
        )
        Expect(err).NotTo(HaveOccurred())

        ip, err := mongodbContainer.Host(ctx)
        Expect(err).NotTo(HaveOccurred())
        port, err := mongodbContainer.MappedPort(ctx, "27017")
        Expect(err).NotTo(HaveOccurred())

        mongoHost = ip
        mongoPort = int(port.Num())
    })

    AfterAll(func() {
        if mongodbContainer != nil {
            testcontainers.CleanupContainer(GinkgoTB(), mongodbContainer)
        }
    })

    BeforeEach(func() {
        AddBeamFor(&TestMongoConfig{host: mongoHost, port: mongoPort})
        AddBeamFor(&UserRepository{})
        GetContainer().EnableScanning()
        Expect(GetContainer().Initialize()).NotTo(HaveOccurred())
    })

    AfterEach(func() {
        client := GetBeamScopedInstance[*mongo.Client]()
        if client != nil {
            Expect(client.Disconnect(context.Background())).NotTo(HaveOccurred())
        }
    })
})
```
