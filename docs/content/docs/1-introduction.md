---
title: "1. Introduction"
weight: 1
---

# Introduction & Architecture

In standard Go MongoDB applications, managing client connections, building repetitive `bson.M` filters,
and threading `mongo.SessionContext` through every service and repository method creates significant boilerplate.

**Rays Data Mongo solves these enterprise problems natively.**

## Simplifying with Dot Imports

Just like the core Rays framework and Rays ORM, it is recommended to use Go's dot import feature for the MongoDB packages:

- `import . "github.com/BeamFoundry/rays-data-mongo/pkg/core"` for core repository types, interfaces, and query builders (`MongoRepository`, `MongoQuery`, `MongoQueryFunc`, `CollectionNamer`).
- `import . "github.com/BeamFoundry/rays-data-mongo/pkg/tx"` for client construction and transaction management (`NewMongoClientBuilder`, `MongoTransactional`, `MongoTransactionalManager`, `MongoSession`).
- `import . "github.com/BeamFoundry/rays-data-mongo/pkg/util"` for `MongoLogSink` and ready-to-use query helpers (`Paginate`, `WithLimit`, `WithSkip`, `OrderByAsc`, `OrderByDesc`, `WithSelect`, `WithOmit`, `WithWhere`, `Combine`, etc.).
- `import . "github.com/BeamFoundry/rays/pkg/util/stream"` for lazy, Java 8-style stream operations on query results (`Stream[T]`).

## Core Architectural Features

1. **Fluent `MongoClientBuilder` & `Configuration` Beams:** Easily build `*mongo.Client` and `*mongo.Database`
   Beams inside standard Rays `Configuration` structs, supporting single or multiple MongoDB instances with
   `Primary` and `Qualifier` without global `ConfigurationProperties` constraints.
2. **Auto-Generated Repository Methods:** By simply defining unexported `func` fields like `findByName func(string) *User`,
   `listByStatusAndAgeGreaterThan func(string, uint) ([]User, Error)`, or `findByAddressCity func(string) []User`
   on your repository struct, Rays Data Mongo parses the signature and binds the underlying BSON query logic during container wiring.
3. **ThreadLocal Transactions (`pkg/tx`):** Using `github.com/timandy/routine`, `MongoTransactional` binds active MongoDB
   sessions (`mongo.SessionContext`) to the current goroutine. `MongoTransactionalManager` has no Stereotype of its own
   and is automatically registered by `internal/autoconfig` when a `*mongo.Client` or `*mongo.Database` Beam is present.
4. **Injection-Safe BSON Queries:** Derived query methods construct strongly-typed `bson.M` / `bson.D` documents
   and safely quote regex matchers (`Containing`, `StartingWith`, `EndingWith`, `Like`), protecting against
   accidental query injection.
5. **Centralized `slog` Observability:** Driver command and error events are bridged through `MongoLogSink`
   directly into the Rays `Logging` subsystem.
