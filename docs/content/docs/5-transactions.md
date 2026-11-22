---
title: "5. ThreadLocal Transactions"
weight: 5
---

# ThreadLocal Transactions

MongoDB multi-document ACID transactions normally require creating a `mongo.Session`, wrapping a
`mongo.SessionContext`, and passing that context through every repository call.

Rays Data Mongo integrates with the global Rays **`TransactionManager`** (`github.com/BeamFoundry/rays/pkg/core`)
and uses `github.com/timandy/routine` to manage **ThreadLocal** `MongoSession` state for your goroutines:

- **Services** inject `TransactionManager` to bind request `context.Context` (`SetContext` / `ClearContext`) and control transaction boundaries (`Transactional`, or manual `Start`, `Commit`, `Rollback`, `End`).
- **Repositories** inject `MongoTransactional` (`Transactional[MongoSession]`, backed by `MongoTransactionalManager`), which lazily enlists itself in the active `TransactionManager` on the first repository operation (`Get()`) and automatically reports repository errors via `SetError(err)` to trigger immediate rollback.

Automatic session binding applies only to repository calls on the transaction's goroutine,
and a query captures its session when created. Construct repository queries inside the transaction callback
so that their operations use the active transaction session rather than executing unbound or capturing a
stale session.

> **Note:** MongoDB multi-document transactions require a MongoDB **replica set** or sharded cluster
> (including single-node replica sets such as `mongod --replSet rs0`).

## Using the `TransactionManager` Closure

Inject `TransactionManager` into your services to execute multi-repository operations inside a coordinated transaction.
Returning any non-nil `Error`, panicking, or encountering a repository error reported via `SetError` triggers
`Rollback()` across all enlisted providers and returns the error to the caller, while returning `nil` triggers `Commit()`.

```go
package services

import (
    "context"

    "go.mongodb.org/mongo-driver/bson"

    . "github.com/BeamFoundry/rays/pkg/core"
    . "github.com/BeamFoundry/rays/pkg/lang"
)

type OrderService struct {
    Service

    txManager TransactionManager `@:"Inject"`
    userRepo  *UserRepository    `@:"Inject"`
    auditRepo *AuditRepository   `@:"Inject"`
}

func (s *OrderService) UpdateUserWithAudit(ctx context.Context, user *User) Error {
    s.txManager.SetContext(ctx)
    defer s.txManager.ClearContext()

    return s.txManager.Transactional(func() Error {
        // Construct the query inside the callback so its operations capture the transaction session
        count, err := s.userRepo.Query().Where(bson.M{"_id": user.ID}).Count()
        if err != nil {
            return err // Triggers Rollback
        }
        if count == 0 {
            return NewError("UserNotFoundError", "user not found")
        }

        if _, err := s.userRepo.Save(user); err != nil {
            return err // Triggers Rollback
        }

        _, err = s.auditRepo.Save(&Audit{Action: "USER_UPDATED", TargetID: user.ID})
        if err != nil {
            return err // Triggers Rollback
        }

        return nil // Triggers Commit
    })
}
```

> **Note:** Even if a repository error is not returned by your callback (for example, `_, _ = s.userRepo.Save(invalidUser)`), `MongoRepository` and derived query methods report database and validation errors (excluding normal `mongo.ErrNoDocuments` misses on non-error single-document lookups such as `FindById`) to `MongoTransactional.SetError(err)`. This immediately rolls back all enlisted transaction providers on the current goroutine and causes `s.txManager.Transactional(...)` to return the recorded error instead of committing.

## Manual Transaction Management

If you need explicit control over `Start()`, `Commit()`, and `Rollback()`, use `TransactionManager` directly and
always `defer s.txManager.End()` to finalize any uncommitted state, close the underlying `mongo.Session`, and clear
ThreadLocal storage:

```go
func (s *OrderService) Transfer(ctx context.Context, user *User) Error {
    s.txManager.SetContext(ctx)
    defer s.txManager.ClearContext()

    if err := s.txManager.Start(); err != nil {
        return err
    }
    defer s.txManager.End()

    if _, err := s.userRepo.Save(user); err != nil {
        _ = s.txManager.Rollback()
        return err
    }

    return s.txManager.Commit()
}
```

Once `Commit()` or `Rollback()` has been called on an active transaction before `End()`, the `TransactionError`
guarantee is limited to repository methods that expose errors (such as `Save`, `SaveAll`, `Delete`, and
`Query` execution paths). Repository methods that do not return errors, such as `FindById`, safely return an
empty `Optional` after finalization and before `End()`, rather than returning a `TransactionError`.

## Multi-Database Routing: Type Splitting & Context Splitting

Because `MongoTransactional` is registered as a **Prototype** Beam by default and `MongoTransactionalManager` tracks active goroutine sessions per `*mongo.Client` instance:

- Repositories that resolve to the **same** `*mongo.Client` on a goroutine (even across different `*mongo.Database` names on that cluster) automatically share the same active `mongo.Session` transaction.
- Repositories that resolve to **different** `*mongo.Client` clusters on a goroutine each lazily enlist their own `mongo.Session` transaction in `TransactionManager` (and can also coordinate alongside `rays-orm` `GormTransactional` SQL providers), providing **best-effort** multi-database commit and rollback coordination rather than an atomic distributed transaction.

When a Prototype `MongoTransactional` injects a Prototype `*mongo.Database` (a `Configuration` method accepting `ctx context.Context`), Rays automatically populates `ctx` with:
- `PrototypeContextProxyType`: The `reflect.Type` of the repository (e.g. `*UserRepository`).
- `PrototypeContextProxyInstance`: The repository instance (which implements `MongoEntityTypeProvider` via `MongoRepository[T, K]`).
- All runtime context values set on `TransactionManager.SetContext(ctx)`.

Rays Data Mongo provides two helper functions in `github.com/BeamFoundry/rays-data-mongo/pkg/core` to inspect the target repository or entity from `ctx`:

| Helper Function | Return Value | Description |
| :--- | :--- | :--- |
| `MongoEntityTypeFromContext(ctx)` | `reflect.Type` | Returns the `reflect.Type` of entity `T` (e.g. `reflect.TypeFor[Audit]()`). |
| `MongoRepositoryTypeFromContext(ctx)` | `reflect.Type` | Returns the `reflect.Type` of the repository pointer (e.g. `reflect.TypeFor[*AuditRepository]()`). |

---

### 1. Type Splitting (by Entity Type `T` or Repository Type)

To route different entities or repositories to separate `*mongo.Database` connections (on the same or different `*mongo.Client` clusters) without modifying your repository structs, define a Prototype `Database(ctx context.Context) *mongo.Database` method in a `Configuration` Beam:

```go
package config

import (
    "context"
    "reflect"

    "go.mongodb.org/mongo-driver/mongo"

    . "github.com/BeamFoundry/rays-data-mongo/pkg/core"
    . "github.com/BeamFoundry/rays/pkg/core"
)

type MultiMongoConfig struct {
    Configuration
    primaryDB *mongo.Database
    auditDB   *mongo.Database
}

// Accepting context.Context makes *mongo.Database a Prototype Beam evaluated per repository
func (this *MultiMongoConfig) Database(ctx context.Context) *mongo.Database {
    // Option A: Route by MongoRepository Entity Type T
    if MongoEntityTypeFromContext(ctx) == reflect.TypeFor[Audit]() {
        return this.auditDB
    }

    // Option B: Route by Repository Type, Name, or Package Path
    if repoType := MongoRepositoryTypeFromContext(ctx); repoType != nil {
        if repoType == reflect.TypeFor[*AuditRepository]() {
            return this.auditDB
        }
    }

    return this.primaryDB
}
```

When a service runs `this.txManager.Transactional(...)` and calls both `userRepo` (`primaryDB`) and `auditRepo` (`auditDB`), both connections lazily enlist in `TransactionManager` for **best-effort** coordination: if an error occurs before commit, all enlisted connections are rolled back together; at completion, each enlisted provider is committed sequentially.

> **Important:** Multi-database coordination across distinct `*mongo.Client` clusters (or across MongoDB and SQL) is **not** an atomic two-phase commit (2PC / XA). If one database commits and a subsequent database's commit fails, the already-committed changes remain durable while the failing and remaining uncommitted providers are rolled back. Application-level recovery or compensation is required to handle partial commit failures.

---

### 2. Context Splitting (Multi-Tenancy & Dynamic Routing)

When a Controller or Middleware binds runtime parameters (such as a tenant ID or shard key) to `TransactionManager.SetContext(ctx)`, a repository can dynamically resolve its `MongoTransactional` (and associated `*mongo.Database`) from the active `context.Context` by:

1. Injecting `Foundry[MongoTransactional]` into the repository struct.
2. Implementing the `MongoTransactionalResolver` interface (`MongoTransactional(ctx context.Context) MongoTransactional`).

`MongoRepository` automatically invokes `MongoTransactional(ctx)` with `TransactionManager.GetContext()` and caches the resolved `MongoTransactional` for that context on the current goroutine:

```go
package repositories

import (
    "context"

    "go.mongodb.org/mongo-driver/bson/primitive"
    "go.mongodb.org/mongo-driver/mongo"

    . "github.com/BeamFoundry/rays-data-mongo/pkg/core"
    . "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
    . "github.com/BeamFoundry/rays/pkg/core"
    . "github.com/BeamFoundry/rays/pkg/lang"
)

type TenantKeyType string

const TenantKey TenantKeyType = "tenant"

// 1. Repository implements MongoTransactionalResolver using an injected Foundry
type UserRepository struct {
    MongoRepository[User, primitive.ObjectID]
    foundry    Foundry[MongoTransactional] `@:"Inject"`
    findByName func(string) *User
}

var _ MongoTransactionalResolver = &UserRepository{}

func (this *UserRepository) MongoTransactional(ctx context.Context) MongoTransactional {
    if ctx.Value(TenantKey) == nil {
        return nil // Intentional default route: falls back to the default injected MongoTransactional
    }
    // Stop the operation and propagate any Forge or database routing failure rather than falling back to defaultDB
    tx := RaiseVal(this.foundry.Forge(ctx))
    if tx.Database() == nil {
        RaiseErr(NewError("UnknownTenantError", "unrecognized tenant in context"))
    }
    return tx
}

func (this *UserRepository) FindByName(name string) *User {
    return this.findByName(name)
}

// 2. Configuration selects the *mongo.Database based on the runtime context.Context
type TenantMongoConfig struct {
    Configuration
    defaultDB *mongo.Database
    tenantADB *mongo.Database
    tenantBDB *mongo.Database
}

func (this *TenantMongoConfig) Database(ctx context.Context) (*mongo.Database, Error) {
    switch ctx.Value(TenantKey) {
    case nil:
        return this.defaultDB, nil
    case "tenant-a":
        return this.tenantADB, nil
    case "tenant-b":
        return this.tenantBDB, nil
    default:
        return nil, NewError("UnknownTenantError", "unrecognized tenant in context")
    }
}
```

In your Controller or Service, simply bind the tenant context to `TransactionManager`:

```go
ctx := context.WithValue(req.Context(), TenantKey, "tenant-a")
this.txManager.SetContext(ctx)
defer this.txManager.ClearContext()

err := this.txManager.Transactional(func() Error {
    // Automatically forged and executed against tenantADB
    _, err := this.userRepo.Save(&User{Name: "Alice"})
    return err
})
```
