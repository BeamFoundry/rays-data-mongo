---
title: "5. ThreadLocal Transactions"
weight: 5
---

# ThreadLocal Transactions

MongoDB multi-document ACID transactions normally require creating a `mongo.Session`, wrapping a
`mongo.SessionContext`, and passing that context through every repository call.

Rays Data Mongo uses `github.com/timandy/routine` to manage **ThreadLocal** `MongoSession` state for
your goroutines. When a service starts a transaction via `MongoTransactional`, all downstream repository
operations automatically execute inside that `mongo.SessionContext`.

> **Note:** MongoDB multi-document transactions require a MongoDB **replica set** or sharded cluster
> (including single-node replica sets such as `mongod --replSet rs0`).

## Using the `MongoTransactional` Closure

Inject `MongoTransactional` into your services to execute multi-repository operations atomically.
Returning any non-nil `Error` (or panicking) triggers `Rollback()`, while returning `nil` triggers `Commit()`.

```go
package services

import (
    "context"

    . "github.com/BeamFoundry/rays-data-mongo/pkg/core"
    . "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
    . "github.com/BeamFoundry/rays/pkg/core"
    . "github.com/BeamFoundry/rays/pkg/lang"
)

type OrderService struct {
    Service

    txManager MongoTransactional `@:"Inject"`
    userRepo  *UserRepository    `@:"Inject"`
    auditRepo *AuditRepository   `@:"Inject"`
}

func (s *OrderService) UpdateUserWithAudit(ctx context.Context, user *User) {
    s.txManager.Transactional(ctx, func(_ *MongoSession) Error {
        if _, err := s.userRepo.Save(user); err != nil {
            return err // Triggers Rollback
        }

        _, err := s.auditRepo.Save(&Audit{Action: "USER_UPDATED", TargetID: user.ID})
        if err != nil {
            return err // Triggers Rollback
        }

        return nil // Triggers Commit
    })
}
```

## Manual Transaction Management

If you need explicit control over `Commit()` and `Rollback()`, use `Start(ctx)` and always `defer End()`
to release the underlying `mongo.Session` and clear ThreadLocal storage:

```go
func (s *OrderService) Transfer(ctx context.Context, user *User) Error {
    sess := s.txManager.Start(ctx)
    defer s.txManager.End()

    if sess.Error() != nil {
        return sess.Error()
    }

    if _, err := s.userRepo.Save(user); err != nil {
        _ = sess.Rollback()
        return err
    }

    return sess.Commit()
}
```

Once `sess.Commit()` or `sess.Rollback()` has been called on a manual `MongoSession`, any subsequent repository
operations executed before `End()` will safely fail with a `TransactionError` rather than executing outside
the finalized transaction.
