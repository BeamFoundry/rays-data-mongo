---
title: "5. ThreadLocal Transactions"
weight: 5
---

# ThreadLocal Transactions

MongoDB multi-document ACID transactions normally require creating a `mongo.Session`, wrapping a
`mongo.SessionContext`, and passing that context through every repository call.

Rays Data Mongo uses `github.com/timandy/routine` to manage **ThreadLocal** `MongoSession` state for
your goroutines. Automatic session binding applies only to repository calls on the transaction's goroutine,
and a query captures its session when created. Construct repository queries inside the transaction callback
so that their operations use the active transaction session rather than executing unbound or capturing a
stale session.

> **Note:** MongoDB multi-document transactions require a MongoDB **replica set** or sharded cluster
> (including single-node replica sets such as `mongod --replSet rs0`).

## Using the `MongoTransactional` Closure

Inject `MongoTransactional` into your services to execute multi-repository operations atomically.
Returning any non-nil `Error` (or panicking) triggers `Rollback()`, while returning `nil` triggers `Commit()`.

```go
package services

import (
    "context"

    "go.mongodb.org/mongo-driver/bson"

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
        // Construct the query inside the callback so its operations capture the transaction session
        count, err := s.userRepo.Query().Where(bson.M{"_id": user.ID}).Count()
        if err != nil {
            return err // Triggers Rollback
        }
        if count == 0 {
            return NewError("user not found")
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

> **Note:** `Transactional` does not return callback or commit failures to its caller. If the transaction callback returns an error, `Rollback()` is triggered; when rollback succeeds, the callback error is neither returned nor logged, so it may be lost. If `Commit()` fails, the error is logged and the session ends without calling `Rollback()`, so the transaction outcome may be unknown. Callers needing the transaction result or explicit error propagation can use `Execute`, `ExecuteWithResult`, or manual transaction management.

## Result-Bearing Transactions (`Execute` and `ExecuteWithResult`)

While `Transactional` conforms to the standard Rays `Transactional[T]` contract, `MongoTransactionalManager` also provides `Execute` and `ExecuteWithResult` so callers can receive results and distinguish errors:

```go
mongoTx := s.txManager.(*MongoTransactionalManager)

// Execute returns any startup, callback, or commit error:
err := mongoTx.Execute(ctx, func(sess *MongoSession) Error {
    _, err := s.userRepo.Save(user)
    return err
})

// ExecuteWithResult returns computed results and propagates errors:
savedUser, err := ExecuteWithResult(ctx, mongoTx, func(sess *MongoSession) (*User, Error) {
    return s.userRepo.Save(user)
})
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

Once `sess.Commit()` or `sess.Rollback()` has been called on a manual `MongoSession`, the `TransactionError`
guarantee is limited to repository methods that expose errors (such as `Save`, `SaveAll`, `Delete`, and
`Query` execution paths). Repository methods that do not return errors, such as `FindById`, safely return an
empty `Optional` after finalization and before `End()`, rather than returning a `TransactionError`.
