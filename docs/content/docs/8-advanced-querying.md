---
title: "8. Advanced Querying"
weight: 8
---

# Advanced Querying

When derived repository methods are not enough, Rays Data Mongo provides the **`MongoQueryFunc`** escape
hatch (`func(*MongoQuery) *MongoQuery`) and the **`*MongoQuery`** fluent query executor.

```go
import (
    . "github.com/BeamFoundry/rays-data-mongo/pkg/core"
    . "github.com/BeamFoundry/rays-data-mongo/pkg/util"
)
```

## 1. Built-in `MongoQueryFunc` Helpers (`pkg/util`)

| Category | Function | Signature | Description |
|---|---|---|---|
| **Pagination** | `Paginate` | `Paginate(page, pageSize int) MongoQueryFunc` | 1-indexed pagination (`skip = (page-1)*pageSize`, `limit = pageSize`) with safe defaults and overflow protection (`PaginationOverflowError`). |
| **Limits** | `WithLimit` / `Limit` | `WithLimit(limit int64) MongoQueryFunc` | Limits returned documents; `WithLimit(0)` returns no results for `Find`, `First`, `Count`, and `Exists`, but does not suppress `Distinct` or `Aggregate`. |
| **Skip / Offset** | `WithSkip` / `WithOffset` | `WithSkip(skip int64) MongoQueryFunc` | Skips documents; passing `0` clears any previously set skip. |
| **Sorting** | `OrderByAsc` | `OrderByAsc(field string) MongoQueryFunc` | Sorts by `field` ascending (`1`). |
| **Sorting** | `OrderByDesc` | `OrderByDesc(field string) MongoQueryFunc` | Sorts by `field` descending (`-1`). |
| **Sorting** | `WithSort` / `WithOrder` | `WithSort(sort any) MongoQueryFunc` | Applies a custom `bson.D`, `bson.E`, or `bson.M` sort document. |
| **Projections** | `WithSelect` | `WithSelect(fields ...string) MongoQueryFunc` | Includes only the specified BSON fields (`1`). |
| **Projections** | `WithOmit` | `WithOmit(fields ...string) MongoQueryFunc` | Excludes the specified BSON fields (`0`). |
| **Projections** | `WithProjection` | `WithProjection(proj any) MongoQueryFunc` | Applies an arbitrary MongoDB projection document. |
| **Filtering** | `WithWhere` / `WithFilter` | `WithWhere(filter any) MongoQueryFunc` | Appends a BSON filter document (`bson.M`, `bson.D`). |
| **Filtering** | `WithOr` | `WithOr(filters ...any) MongoQueryFunc` | Appends an `$or` filter clause. After `WithWhere(A)`, single-filter `WithOr(B)` matches `A OR B`, while multi-filter `WithOr(B, C)` matches `A AND (B OR C)`. |
| **Collections** | `WithCollection` | `WithCollection(name string) MongoQueryFunc` | Overrides the target collection name (e.g. archive collections). |
| **Mongo Options** | `WithHint` | `WithHint(hint any) MongoQueryFunc` | Specifies a MongoDB index hint. |
| **Mongo Options** | `WithMaxTime` | `WithMaxTime(d time.Duration) MongoQueryFunc` | Sets server-side query timeout. |
| **Mongo Options** | `WithCollation` | `WithCollation(c *options.Collation) MongoQueryFunc` | Sets MongoDB string comparison collation. |
| **Context** | `WithQueryContext` | `WithQueryContext(ctx context.Context) MongoQueryFunc` | Overrides the query execution context. |
| **Composition** | `Combine` | `Combine(funcs ...MongoQueryFunc) MongoQueryFunc` | Composes multiple helpers into a reusable specification. |

## 2. Pagination & Scope Composition

```go
func ActiveRecentUsers(page int) MongoQueryFunc {
    return Combine(
        WithWhere(bson.M{"active": true}),
        OrderByDesc("created_at"),
        Paginate(page, 20),
    )
}

users, err := userRepo.ListByAddressCity("Emerald City", ActiveRecentUsers(1))
```

## 3. Aggregations, Distinct, and Raw Collection Access

Every repository embedding `MongoRepository` exposes `Query(queries ...MongoQueryFunc) *MongoQuery` and
`Collection() *mongo.Collection`. While `Query()` automatically uses the active ThreadLocal transaction context,
`Collection()` returns an unbound `*mongo.Collection`; raw driver operations executed directly against `Collection()`
must receive the active session context explicitly to participate in the transaction:

```go
// Distinct values
var ages []uint
err := userRepo.Query(WithWhere(bson.M{"active": true})).
    Distinct("age", &ages)
if err == nil {
    slices.Sort(ages) // Sort in Go if ascending order is needed; MongoDB distinct results are unordered
}

// Aggregation pipeline
type RoleStats struct {
    Role  string `bson:"_id"`
    Count int    `bson:"count"`
}
var stats []RoleStats
err = userRepo.Query().Aggregate(mongo.Pipeline{
    {{Key: "$group", Value: bson.D{
        {Key: "_id", Value: "$role"},
        {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
    }}},
}, &stats)
```
