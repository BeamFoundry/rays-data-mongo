---
title: "4. Auto-Generated Repositories"
weight: 4
---

# Auto-Generated Repositories

Rays Data Mongo eliminates MongoDB CRUD boilerplate by automatically implementing repository query fields based
on their function signatures during container initialization.

## 1. The Base Repository

Embed `MongoRepository[T, K]` in your repository struct (where `T` is your document struct and `K` is the
comparable `_id` type, such as `primitive.ObjectID` or `string`) to inherit `MongoRepositoryInterface[T, K]`:

* **`Collection() *mongo.Collection`**: Returns the underlying `*mongo.Collection` for `T`.
* **`Query(queries ...MongoQueryFunc) *MongoQuery`**: Creates a fluent `*MongoQuery` scoped to `T`'s collection and active ThreadLocal session.
* **`FindById(id K) Optional[*T]`**: Finds a document by `_id` and wraps it in `Optional[*T]`.
* **`FindAll() ([]T, Error)`**: Retrieves all documents in the collection.
* **`Count() (int64, Error)`**: Returns the total document count.
* **`ExistsById(id K) (bool, Error)`**: Checks if a document with `_id` exists.
* **`Save(object *T) (*T, Error)`**: Inserts a new document (auto-generating `primitive.NewObjectID()` if `_id` is zero) or upserts an existing document via `ReplaceOne`.
* **`SaveAll(objects []*T) ([]*T, Error)`**: Performs a bulk insert/upsert in a single `BulkWrite` call.
* **`Delete(object *T) Error`**: Deletes a document by its `_id` field.
* **`DeleteById(id K) Error`**: Deletes a document by its `_id` value.
* **`EnsureIndexes(ctx context.Context, models ...mongo.IndexModel) ([]string, Error)`**: Creates indexes on the collection.

### Collection Name Resolution

By default, `MongoRepository` derives the collection name by converting the entity struct name to pluralized
`snake_case` (e.g. `User` -> `users`, `OrderItem` -> `order_items`).

To specify a custom collection name, implement `CollectionNamer` on your entity:

```go
func (User) CollectionName() string {
    return "app_users"
}
```

## 2. The Query Builder Syntax

Define an unexported `func` field on your repository struct. During container wiring, Rays Data Mongo parses
the field name and binds a compiled BSON query implementation to it.

### Anatomy of a Method Name

`[Prefix][Subject]By[Conditions]OrderBy[SortRules]`

1. **Prefix:**
   * `find` / `Find` / `query` / `Query`: single entity (`*T`, `Optional[*T]`), slice (`[]T`), `Stream[T]`, or `*MongoQuery`.
   * `list` / `List`: multiple entities (`[]T`).
   * `stream` / `Stream`: `Stream[T]` (buffers matching documents into memory via `Find` before stream creation).
   * `count` / `Count`: returns `int64` or `(int64, Error)`.
   * `exists` / `Exists`: returns `bool` or `(bool, Error)`.
   * `delete` / `Delete`: deletes matching documents and returns `Error`, `int64`, or `(int64, Error)`.
2. **Subject:** Optional descriptive text before `By` (e.g. `ActiveUsers`).
3. **Separator:** Must contain `By`.
4. **Conditions:** Struct field paths (including nested document paths like `AddressCity` -> `address.city`)
   followed by an optional operator, chained with `And` or `Or`. BSON field names respect `bson:"..."` struct
   tags (and `bson:",inline"`), defaulting to `strings.ToLower(field.Name)` to match the MongoDB Go driver.
5. **OrderBy:** Optional `OrderBy` followed by chained `<Field>[Asc|Desc]` rules (defaults to `Asc`).

### Supported Operators

| Operator Keyword | MongoDB BSON Translation | Arguments Required | Example |
| :--- | :--- | :--- | :--- |
| *(None)*, `Is`, `Equals` | `{field: val}` | 1 (Field Type) | `findByName(string)` |
| `Not` | `{field: {$ne: val}}` | 1 (Field Type) | `findByStatusNot(string)` |
| `GreaterThan`, `After` | `{field: {$gt: val}}` | 1 (Field Type) | `findByAgeGreaterThan(uint)` |
| `LessThan`, `Before` | `{field: {$lt: val}}` | 1 (Field Type) | `findByCreatedAtBefore(time.Time)` |
| `GreaterThanOrEqual` | `{field: {$gte: val}}` | 1 (Field Type) | `findByAgeGreaterThanOrEqual(uint)` |
| `LessThanOrEqual` | `{field: {$lte: val}}` | 1 (Field Type) | `findByAgeLessThanOrEqual(uint)` |
| `IsNull`, `Null` | `{field: nil}` | 0 | `findByDeletedAtIsNull()` |
| `IsNotNull`, `NotNull` | `{field: {$ne: nil}}` | 0 | `findByEmailNotNull()` |
| `Between` | `{field: {$gte: a, $lte: b}}` | 2 (Field Type) | `findByAgeBetween(uint, uint)` |
| `Like` | `{field: primitive.Regex{Pattern: ^...$}}` | 1 (`string`) | `findByNameLike(string)` |
| `NotLike` | `{field: {$not: primitive.Regex{...}}}` | 1 (`string`) | `findByNameNotLike(string)` |
| `Regex`, `Matches` | `{field: primitive.Regex{Pattern: p}}` | 1 (`string`) | `findByNameRegex(string)` |
| `NotRegex` | `{field: {$not: primitive.Regex{...}}}` | 1 (`string`) | `findByNameNotRegex(string)` |
| `Containing` | `{field: primitive.Regex{Pattern: quoted}}` | 1 (`string`) | `findByNameContaining(string)` |
| `NotContaining` | `{field: {$not: primitive.Regex{...}}}` | 1 (`string`) | `findByNameNotContaining(string)` |
| `StartingWith` | `{field: primitive.Regex{Pattern: ^quoted}}` | 1 (`string`) | `findByNameStartingWith(string)` |
| `EndingWith` | `{field: primitive.Regex{Pattern: quoted$}}` | 1 (`string`) | `findByNameEndingWith(string)` |
| `In` | `{field: {$in: slice}}` | 1 (`[]FieldType`) | `findByRoleIn([]string)` |
| `NotIn` | `{field: {$nin: slice}}` | 1 (`[]FieldType`) | `findByStatusNotIn([]string)` |
| `True`, `IsTrue` | `{field: true}` | 0 | `findByActiveTrue()` |
| `False`, `IsFalse` | `{field: false}` | 0 | `findByActiveFalse()` |

### Return Types

* **Single Entity:** `*T` or `Optional[*T]`
* **Multiple Entities:** `[]T` or `Stream[T]` (Note: `Stream[T]` buffers all matching documents into memory via `Find` before returning the stream for functional pipeline transformations).
* **Aggregates / Existence:** `int64` (`CountBy...`, `DeleteBy...`), `bool` (`ExistsBy...`)
* **Query Builder:** `*MongoQuery` (returns the compiled query prior to execution)
* **Error Handling:** Append `Error` as a second return value (or sole return value for `DeleteBy...`). Single-result methods without an `Error` return (`*T`, `Optional[*T]`) return `nil` or an empty `Optional` on error; callers needing to distinguish failures from misses should declare an `(Optional[*T], Error)` or `(*T, Error)` signature.

## 3. Complete Example

```go
package repositories

import (
    "go.mongodb.org/mongo-driver/bson/primitive"
    . "github.com/BeamFoundry/rays-data-mongo/pkg/core"
    . "github.com/BeamFoundry/rays/pkg/lang"
    . "github.com/BeamFoundry/rays/pkg/util/stream"
)

type Address struct {
    City    string `bson:"city"`
    Country string `bson:"country"`
}

type User struct {
    ID      primitive.ObjectID `bson:"_id,omitempty"`
    Name    string             `bson:"name"`
    Status  string             `bson:"status"`
    Age     uint               `bson:"age"`
    Active  bool               `bson:"active"`
    Address Address            `bson:"address"`
}

type UserRepositoryInterface interface {
    MongoRepositoryInterface[User, primitive.ObjectID]

    FindByName(name string) Optional[*User]
    ListByAddressCity(city string, filters ...MongoQueryFunc) ([]User, Error)
    ListActiveInCity(city string, filters ...MongoQueryFunc) ([]User, Error)
    StreamByAgeGreaterThan(minAge uint) (Stream[User], Error)
    CountByStatus(status string) (int64, Error)
}

type UserRepository struct {
    MongoRepository[User, primitive.ObjectID]

    findByName                       func(string) Optional[*User]
    listByAddressCity                func(string, ...MongoQueryFunc) ([]User, Error)
    listByAddressCityAndActiveIsTrue func(string, ...MongoQueryFunc) ([]User, Error)
    streamByAgeGreaterThan          func(uint) (Stream[User], Error)
    countByStatus                   func(string) (int64, Error)
}

func (r *UserRepository) FindByName(name string) Optional[*User] {
    return r.findByName(name)
}

func (r *UserRepository) ListByAddressCity(city string, filters ...MongoQueryFunc) ([]User, Error) {
    return r.listByAddressCity(city, filters...)
}

func (r *UserRepository) ListActiveInCity(city string, filters ...MongoQueryFunc) ([]User, Error) {
    return r.listByAddressCityAndActiveIsTrue(city, filters...)
}

func (r *UserRepository) StreamByAgeGreaterThan(minAge uint) (Stream[User], Error) {
    return r.streamByAgeGreaterThan(minAge)
}

func (r *UserRepository) CountByStatus(status string) (int64, Error) {
    return r.countByStatus(status)
}
```
