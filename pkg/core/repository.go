package core

import (
	"context"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	. "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/lang"
	. "github.com/BeamFoundry/rays/pkg/util"
)

const bsonIDField = "_id"

// CollectionNamer can be implemented by an entity struct (value or pointer receiver)
// to explicitly specify its MongoDB collection name instead of using the derived default.
type CollectionNamer interface {
	CollectionName() string
}

// MongoRepositoryInterface defines the standard CRUD and query contract for a MongoDB repository
// managing entity type T with identifier type K.
type MongoRepositoryInterface[T any, K comparable] interface {
	// Collection returns the underlying [*mongo.Collection] for entity T.
	Collection() *mongo.Collection
	// Query returns a fluent [*MongoQuery] scoped to entity T's collection and active ThreadLocal session.
	Query(queries ...MongoQueryFunc) *MongoQuery
	// FindById finds a document by its _id field and returns an [Optional] pointer.
	FindById(id K) Optional[*T]
	// FindAll returns all documents in the collection.
	FindAll() ([]T, Error)
	// Count returns the total number of documents in the collection.
	Count() (int64, Error)
	// ExistsById returns true if a document with the given _id exists.
	ExistsById(id K) (bool, Error)
	// Delete deletes the given document using its _id field.
	Delete(object *T) Error
	// DeleteById deletes a document by its _id value.
	DeleteById(id K) Error
	// Save inserts or upserts the given document and populates generated ObjectID if applicable.
	Save(object *T) (*T, Error)
	// SaveAll inserts or upserts a slice of documents in bulk.
	SaveAll(objects []*T) ([]*T, Error)
	// EnsureIndexes creates the specified MongoDB indexes on the entity collection.
	EnsureIndexes(ctx context.Context, models ...mongo.IndexModel) ([]string, Error)
}

// MongoRepository is the base embeddable repository for MongoDB entities.
// Embed this struct inside a domain repository alongside unexported query function fields
// (e.g. findByName func(string) *User) to have Rays automatically wire them during container initialization.
type MongoRepository[T any, K comparable] struct {
	Repository
	database           *mongo.Database    `@:"Inject"`
	transactionManager MongoTransactional `@:"Inject"`
	logger             Logging            `@:"Inject"`
	collectionName     string
}

func (this *MongoRepository[T, K]) validateActiveSession() (context.Context, Error) {
	if this.transactionManager == nil {
		return context.Background(), nil
	}
	ms := this.transactionManager.Get()
	if err := ms.Validate(); err != nil {
		return ms.Context(), err
	}
	return ms.Context(), nil
}

func (this *MongoRepository[T, K]) activeContext() context.Context {
	if this.transactionManager != nil {
		return this.transactionManager.Get()
	}
	return context.Background()
}

func (this *MongoRepository[T, K]) resolveCollectionName() string {
	if this.collectionName != "" {
		return this.collectionName
	}
	var zero T
	if namer, ok := any(zero).(CollectionNamer); ok {
		if name := namer.CollectionName(); name != "" {
			return name
		}
	}
	if namer, ok := any(&zero).(CollectionNamer); ok {
		if name := namer.CollectionName(); name != "" {
			return name
		}
	}
	entity := reflect.TypeFor[T]()
	for entity.Kind() == reflect.Pointer {
		entity = entity.Elem()
	}
	return pluralize(CaseUtils(entity.Name()).SnakeCase().String())
}

func pluralize(word string) string {
	if word == "" {
		return "documents"
	}
	lower := strings.ToLower(word)
	switch {
	case strings.HasSuffix(lower, "s") || strings.HasSuffix(lower, "x") ||
		strings.HasSuffix(lower, "z") || strings.HasSuffix(lower, "ch") ||
		strings.HasSuffix(lower, "sh"):
		return lower + "es"
	case len(lower) > 1 && strings.HasSuffix(lower, "y") && !strings.ContainsRune("aeiou", rune(lower[len(lower)-2])):
		return lower[:len(lower)-1] + "ies"
	default:
		return lower + "s"
	}
}

// Collection returns the underlying [*mongo.Collection] for entity T.
func (this *MongoRepository[T, K]) Collection() *mongo.Collection {
	return this.database.Collection(this.resolveCollectionName())
}

// Query creates a new [*MongoQuery] targeting T's collection with any active ThreadLocal transaction context.
func (this *MongoRepository[T, K]) Query(queries ...MongoQueryFunc) *MongoQuery {
	q := NewMongoQuery(this.activeContext(), this.database, this.resolveCollectionName())
	return q.Scopes(queries...)
}

// FindById finds a document by its _id field and returns an [Optional] pointer.
func (this *MongoRepository[T, K]) FindById(id K) Optional[*T] {
	ctx, err := this.validateActiveSession()
	if err != nil {
		return OptionalEmpty[*T]()
	}
	var t T
	res := this.Collection().FindOne(ctx, bson.M{bsonIDField: id})
	if err := res.Decode(&t); err != nil {
		return OptionalEmpty[*T]()
	}
	return OptionalOf(&t)
}

// FindAll returns all documents in the entity collection.
func (this *MongoRepository[T, K]) FindAll() ([]T, Error) {
	var results []T
	if err := this.Query().Find(&results); err != nil {
		return nil, err
	}
	return results, nil
}

// Count returns the total number of documents in the entity collection.
func (this *MongoRepository[T, K]) Count() (int64, Error) {
	return this.Query().Count()
}

// ExistsById returns true if a document with the given _id exists in the collection.
func (this *MongoRepository[T, K]) ExistsById(id K) (bool, Error) {
	ctx, err := this.validateActiveSession()
	if err != nil {
		return false, err
	}
	count, driverErr := this.Collection().CountDocuments(
		ctx,
		bson.M{bsonIDField: id},
		options.Count().SetLimit(1),
	)
	if driverErr != nil {
		return false, NewWrappedError("MongoError", driverErr)
	}
	return count > 0, nil
}

// Delete removes the given entity document by its _id field.
func (this *MongoRepository[T, K]) Delete(object *T) Error {
	if object == nil {
		return NewError("IllegalArgumentError", "Cannot delete nil entity")
	}
	ctx, err := this.validateActiveSession()
	if err != nil {
		return err
	}
	idVal, hasID := extractEntityID(object)
	if !hasID || isZeroValue(idVal) {
		return NewError("IllegalArgumentError", "Entity does not have a non-zero _id field")
	}
	if _, driverErr := this.Collection().DeleteOne(ctx, bson.M{bsonIDField: idVal}); driverErr != nil {
		return NewWrappedError("MongoError", driverErr)
	}
	return nil
}

// DeleteById removes a document by its _id value.
func (this *MongoRepository[T, K]) DeleteById(id K) Error {
	ctx, err := this.validateActiveSession()
	if err != nil {
		return err
	}
	if _, driverErr := this.Collection().DeleteOne(ctx, bson.M{bsonIDField: id}); driverErr != nil {
		return NewWrappedError("MongoError", driverErr)
	}
	return nil
}

// Save inserts a new document or upserts an existing one by its _id field.
// If the _id field is a zero [primitive.ObjectID], a new ObjectID is generated and assigned to object.
func (this *MongoRepository[T, K]) Save(object *T) (*T, Error) {
	if object == nil {
		return nil, NewError("IllegalArgumentError", "Cannot save nil entity")
	}
	ctx, err := this.validateActiveSession()
	if err != nil {
		return nil, err
	}

	coll := this.Collection()
	idField, idVal := prepareEntityIDForSave(object)

	if idVal != nil && !isZeroValue(idVal) {
		opts := options.Replace().SetUpsert(true)
		if _, driverErr := coll.ReplaceOne(ctx, bson.M{bsonIDField: idVal}, object, opts); driverErr != nil {
			return nil, NewWrappedError("MongoError", driverErr)
		}
		return object, nil
	}

	res, driverErr := coll.InsertOne(ctx, object)
	if driverErr != nil {
		return nil, NewWrappedError("MongoError", driverErr)
	}
	if idField.IsValid() && idField.CanSet() && res.InsertedID != nil {
		insertedVal := reflect.ValueOf(res.InsertedID)
		if insertedVal.Type().AssignableTo(idField.Type()) {
			idField.Set(insertedVal)
		}
	}
	return object, nil
}

// SaveAll inserts or upserts a slice of documents using a single MongoDB bulk write.
func (this *MongoRepository[T, K]) SaveAll(objects []*T) ([]*T, Error) {
	if len(objects) == 0 {
		return objects, nil
	}
	ctx, err := this.validateActiveSession()
	if err != nil {
		return nil, err
	}

	models := make([]mongo.WriteModel, 0, len(objects))
	for _, object := range objects {
		if object == nil {
			return nil, NewError("IllegalArgumentError", "Cannot save nil entity in SaveAll")
		}
		_, idVal := prepareEntityIDForSave(object)
		if idVal != nil && !isZeroValue(idVal) {
			models = append(models, mongo.NewReplaceOneModel().
				SetFilter(bson.M{bsonIDField: idVal}).
				SetReplacement(object).
				SetUpsert(true))
		} else {
			models = append(models, mongo.NewInsertOneModel().SetDocument(object))
		}
	}

	if _, driverErr := this.Collection().BulkWrite(ctx, models); driverErr != nil {
		return nil, NewWrappedError("MongoError", driverErr)
	}
	return objects, nil
}

// EnsureIndexes creates the provided MongoDB indexes on the repository's collection.
func (this *MongoRepository[T, K]) EnsureIndexes(ctx context.Context, models ...mongo.IndexModel) ([]string, Error) {
	if len(models) == 0 {
		return nil, nil
	}
	//nolint:contextcheck // Fallback to active repository session context when nil
	execCtx := ctx
	if execCtx == nil {
		execCtx = this.activeContext()
	}
	names, err := this.Collection().Indexes().CreateMany(execCtx, models)
	if err != nil {
		return nil, NewWrappedError("MongoError", err)
	}
	return names, nil
}

// PostConstruct implements [InnerBeamPostConstruct] to parse and wire derived query function fields
// declared on the embedding repository struct.
func (this *MongoRepository[T, K]) PostConstruct(parent any) Error {
	this.collectionName = this.resolveCollectionName()
	reflectType := reflect.TypeOf(parent)
	queryBuilder := mongoQueryBuilder[T, K]{}.New(this.logger)
	for field := range reflectType.Elem().Fields() {
		if field.Type.Kind() != reflect.Func {
			continue
		}
		function := &field

		compiled, err := queryBuilder.Build(function)
		if err != nil {
			err.LogError()
			continue
		}
		f := reflect.ValueOf(parent).Elem().FieldByName(function.Name)
		if !function.IsExported() {
			f = UnprotectValue(f)
		}
		query := compiled.Query(this.database, this.transactionManager, this.collectionName)
		f.Set(reflect.MakeFunc(function.Type, query))
	}
	return nil
}

func findIDField(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return reflect.Value{}
	}

	t := v.Type()
	// Prefer explicit bson:"_id" tag
	for i := range t.NumField() {
		field := t.Field(i)
		bsonName, inline, skip := parseBSONTag(&field)
		if skip {
			continue
		}
		if inline {
			if sub := findIDField(v.Field(i)); sub.IsValid() {
				return sub
			}
			continue
		}
		if bsonName == bsonIDField {
			return v.Field(i)
		}
	}
	// Fallback to field named ID
	if f := v.FieldByName("ID"); f.IsValid() {
		return f
	}
	return reflect.Value{}
}

func extractEntityID(object any) (any, bool) {
	field := findIDField(reflect.ValueOf(object))
	if !field.IsValid() {
		return nil, false
	}
	return field.Interface(), true
}

func prepareEntityIDForSave(object any) (idField reflect.Value, idVal any) {
	idField = findIDField(reflect.ValueOf(object))
	if !idField.IsValid() {
		return reflect.Value{}, nil
	}
	if idField.Type() == objectIDType && idField.IsZero() && idField.CanSet() {
		newID := primitive.NewObjectID()
		idField.Set(reflect.ValueOf(newID))
		return idField, newID
	}
	return idField, idField.Interface()
}

func isZeroValue(val any) bool {
	if val == nil {
		return true
	}
	return reflect.ValueOf(val).IsZero()
}
