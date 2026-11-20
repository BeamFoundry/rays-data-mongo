package core

import (
	"bytes"
	"context"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
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
	if this.database == nil {
		return nil
	}
	return this.database.Collection(this.resolveCollectionName())
}

// Query creates a new [*MongoQuery] targeting T's collection with any active ThreadLocal transaction context.
func (this *MongoRepository[T, K]) Query(queries ...MongoQueryFunc) *MongoQuery {
	q := NewMongoQuery(this.activeContext(), this.database, this.resolveCollectionName())
	return q.Scopes(queries...)
}

// FindById finds a document by its _id field and returns an [Optional] pointer.
func (this *MongoRepository[T, K]) FindById(id K) Optional[*T] {
	if this.database == nil {
		return OptionalEmpty[*T]()
	}
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
	if this.database == nil {
		return false, NewError("MongoConfigurationError", "Database is not configured for repository")
	}
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
// If no _id-mapped field exists, entity-based deletion is rejected and callers must use DeleteById with the actual key.
func (this *MongoRepository[T, K]) Delete(object *T) Error {
	if object == nil {
		return NewError("IllegalArgumentError", "Cannot delete nil entity")
	}
	ctx, err := this.validateActiveSession()
	if err != nil {
		return err
	}
	coll := this.Collection()
	st, encodeErr := entityIDStatus(coll, object)
	if encodeErr != nil {
		return encodeErr
	}
	if !st.hasEncoded {
		return NewError("IllegalArgumentError", "Entity does not have a non-zero _id field; use DeleteById with the actual key")
	}
	if coll == nil {
		return NewError("MongoConfigurationError", "Database is not configured for repository")
	}
	if _, driverErr := coll.DeleteOne(ctx, bson.M{bsonIDField: st.val}); driverErr != nil {
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
	coll := this.Collection()
	if coll == nil {
		return NewError("MongoConfigurationError", "Database is not configured for repository")
	}
	if _, driverErr := coll.DeleteOne(ctx, bson.M{bsonIDField: id}); driverErr != nil {
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
	if coll == nil {
		return nil, NewError("MongoConfigurationError", "Database is not configured for repository")
	}
	st, encodeErr := entityIDStatus(coll, object)
	if encodeErr != nil {
		return nil, encodeErr
	}

	if st.hasEncoded {
		opts := options.Replace().SetUpsert(true)
		if _, driverErr := coll.ReplaceOne(ctx, bson.M{bsonIDField: st.val}, bson.Raw(st.docBytes), opts); driverErr != nil {
			return nil, NewWrappedError("MongoError", driverErr)
		}
		return object, nil
	}

	_ = assignGeneratedID(st.field)

	res, driverErr := coll.InsertOne(ctx, object)
	if driverErr != nil {
		return nil, NewWrappedError("MongoError", driverErr)
	}
	if st.field.IsValid() && st.field.CanSet() && res.InsertedID != nil {
		insertedVal := reflect.ValueOf(res.InsertedID)
		if insertedVal.Type().AssignableTo(st.field.Type()) {
			st.field.Set(insertedVal)
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

	coll := this.Collection()
	if coll == nil {
		return nil, NewError("MongoConfigurationError", "Database is not configured for repository")
	}
	models := make([]mongo.WriteModel, 0, len(objects))
	for _, object := range objects {
		if object == nil {
			return nil, NewError("IllegalArgumentError", "Cannot save nil entity in SaveAll")
		}
		st, encodeErr := entityIDStatus(coll, object)
		if encodeErr != nil {
			return nil, encodeErr
		}
		if st.hasEncoded {
			models = append(models, mongo.NewReplaceOneModel().
				SetFilter(bson.M{bsonIDField: st.val}).
				SetReplacement(bson.Raw(st.docBytes)).
				SetUpsert(true))
		} else {
			if !assignGeneratedID(st.field) {
				return nil, NewError("IllegalArgumentError", "Entity does not have a settable _id field to retain generated ID in SaveAll")
			}
			models = append(models, mongo.NewInsertOneModel().SetDocument(object))
		}
	}

	if _, driverErr := coll.BulkWrite(ctx, models); driverErr != nil {
		return nil, NewWrappedError("MongoError", driverErr)
	}
	return objects, nil
}

// EnsureIndexes creates the provided MongoDB indexes on the repository's collection.
func (this *MongoRepository[T, K]) EnsureIndexes(ctx context.Context, models ...mongo.IndexModel) ([]string, Error) {
	if len(models) == 0 {
		return nil, nil
	}
	activeCtx, sessErr := this.validateActiveSession()
	if sessErr != nil {
		return nil, sessErr
	}
	//nolint:contextcheck // Fallback to active repository session context when caller passes nil
	execCtx := ctx
	if execCtx == nil {
		execCtx = activeCtx
	}
	coll := this.Collection()
	if coll == nil {
		return nil, NewError("MongoConfigurationError", "Database is not configured for repository")
	}
	names, err := coll.Indexes().CreateMany(execCtx, models)
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
	queryBuilder := mongoQueryBuilder[T, K]{}.New(this.logger, this.Collection())
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

func collectionUseJSONStructTags(coll *mongo.Collection) bool {
	if coll == nil {
		return false
	}
	optsField := reflect.ValueOf(coll).Elem().FieldByName("bsonOpts")
	if optsField.IsValid() {
		optsVal := UnprotectValue(optsField)
		if o, ok := optsVal.Interface().(*options.BSONOptions); ok && o != nil {
			return o.UseJSONStructTags
		}
	}
	return false
}

func findIDField(v reflect.Value, useJSONTags ...bool) reflect.Value {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return reflect.Value{}
	}

	useJSON := len(useJSONTags) > 0 && useJSONTags[0]

	currentLevel := []reflect.Value{v}
	for len(currentLevel) > 0 {
		var nextLevel []reflect.Value

		// Phase 1: Check direct fields at current level.
		// Shallowest field matching _id wins over deeper inline fields (dominance rule).
		for _, cur := range currentLevel {
			t := cur.Type()
			for i := range t.NumField() {
				field := t.Field(i)
				if !field.IsExported() {
					continue
				}
				bsonName, inline, skip := parseBSONTag(&field, useJSON)
				if skip || inline {
					continue
				}
				if bsonName == bsonIDField {
					return cur.Field(i)
				}
			}
		}

		// Phase 2: If no direct _id field at this level, collect inline structs for next level.
		for _, cur := range currentLevel {
			t := cur.Type()
			for i := range t.NumField() {
				field := t.Field(i)
				if !field.IsExported() {
					continue
				}
				_, inline, skip := parseBSONTag(&field, useJSON)
				if skip || !inline {
					continue
				}
				sub := cur.Field(i)
				for sub.Kind() == reflect.Pointer {
					if sub.IsNil() {
						break
					}
					sub = sub.Elem()
				}
				if sub.IsValid() && sub.Kind() == reflect.Struct {
					nextLevel = append(nextLevel, sub)
				}
			}
		}

		currentLevel = nextLevel
	}
	// Untagged "ID" fields are intentionally not selected as identity because the MongoDB driver
	// only serializes fields to "_id" if explicitly tagged with bson:"_id" (or json:"_id" with UseJSONStructTags).
	return reflect.Value{}
}

type idStatus struct {
	val        any
	field      reflect.Value
	docBytes   []byte
	hasEncoded bool
}

//nolint:wrapcheck,errcheck // Internal helper propagating BSON encoding errors; ValueWriter and Encoder constructors are infallible
func marshalWithCollection(coll *mongo.Collection, val any) ([]byte, error) {
	if coll == nil {
		return bson.Marshal(val)
	}
	cv := reflect.ValueOf(coll).Elem()
	regField := cv.FieldByName("registry")
	optsField := cv.FieldByName("bsonOpts")

	var reg *bsoncodec.Registry
	if regField.IsValid() {
		regVal := UnprotectValue(regField)
		if r, ok := regVal.Interface().(*bsoncodec.Registry); ok {
			reg = r
		}
	}

	var opts *options.BSONOptions
	if optsField.IsValid() {
		optsVal := UnprotectValue(optsField)
		if o, ok := optsVal.Interface().(*options.BSONOptions); ok {
			opts = o
		}
	}

	if reg == nil && opts == nil {
		return bson.Marshal(val)
	}

	buf := new(bytes.Buffer)
	vw, _ := bsonrw.NewBSONValueWriter(buf)
	enc, _ := bson.NewEncoder(vw)
	if opts != nil {
		if opts.UseJSONStructTags {
			enc.UseJSONStructTags()
		}
		if opts.ErrorOnInlineDuplicates {
			enc.ErrorOnInlineDuplicates()
		}
		if opts.IntMinSize {
			enc.IntMinSize()
		}
		if opts.NilMapAsEmpty {
			enc.NilMapAsEmpty()
		}
		if opts.NilSliceAsEmpty {
			enc.NilSliceAsEmpty()
		}
		if opts.NilByteSliceAsEmpty {
			enc.NilByteSliceAsEmpty()
		}
		if opts.OmitZeroStruct {
			enc.OmitZeroStruct()
		}
		if opts.StringifyMapKeysWithFmt {
			enc.StringifyMapKeysWithFmt()
		}
	}
	if reg != nil {
		_ = enc.SetRegistry(reg)
	}
	if err := enc.Encode(val); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func entityIDStatus(coll *mongo.Collection, object any) (idStatus, Error) {
	if object == nil {
		return idStatus{}, nil
	}
	useJSON := collectionUseJSONStructTags(coll)
	idField := findIDField(reflect.ValueOf(object), useJSON)

	rawBytes, err := marshalWithCollection(coll, object)
	if err != nil {
		return idStatus{field: idField}, NewWrappedError("MongoError", err)
	}
	rawVal, err := bson.Raw(rawBytes).LookupErr(bsonIDField)
	if err != nil {
		//nolint:nilerr // Missing _id field in serialized BSON is expected when omitted
		return idStatus{field: idField, docBytes: rawBytes}, nil
	}

	if rawVal.Type == bson.TypeNull {
		if idField.IsValid() && idField.Kind() == reflect.Pointer && idField.Type().Elem() == objectIDType && idField.IsNil() {
			return idStatus{field: idField, docBytes: rawBytes}, nil
		}
	}

	if rawVal.Type == bson.TypeObjectID {
		oid := rawVal.ObjectID()
		if oid.IsZero() {
			return idStatus{field: idField, docBytes: rawBytes}, nil
		}
		return idStatus{field: idField, val: oid, docBytes: rawBytes, hasEncoded: true}, nil
	}

	var decodedID any
	//nolint:errcheck // Unmarshaling valid BSON raw value into any cannot fail
	_ = rawVal.Unmarshal(&decodedID)
	return idStatus{field: idField, val: decodedID, docBytes: rawBytes, hasEncoded: true}, nil
}

func assignGeneratedID(idField reflect.Value) bool {
	if !idField.IsValid() || !idField.CanSet() {
		return false
	}
	if objectIDType.AssignableTo(idField.Type()) {
		idField.Set(reflect.ValueOf(primitive.NewObjectID()))
		return true
	}
	if reflect.PointerTo(objectIDType).AssignableTo(idField.Type()) {
		newID := primitive.NewObjectID()
		idField.Set(reflect.ValueOf(&newID))
		return true
	}
	return false
}
