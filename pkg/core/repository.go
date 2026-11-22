package core

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"

	"github.com/timandy/routine"
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

// MongoTransactionalResolver can be implemented by a repository struct embedding [MongoRepository]
// to return a [MongoTransactional] for a given [context.Context] (for example, using an injected
// [Foundry] of [MongoTransactional]).
type MongoTransactionalResolver interface {
	MongoTransactional(ctx context.Context) MongoTransactional
}

// MongoEntityTypeProvider is implemented by [MongoRepository] (and promoted to any embedding repository)
// to expose the managed entity's [reflect.Type].
type MongoEntityTypeProvider interface {
	EntityType() reflect.Type
}

// MongoRepositoryTypeFromContext extracts the embedding repository's [reflect.Type] (e.g. *UserRepository)
// from a Prototype [context.Context] (checking [PrototypeContextProxyType] first, then [PrototypeContextParentType]).
func MongoRepositoryTypeFromContext(ctx context.Context) reflect.Type {
	if t, ok := ctx.Value(PrototypeContextProxyType).(reflect.Type); ok {
		return t
	}
	if t, ok := ctx.Value(PrototypeContextParentType).(reflect.Type); ok {
		return t
	}
	return nil
}

// MongoEntityTypeFromContext extracts the entity's [reflect.Type] (e.g. User) from a Prototype [context.Context]
// (checking [PrototypeContextProxyInstance] first, then [PrototypeContextParentInstance]).
func MongoEntityTypeFromContext(ctx context.Context) reflect.Type {
	if p, ok := ctx.Value(PrototypeContextProxyInstance).(MongoEntityTypeProvider); ok {
		return p.EntityType()
	}
	if p, ok := ctx.Value(PrototypeContextParentInstance).(MongoEntityTypeProvider); ok {
		return p.EntityType()
	}
	return nil
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

type txContextEntry struct {
	//nolint:containedctx // Caches resolved MongoTransactional for the active goroutine context
	ctx context.Context
	tx  MongoTransactional
}

// MongoRepository is the base embeddable repository for MongoDB entities.
// Embed this struct inside a domain repository alongside unexported query function fields
// (e.g. findByName func(string) *User) to have Rays automatically wire them during container initialization.
type MongoRepository[T any, K comparable] struct {
	Repository
	txManager          TransactionManager `@:"Inject"`
	transactionManager MongoTransactional `@:"Inject"`
	logger             Logging            `@:"Inject"`
	resolver           MongoTransactionalResolver
	txCache            routine.ThreadLocal[*txContextEntry]
	collectionName     string
}

// EntityType returns the [reflect.Type] of the entity T managed by this repository.
func (this *MongoRepository[T, K]) EntityType() reflect.Type {
	return reflect.TypeFor[T]()
}

func (this *MongoRepository[T, K]) tx() MongoTransactional {
	if this.resolver == nil {
		return this.transactionManager
	}
	ctx := this.txManager.GetContext()
	comparableCtx := reflect.ValueOf(ctx).Comparable()
	if this.txCache != nil && comparableCtx {
		if entry := this.txCache.Get(); entry != nil && entry.tx != nil && reflect.ValueOf(entry.ctx).Comparable() && entry.ctx == ctx {
			return entry.tx
		}
	}
	resolved := this.resolver.MongoTransactional(ctx)
	if resolved == nil {
		return this.transactionManager
	}
	if this.txCache != nil && comparableCtx {
		this.txCache.Set(&txContextEntry{ctx: ctx, tx: resolved})
	}
	return resolved
}

func (this *MongoRepository[T, K]) txAndDB() (MongoTransactional, *mongo.Database) {
	tx := this.tx()
	if tx == nil {
		return nil, nil
	}
	return tx, tx.Database()
}

func (this *MongoRepository[T, K]) recordError(err Error) Error {
	if err != nil {
		if tx := this.tx(); tx != nil {
			tx.SetError(err)
		}
	}
	return err
}

func (this *MongoRepository[T, K]) validateActiveSession() (context.Context, Error) {
	tx := this.tx()
	if tx == nil {
		return context.Background(), nil
	}
	ms := tx.Get()
	if err := ms.Validate(); err != nil {
		return ms.Context(), err
	}
	return ms.Context(), nil
}

// newQueryErrorReporter captures the active [MongoSession] from tx at query construction time
// and returns an error-reporting callback that reports errors to that session, or to any session
// subsequently rebound on the query via [MongoQuery.WithContext].
func newQueryErrorReporter(tx MongoTransactional) (ctx context.Context, recordErr func(context.Context, Error)) {
	var sess *MongoSession
	ctx = context.Background()
	if tx != nil {
		sess = tx.Get()
		ctx = sess
	}
	return ctx, func(queryCtx context.Context, err Error) {
		if err != nil && tx != nil {
			target := sess
			if queryCtx != nil && queryCtx != ctx {
				target = extractSession(queryCtx)
			}
			tx.ReportSessionError(target, err)
		}
	}
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
	_, db := this.txAndDB()
	if db == nil {
		return nil
	}
	return db.Collection(this.resolveCollectionName())
}

// Query creates a new [*MongoQuery] targeting T's collection with any active ThreadLocal transaction context.
// Terminal query errors are reported to the session captured by Query, or to the session rebound via
// [MongoQuery.WithContext].
func (this *MongoRepository[T, K]) Query(queries ...MongoQueryFunc) *MongoQuery {
	tx, db := this.txAndDB()
	ctx, recordErr := newQueryErrorReporter(tx)
	q := NewMongoQuery(ctx, db, this.resolveCollectionName()).withErrorReporter(recordErr)
	q = q.Scopes(queries...)
	recordErr(q.ctx, q.Error())
	return q
}

// FindById finds a document by its _id field and returns an [Optional] pointer.
func (this *MongoRepository[T, K]) FindById(id K) Optional[*T] {
	coll := this.Collection()
	if coll == nil {
		return OptionalEmpty[*T]()
	}
	ctx, err := this.validateActiveSession()
	if err != nil {
		//nolint:errcheck // FindById returns Optional[*T] while recording session error on transaction manager
		_ = this.recordError(err)
		return OptionalEmpty[*T]()
	}
	var t T
	res := coll.FindOne(ctx, bson.M{bsonIDField: id})
	if decodeErr := res.Decode(&t); decodeErr != nil {
		if !errors.Is(decodeErr, mongo.ErrNoDocuments) {
			//nolint:errcheck // FindById returns Optional[*T] while recording decode error on transaction manager
			_ = this.recordError(NewWrappedError("MongoError", decodeErr))
		}
		return OptionalEmpty[*T]()
	}
	return OptionalOf(&t)
}

// FindAll returns all documents in the entity collection.
func (this *MongoRepository[T, K]) FindAll() ([]T, Error) {
	var results []T
	if err := this.Query().Find(&results); err != nil {
		return nil, this.recordError(err)
	}
	return results, nil
}

// Count returns the total number of documents in the entity collection.
func (this *MongoRepository[T, K]) Count() (int64, Error) {
	count, err := this.Query().Count()
	return count, this.recordError(err)
}

// ExistsById returns true if a document with the given _id exists in the collection.
func (this *MongoRepository[T, K]) ExistsById(id K) (bool, Error) {
	coll := this.Collection()
	if coll == nil {
		return false, this.recordError(NewError("MongoConfigurationError", "Database is not configured for repository"))
	}
	ctx, err := this.validateActiveSession()
	if err != nil {
		return false, this.recordError(err)
	}
	count, driverErr := coll.CountDocuments(
		ctx,
		bson.M{bsonIDField: id},
		options.Count().SetLimit(1),
	)
	if driverErr != nil {
		return false, this.recordError(NewWrappedError("MongoError", driverErr))
	}
	return count > 0, nil
}

// Delete removes the given entity document by its _id field.
// If no _id-mapped field exists, entity-based deletion is rejected and callers must use DeleteById with the actual key.
func (this *MongoRepository[T, K]) Delete(object *T) Error {
	if object == nil {
		return this.recordError(NewError("IllegalArgumentError", "Cannot delete nil entity"))
	}
	ctx, err := this.validateActiveSession()
	if err != nil {
		return this.recordError(err)
	}
	coll := this.Collection()
	st, encodeErr := entityIDStatus(coll, object)
	if encodeErr != nil {
		return this.recordError(encodeErr)
	}
	if !st.hasEncoded {
		return this.recordError(NewError("IllegalArgumentError", "Entity does not have a non-zero _id field; use DeleteById with the actual key"))
	}
	if coll == nil {
		return this.recordError(NewError("MongoConfigurationError", "Database is not configured for repository"))
	}
	if _, driverErr := coll.DeleteOne(ctx, bson.M{bsonIDField: st.val}); driverErr != nil {
		return this.recordError(NewWrappedError("MongoError", driverErr))
	}
	return nil
}

// DeleteById removes a document by its _id value.
func (this *MongoRepository[T, K]) DeleteById(id K) Error {
	ctx, err := this.validateActiveSession()
	if err != nil {
		return this.recordError(err)
	}
	coll := this.Collection()
	if coll == nil {
		return this.recordError(NewError("MongoConfigurationError", "Database is not configured for repository"))
	}
	if _, driverErr := coll.DeleteOne(ctx, bson.M{bsonIDField: id}); driverErr != nil {
		return this.recordError(NewWrappedError("MongoError", driverErr))
	}
	return nil
}

// Save inserts a new document or upserts an existing one by its _id field.
// ID generation depends on the _id field's Go type: zero [primitive.ObjectID] and
// *[primitive.ObjectID] fields receive a generated ObjectID; other field types with
// omitted serialized _id values return IllegalArgumentError without writing, while
// explicit zero values are upserted as-is.
func (this *MongoRepository[T, K]) Save(object *T) (*T, Error) {
	if object == nil {
		return nil, this.recordError(NewError("IllegalArgumentError", "Cannot save nil entity"))
	}
	ctx, err := this.validateActiveSession()
	if err != nil {
		return nil, this.recordError(err)
	}

	coll := this.Collection()
	if coll == nil {
		return nil, this.recordError(NewError("MongoConfigurationError", "Database is not configured for repository"))
	}
	useJSON := collectionUseJSONStructTags(coll)
	path := findIDPath(reflect.TypeOf(object), useJSON)
	ptrSnapshots := captureInlinePointers(reflect.ValueOf(object), path)
	origIDField := findIDField(reflect.ValueOf(object), useJSON)
	origID, origVal := captureIDSnapshot(origIDField)
	mutation := entityMutation{
		root:     reflect.ValueOf(object),
		idField:  origIDField,
		origID:   origID,
		origVal:  origVal,
		pointers: ptrSnapshots,
		useJSON:  useJSON,
	}

	st, encodeErr := entityIDStatus(coll, object)
	if encodeErr != nil {
		rollbackMutations([]entityMutation{mutation})
		return nil, this.recordError(encodeErr)
	}

	if st.hasEncoded {
		opts := options.Replace().SetUpsert(true)
		if _, driverErr := coll.ReplaceOne(ctx, bson.M{bsonIDField: st.val}, bson.Raw(st.docBytes), opts); driverErr != nil {
			return nil, this.recordError(NewWrappedError("MongoError", driverErr))
		}
		return object, nil
	}

	if len(path) == 0 {
		if _, driverErr := coll.InsertOne(ctx, bson.Raw(st.docBytes)); driverErr != nil {
			return nil, this.recordError(NewWrappedError("MongoError", driverErr))
		}
		return object, nil
	}

	idField := st.field
	var allocated []reflect.Value
	if !idField.IsValid() {
		idField, allocated = initIDField(reflect.ValueOf(object), useJSON)
	}
	mutation.allocated = allocated
	mutation.idField = idField

	if !assignGeneratedID(idField) {
		rollbackMutations([]entityMutation{mutation})
		return nil, this.recordError(NewError("IllegalArgumentError", "Entity does not have a settable _id field to retain generated ID in Save"))
	}

	generatedOID, ok := extractObjectID(idField)
	mutation.generatedOID = generatedOID

	docBytes, encErr := marshalWithCollection(coll, object)
	if encErr != nil {
		rollbackMutations([]entityMutation{mutation})
		return nil, this.recordError(NewWrappedError("MongoError", encErr))
	}

	retainedField := findIDField(reflect.ValueOf(object), useJSON)
	retainedOID, retainedOk := extractObjectID(retainedField)
	rawVal, lookupErr := bson.Raw(docBytes).LookupErr(bsonIDField)
	if !ok || !retainedOk || retainedOID != generatedOID || lookupErr != nil || rawVal.Type != bson.TypeObjectID || rawVal.ObjectID() != generatedOID {
		rollbackMutations([]entityMutation{mutation})
		return nil, this.recordError(NewError("IllegalArgumentError", "Entity does not have an _id field matching generated ID in Save"))
	}

	if _, driverErr := coll.InsertOne(ctx, rawDocument(docBytes)); driverErr != nil {
		return nil, this.recordError(NewWrappedError("MongoError", driverErr))
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
		return nil, this.recordError(err)
	}

	coll := this.Collection()
	if coll == nil {
		return nil, this.recordError(NewError("MongoConfigurationError", "Database is not configured for repository"))
	}

	useJSON := collectionUseJSONStructTags(coll)
	mutations := make([]entityMutation, len(objects))
	for i, object := range objects {
		if object == nil {
			rollbackMutations(mutations[:i])
			return nil, this.recordError(NewError("IllegalArgumentError", "Cannot save nil entity in SaveAll"))
		}
		path := findIDPath(reflect.TypeOf(object), useJSON)
		ptrSnapshots := captureInlinePointers(reflect.ValueOf(object), path)
		origIDField := findIDField(reflect.ValueOf(object), useJSON)
		origID, origVal := captureIDSnapshot(origIDField)
		mutations[i] = entityMutation{
			root:     reflect.ValueOf(object),
			idField:  origIDField,
			origID:   origID,
			origVal:  origVal,
			pointers: ptrSnapshots,
			useJSON:  useJSON,
		}
	}

	models := make([]mongo.WriteModel, 0, len(objects))
	for i, object := range objects {
		st, encodeErr := entityIDStatus(coll, object)
		if encodeErr != nil {
			rollbackMutations(mutations)
			return nil, this.recordError(encodeErr)
		}
		if st.hasEncoded {
			models = append(models, mongo.NewReplaceOneModel().
				SetFilter(bson.M{bsonIDField: st.val}).
				SetReplacement(bson.Raw(st.docBytes)).
				SetUpsert(true))
		} else {
			idField := st.field
			var allocated []reflect.Value
			if !idField.IsValid() {
				idField, allocated = initIDField(reflect.ValueOf(object), useJSON)
			}
			mutations[i].allocated = allocated
			mutations[i].idField = idField

			if !assignGeneratedID(idField) {
				rollbackMutations(mutations)
				return nil, this.recordError(NewError("IllegalArgumentError", "Entity does not have a settable _id field to retain generated ID in SaveAll"))
			}
			generatedOID, ok := extractObjectID(idField)
			mutations[i].generatedOID = generatedOID

			docBytes, encErr := marshalWithCollection(coll, object)
			if encErr != nil {
				rollbackMutations(mutations)
				return nil, this.recordError(NewWrappedError("MongoError", encErr))
			}
			retainedField := findIDField(reflect.ValueOf(object), useJSON)
			retainedOID, retainedOk := extractObjectID(retainedField)
			rawVal, lookupErr := bson.Raw(docBytes).LookupErr(bsonIDField)
			if !ok || !retainedOk || retainedOID != generatedOID || lookupErr != nil || rawVal.Type != bson.TypeObjectID || rawVal.ObjectID() != generatedOID {
				rollbackMutations(mutations)
				return nil, this.recordError(NewError("IllegalArgumentError", "Entity does not have an _id field matching generated ID in SaveAll"))
			}
			models = append(models, mongo.NewInsertOneModel().SetDocument(rawDocument(docBytes)))
		}
	}

	for i := range mutations {
		m := &mutations[i]
		if m.generatedOID.IsZero() {
			continue
		}
		retainedField := findIDField(m.root, m.useJSON)
		retainedOID, retainedOk := extractObjectID(retainedField)
		if !retainedOk || retainedOID != m.generatedOID {
			rollbackMutations(mutations)
			return nil, this.recordError(NewError("IllegalArgumentError", "Entity does not have an _id field matching generated ID in SaveAll"))
		}
	}

	if _, driverErr := coll.BulkWrite(ctx, models); driverErr != nil {
		return nil, this.recordError(NewWrappedError("MongoError", driverErr))
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
		return nil, this.recordError(sessErr)
	}
	//nolint:contextcheck // Fallback to active repository session context when caller passes nil
	execCtx := ctx
	if execCtx == nil {
		execCtx = activeCtx
	}
	coll := this.Collection()
	if coll == nil {
		return nil, this.recordError(NewError("MongoConfigurationError", "Database is not configured for repository"))
	}
	names, err := coll.Indexes().CreateMany(execCtx, models)
	if err != nil {
		return nil, this.recordError(NewWrappedError("MongoError", err))
	}
	return names, nil
}

// PostConstruct implements [InnerBeamPostConstruct] to parse and wire derived query function fields
// declared on the embedding repository struct.
func (this *MongoRepository[T, K]) PostConstruct(parent any) Error {
	this.txCache = routine.NewThreadLocal[*txContextEntry]()
	this.collectionName = this.resolveCollectionName()
	reflectType := reflect.TypeOf(parent)
	queryBuilder := mongoQueryBuilder[T, K]{}.New(this.logger, this.Collection())
	if resolver, ok := parent.(MongoTransactionalResolver); ok {
		this.resolver = resolver
	}
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
		query := compiled.Query(this.txAndDB, this.collectionName)
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

type idPathNode struct {
	t    reflect.Type
	path []int
}

func findIDPath(t reflect.Type, useJSON bool) []int {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}

	visited := map[reflect.Type]bool{t: true}
	currentLevel := []idPathNode{{t: t, path: nil}}
	for len(currentLevel) > 0 {
		var nextLevel []idPathNode

		// Phase 1: Check direct fields at current level.
		// Dominance rule: shallowest field matching _id wins over deeper inline fields.
		for _, node := range currentLevel {
			for i := range node.t.NumField() {
				field := node.t.Field(i)
				if !field.IsExported() {
					continue
				}
				bsonName, inline, skip := parseBSONTag(&field, useJSON)
				if skip || inline {
					continue
				}
				if bsonName == bsonIDField {
					res := make([]int, len(node.path), len(node.path)+1)
					copy(res, node.path)
					return append(res, i)
				}
			}
		}

		// Phase 2: If no direct _id field at this level, collect inline structs for next level.
		for _, node := range currentLevel {
			for i := range node.t.NumField() {
				field := node.t.Field(i)
				if !field.IsExported() {
					continue
				}
				_, inline, skip := parseBSONTag(&field, useJSON)
				if skip || !inline {
					continue
				}
				ft := field.Type
				for ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				if ft.Kind() == reflect.Struct && !visited[ft] {
					visited[ft] = true
					subPath := make([]int, len(node.path), len(node.path)+1)
					copy(subPath, node.path)
					nextLevel = append(nextLevel, idPathNode{
						t:    ft,
						path: append(subPath, i),
					})
				}
			}
		}

		currentLevel = nextLevel
	}

	return nil
}

func terminalIDType(t reflect.Type, path []int) reflect.Type {
	cur := t
	for k, idx := range path {
		for cur.Kind() == reflect.Pointer {
			cur = cur.Elem()
		}
		field := cur.Field(idx)
		if k == len(path)-1 {
			return field.Type
		}
		cur = field.Type
	}
	return nil
}

func supportsGeneratedID(t reflect.Type) bool {
	if t == nil {
		return false
	}
	return objectIDType.AssignableTo(t) || reflect.PointerTo(objectIDType).AssignableTo(t)
}

type rawDocument []byte

// MarshalBSON returns the underlying pre-encoded BSON bytes without transformation.
func (r rawDocument) MarshalBSON() ([]byte, error) {
	return r, nil
}

type pointerSnapshot struct {
	field   reflect.Value
	origVal reflect.Value
}

func captureInlinePointers(v reflect.Value, path []int) []pointerSnapshot {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct || len(path) <= 1 {
		return nil
	}
	var snapshots []pointerSnapshot
	cur := v
	for _, idx := range path[:len(path)-1] {
		field := cur.Field(idx)
		switch field.Kind() {
		case reflect.Pointer:
			var origVal reflect.Value
			if field.IsNil() {
				origVal = reflect.Zero(field.Type())
			} else {
				origVal = reflect.ValueOf(field.Interface())
			}
			snapshots = append(snapshots, pointerSnapshot{
				field:   field,
				origVal: origVal,
			})
			if field.IsNil() {
				return snapshots
			}
			cur = field.Elem()
		case reflect.Struct:
			cur = field
		default:
			return snapshots
		}
	}
	return snapshots
}

func captureIDSnapshot(origIDField reflect.Value) (origID, origVal reflect.Value) {
	if !origIDField.IsValid() {
		return reflect.Value{}, reflect.Value{}
	}
	origID = reflect.New(origIDField.Type()).Elem()
	origID.Set(origIDField)
	if origIDField.Kind() == reflect.Pointer && !origIDField.IsNil() {
		val := reflect.New(origIDField.Type().Elem()).Elem()
		val.Set(origIDField.Elem())
		origVal = val
	}
	return origID, origVal
}

type entityMutation struct {
	root         reflect.Value
	idField      reflect.Value
	origID       reflect.Value
	origVal      reflect.Value
	allocated    []reflect.Value
	pointers     []pointerSnapshot
	generatedOID primitive.ObjectID
	useJSON      bool
}

func rollbackMutations(mutations []entityMutation) {
	for i := len(mutations) - 1; i >= 0; i-- {
		m := &mutations[i]
		undoAllocations(m.allocated)
		for _, p := range slices.Backward(m.pointers) {
			if p.field.IsValid() && p.field.CanSet() && p.origVal.IsValid() {
				p.field.Set(p.origVal)
			}
		}
		if m.origVal.IsValid() && m.origID.IsValid() && m.origID.Kind() == reflect.Pointer && !m.origID.IsNil() {
			if m.origID.Elem().CanSet() {
				m.origID.Elem().Set(m.origVal)
			}
		}
		targetID := m.idField
		if m.root.IsValid() {
			if resolvedID := findIDField(m.root, m.useJSON); resolvedID.IsValid() {
				targetID = resolvedID
			}
		}
		if targetID.IsValid() && targetID.CanSet() && m.origID.IsValid() {
			targetID.Set(m.origID)
		}
	}
}

func undoAllocations(allocated []reflect.Value) {
	for _, ptr := range allocated {
		if ptr.IsValid() && ptr.CanSet() {
			ptr.Set(reflect.Zero(ptr.Type()))
		}
	}
}

func extractObjectID(idField reflect.Value) (primitive.ObjectID, bool) {
	if !idField.IsValid() {
		return primitive.NilObjectID, false
	}
	if idField.Kind() == reflect.Pointer {
		if idField.IsNil() {
			return primitive.NilObjectID, false
		}
		if oid, ok := idField.Elem().Interface().(primitive.ObjectID); ok {
			return oid, true
		}
		return primitive.NilObjectID, false
	}
	if oid, ok := idField.Interface().(primitive.ObjectID); ok {
		return oid, true
	}
	return primitive.NilObjectID, false
}

func initIDField(v reflect.Value, useJSON bool) (idField reflect.Value, allocated []reflect.Value) {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}, nil
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return reflect.Value{}, nil
	}

	path := findIDPath(v.Type(), useJSON)
	cur := v
	for k, idx := range path {
		if k == len(path)-1 {
			return cur.Field(idx), allocated
		}
		sub := cur.Field(idx)
		if sub.Kind() == reflect.Pointer {
			if sub.IsNil() {
				if !sub.CanSet() || !supportsGeneratedID(terminalIDType(v.Type(), path)) {
					return reflect.Value{}, nil
				}
				sub.Set(reflect.New(sub.Type().Elem()))
				allocated = append(allocated, sub)
			}
			sub = sub.Elem()
		}
		cur = sub
	}

	return reflect.Value{}, nil
}

func findIDField(v reflect.Value, useJSONTags ...bool) reflect.Value {
	useJSON := len(useJSONTags) > 0 && useJSONTags[0]
	initializePath := len(useJSONTags) > 1 && useJSONTags[1]
	if initializePath {
		field, _ := initIDField(v, useJSON)
		return field
	}

	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return reflect.Value{}
	}

	path := findIDPath(v.Type(), useJSON)
	cur := v
	for k, idx := range path {
		if k == len(path)-1 {
			return cur.Field(idx)
		}
		sub := cur.Field(idx)
		if sub.Kind() == reflect.Pointer {
			if sub.IsNil() {
				return reflect.Value{}
			}
			sub = sub.Elem()
		}
		cur = sub
	}

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

// assignGeneratedID sets a newly generated ObjectID on idField if its Go type supports
// generated IDs. ID generation depends on the _id field's Go type: zero [primitive.ObjectID]
// and *[primitive.ObjectID] fields receive a generated ObjectID; other field types with
// omitted serialized _id values return IllegalArgumentError without writing, while
// explicit zero values are upserted as-is.
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
