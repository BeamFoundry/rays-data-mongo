package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/BeamFoundry/rays-data-mongo/pkg/tx"
	. "github.com/BeamFoundry/rays/pkg/lang"
)

const (
	bsonOpOr  = "$or"
	bsonOpAnd = "$and"
	bsonOpNot = "$not"
)

// MongoQueryFunc is a functional option that transforms a [*MongoQuery],
// analogous to OrmQueryFunc in rays-orm.
type MongoQueryFunc = func(*MongoQuery) *MongoQuery

// MongoQuery represents a composable, immutable MongoDB query builder and executor
// bound to a [*mongo.Database], collection name, and active execution [context.Context].
type MongoQuery struct {
	//nolint:containedctx // Stores execution/session context for fluent MongoDB query execution
	ctx            context.Context
	validator      interface{ Validate() Error }
	err            Error
	hint           any
	onError        func(context.Context, Error)
	collation      *options.Collation
	maxTime        *time.Duration
	skip           *int64
	limit          *int64
	collection     *mongo.Collection
	database       *mongo.Database
	collectionName string
	andFilters     []any
	sort           bson.D
	projection     bson.D
	extraFindOpts  []*options.FindOptions
}

func extractSession(ctx context.Context) *tx.MongoSession {
	if s, ok := ctx.(*tx.MongoSession); ok {
		return s
	}
	if ctx != nil {
		if s, ok := ctx.Value((*tx.MongoSession)(nil)).(*tx.MongoSession); ok {
			return s
		}
	}
	return nil
}

func extractValidator(ctx context.Context) interface{ Validate() Error } {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.(interface{ Validate() Error }); ok && v != nil {
		return v
	}
	if v, ok := ctx.Value((*tx.MongoSession)(nil)).(interface{ Validate() Error }); ok && v != nil {
		return v
	}
	return nil
}

// NewMongoQuery creates a new [*MongoQuery] targeting the given database and collection.
func NewMongoQuery(ctx context.Context, db *mongo.Database, collectionName string) *MongoQuery {
	//nolint:contextcheck // Fallback to background context when caller passes nil
	baseCtx := ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	var coll *mongo.Collection
	if db != nil && collectionName != "" {
		coll = db.Collection(collectionName)
	}
	return &MongoQuery{
		database:       db,
		collection:     coll,
		collectionName: collectionName,
		ctx:            baseCtx,
		validator:      extractValidator(baseCtx),
	}
}

func (this *MongoQuery) clone() *MongoQuery {
	if this == nil {
		return &MongoQuery{ctx: context.Background()}
	}
	c := *this
	if len(this.andFilters) > 0 {
		c.andFilters = make([]any, len(this.andFilters))
		for i, f := range this.andFilters {
			c.andFilters[i] = copyFilter(f)
		}
	}
	if len(this.sort) > 0 {
		c.sort = make(bson.D, len(this.sort))
		copy(c.sort, this.sort)
	}
	if len(this.projection) > 0 {
		c.projection = make(bson.D, len(this.projection))
		copy(c.projection, this.projection)
	}
	if this.limit != nil {
		v := *this.limit
		c.limit = &v
	}
	if this.skip != nil {
		v := *this.skip
		c.skip = &v
	}
	if this.maxTime != nil {
		v := *this.maxTime
		c.maxTime = &v
	}
	if len(this.extraFindOpts) > 0 {
		c.extraFindOpts = make([]*options.FindOptions, len(this.extraFindOpts))
		copy(c.extraFindOpts, this.extraFindOpts)
	}
	return &c
}

// Scopes applies a sequence of [MongoQueryFunc] functions to the query.
func (this *MongoQuery) Scopes(funcs ...MongoQueryFunc) *MongoQuery {
	q := this
	for _, f := range funcs {
		if f != nil {
			q = f(q)
		}
	}
	return q
}

// WithContext returns a copy of the query using the provided [context.Context].
// If ctx is (or wraps) a [*tx.MongoSession], session validation and terminal error
// reporting are rebound to that session; if ctx does not carry a [*tx.MongoSession],
// terminal error reporting is detached from the previously captured session.
func (this *MongoQuery) WithContext(ctx context.Context) *MongoQuery {
	q := this.clone()
	if ctx != nil {
		q.ctx = ctx
		q.validator = extractValidator(ctx)
	}
	return q
}

// Collection returns a copy of the query targeting a different collection name.
func (this *MongoQuery) Collection(name string) *MongoQuery {
	q := this.clone()
	q.collectionName = name
	if q.database != nil && name != "" {
		q.collection = q.database.Collection(name)
	} else {
		q.collection = nil
	}
	return q
}

// CollectionName returns the target collection name.
func (this *MongoQuery) CollectionName() string {
	return this.collectionName
}

// RawCollection returns the underlying [*mongo.Collection] for direct MongoDB driver calls.
func (this *MongoQuery) RawCollection() *mongo.Collection {
	if this == nil {
		return nil
	}
	return this.collection
}

// AddError attaches an [Error] to a cloned query without mutating the original handle.
func (this *MongoQuery) AddError(err Error) *MongoQuery {
	q := this.clone()
	if err != nil {
		q.err = err
	}
	return q
}

// Error returns any error accumulated during query construction.
func (this *MongoQuery) Error() Error {
	if this == nil {
		return nil
	}
	return this.err
}

// Where appends a filter document (such as bson.M, bson.D, or map[string]any) to the query's AND filters.
func (this *MongoQuery) Where(filter any) *MongoQuery {
	if filter == nil {
		return this
	}
	if d, ok := filter.(bson.D); ok && d == nil {
		filter = bson.D{}
	}
	q := this.clone()
	q.andFilters = append(q.andFilters, copyFilter(filter))
	return q
}

// Or adds an $or filter clause.
// If a single filter is provided and existing filters are present, it combines (existingFilters OR filter).
// If multiple filters are provided, it appends ($or: [filters...]) to the query.
func (this *MongoQuery) Or(filters ...any) *MongoQuery {
	if len(filters) == 0 {
		return this
	}
	q := this.clone()
	normFilters := make([]any, len(filters))
	for i, f := range filters {
		if d, ok := f.(bson.D); ok && d == nil {
			normFilters[i] = bson.D{}
		} else {
			normFilters[i] = f
		}
	}
	if len(normFilters) == 1 && len(q.andFilters) > 0 {
		existing := q.Filter()
		q.andFilters = []any{bson.M{bsonOpOr: bson.A{existing, copyFilter(normFilters[0])}}}
		return q
	}
	if len(normFilters) == 1 {
		q.andFilters = append(q.andFilters, copyFilter(normFilters[0]))
		return q
	}
	orSlice := make(bson.A, 0, len(normFilters))
	for _, f := range normFilters {
		orSlice = append(orSlice, copyFilter(f))
	}
	q.andFilters = append(q.andFilters, bson.M{bsonOpOr: orSlice})
	return q
}

// OrderByAsc appends an ascending sort rule for the given BSON field.
func (this *MongoQuery) OrderByAsc(field string) *MongoQuery {
	q := this.clone()
	q.sort = append(q.sort, bson.E{Key: field, Value: 1})
	return q
}

// OrderByDesc appends a descending sort rule for the given BSON field.
func (this *MongoQuery) OrderByDesc(field string) *MongoQuery {
	q := this.clone()
	q.sort = append(q.sort, bson.E{Key: field, Value: -1})
	return q
}

// Order applies an arbitrary sort document (bson.D, bson.E, or bson.M).
func (this *MongoQuery) Order(sort any) *MongoQuery {
	if sort == nil {
		return this
	}
	q := this.clone()
	switch s := sort.(type) {
	case bson.D:
		q.sort = append(q.sort, s...)
	case bson.E:
		q.sort = append(q.sort, s)
	case bson.M:
		if len(s) > 1 {
			q.err = NewError("IllegalArgumentError", "Compound sort requires bson.D for deterministic key ordering; map with multiple keys is not supported")
			return q
		}
		for k, v := range s {
			q.sort = append(q.sort, bson.E{Key: k, Value: v})
		}
	case map[string]any:
		if len(s) > 1 {
			q.err = NewError("IllegalArgumentError", "Compound sort requires bson.D for deterministic key ordering; map with multiple keys is not supported")
			return q
		}
		for k, v := range s {
			q.sort = append(q.sort, bson.E{Key: k, Value: v})
		}
	default:
		q.err = NewError("IllegalArgumentError", "Unsupported sort type")
	}
	return q
}

// Limit sets the maximum number of documents to return.
// Setting limit to 0 explicitly enforces zero returned documents.
func (this *MongoQuery) Limit(limit int64) *MongoQuery {
	q := this.clone()
	q.limit = &limit
	return q
}

// Skip sets the number of documents to skip. Passing 0 clears any previously set skip.
func (this *MongoQuery) Skip(skip int64) *MongoQuery {
	q := this.clone()
	if skip <= 0 {
		q.skip = nil
		return q
	}
	q.skip = &skip
	return q
}

// Offset is an alias for [MongoQuery.Skip].
func (this *MongoQuery) Offset(offset int64) *MongoQuery {
	return this.Skip(offset)
}

// Select includes only the specified BSON fields in the projection.
func (this *MongoQuery) Select(fields ...string) *MongoQuery {
	if len(fields) == 0 {
		return this
	}
	q := this.clone()
	for _, field := range fields {
		q.projection = append(q.projection, bson.E{Key: field, Value: 1})
	}
	return q
}

// Omit excludes the specified BSON fields from the projection.
func (this *MongoQuery) Omit(fields ...string) *MongoQuery {
	if len(fields) == 0 {
		return this
	}
	q := this.clone()
	for _, field := range fields {
		q.projection = append(q.projection, bson.E{Key: field, Value: 0})
	}
	return q
}

// Projection applies an arbitrary BSON projection document.
func (this *MongoQuery) Projection(proj any) *MongoQuery {
	if proj == nil {
		return this
	}
	q := this.clone()
	switch p := proj.(type) {
	case bson.D:
		q.projection = append(q.projection, p...)
	case bson.E:
		q.projection = append(q.projection, p)
	case bson.M:
		for k, v := range p {
			q.projection = append(q.projection, bson.E{Key: k, Value: v})
		}
	case map[string]any:
		for k, v := range p {
			q.projection = append(q.projection, bson.E{Key: k, Value: v})
		}
	default:
		q.err = NewError("IllegalArgumentError", "Unsupported projection type")
	}
	return q
}

// Hint sets the index hint for the query.
func (this *MongoQuery) Hint(hint any) *MongoQuery {
	q := this.clone()
	q.hint = hint
	return q
}

// MaxTime sets the maximum server execution duration for the query.
func (this *MongoQuery) MaxTime(d time.Duration) *MongoQuery {
	q := this.clone()
	q.maxTime = &d
	return q
}

// Collation sets the collation for string comparisons.
func (this *MongoQuery) Collation(collation *options.Collation) *MongoQuery {
	q := this.clone()
	q.collation = collation
	return q
}

// WithFindOptions appends raw MongoDB [*options.FindOptions] to the query.
func (this *MongoQuery) WithFindOptions(opts ...*options.FindOptions) *MongoQuery {
	q := this.clone()
	q.extraFindOpts = append(q.extraFindOpts, opts...)
	return q
}

// Filter compiles the accumulated filter conditions into a BSON filter document.
func (this *MongoQuery) Filter() any {
	switch len(this.andFilters) {
	case 0:
		return bson.D{}
	case 1:
		return copyFilter(this.andFilters[0])
	default:
		andSlice := make(bson.A, 0, len(this.andFilters))
		for _, f := range this.andFilters {
			andSlice = append(andSlice, copyFilter(f))
		}
		return bson.M{bsonOpAnd: andSlice}
	}
}

// FindOptions builds the slice of [*options.FindOptions] for this query.
func (this *MongoQuery) FindOptions() []*options.FindOptions {
	allOpts := make([]*options.FindOptions, 0, 1+len(this.extraFindOpts))
	base := options.Find()
	if len(this.sort) > 0 {
		sortCopy := make(bson.D, len(this.sort))
		copy(sortCopy, this.sort)
		base.SetSort(sortCopy)
	}
	if len(this.projection) > 0 {
		projCopy := make(bson.D, len(this.projection))
		copy(projCopy, this.projection)
		base.SetProjection(projCopy)
	}
	if this.limit != nil && *this.limit > 0 {
		base.SetLimit(*this.limit)
	}
	if this.skip != nil && *this.skip > 0 {
		base.SetSkip(*this.skip)
	}
	if this.hint != nil {
		base.SetHint(this.hint)
	}
	if this.maxTime != nil {
		base.SetMaxTime(*this.maxTime)
	}
	if this.collation != nil {
		base.SetCollation(this.collation)
	}
	allOpts = append(allOpts, base)
	allOpts = append(allOpts, this.extraFindOpts...)
	return allOpts
}

func (this *MongoQuery) findOneOptions() *options.FindOneOptions {
	opt := options.FindOne()
	if len(this.sort) > 0 {
		sortCopy := make(bson.D, len(this.sort))
		copy(sortCopy, this.sort)
		opt.SetSort(sortCopy)
	}
	if len(this.projection) > 0 {
		projCopy := make(bson.D, len(this.projection))
		copy(projCopy, this.projection)
		opt.SetProjection(projCopy)
	}
	if this.skip != nil && *this.skip > 0 {
		opt.SetSkip(*this.skip)
	}
	if this.hint != nil {
		opt.SetHint(this.hint)
	}
	if this.maxTime != nil {
		opt.SetMaxTime(*this.maxTime)
	}
	if this.collation != nil {
		opt.SetCollation(this.collation)
	}
	return opt
}

func (this *MongoQuery) withErrorReporter(fn func(context.Context, Error)) *MongoQuery {
	q := this.clone()
	q.onError = fn
	return q
}

func (this *MongoQuery) reportError(err Error) Error {
	if this != nil && this.onError != nil {
		this.onError(this.ctx, err)
	}
	return err
}

func (this *MongoQuery) validateExecution() (*mongo.Collection, Error) {
	if this == nil {
		return nil, NewError("IllegalArgumentError", "Query cannot be nil")
	}
	if this.err != nil {
		return nil, this.err
	}
	v := this.validator
	if v == nil {
		v = extractValidator(this.ctx)
	}
	if v != nil {
		if err := v.Validate(); err != nil {
			return nil, err
		}
	}
	coll := this.RawCollection()
	if coll == nil {
		return nil, NewError("IllegalArgumentError", "No MongoDB collection configured on query")
	}
	return coll, nil
}

// Find executes the query and decodes all matching documents into dest (must be a pointer to a slice).
func (this *MongoQuery) Find(dest any) Error {
	coll, err := this.validateExecution()
	if err != nil {
		return this.reportError(err)
	}
	if dest == nil {
		return this.reportError(NewError("IllegalArgumentError", "Destination cannot be nil"))
	}

	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Pointer || destVal.Elem().Kind() != reflect.Slice {
		return this.reportError(NewError("IllegalArgumentError", "Destination must be a pointer to a slice"))
	}

	// Enforce explicit Limit(0) by returning an empty slice without querying unbounded documents.
	if this.limit != nil && *this.limit == 0 {
		destVal.Elem().Set(reflect.MakeSlice(destVal.Elem().Type(), 0, 0))
		return nil
	}

	cursor, driverErr := coll.Find(this.ctx, this.Filter(), this.FindOptions()...)
	if driverErr != nil {
		return this.reportError(NewWrappedError("MongoError", driverErr))
	}
	defer func() {
		_ = cursor.Close(this.ctx)
	}()

	if driverErr := cursor.All(this.ctx, dest); driverErr != nil {
		return this.reportError(NewWrappedError("MongoError", driverErr))
	}
	return nil
}

// First executes the query for at most one document and decodes it into dest (must be a struct pointer).
func (this *MongoQuery) First(dest any) Error {
	coll, err := this.validateExecution()
	if err != nil {
		return this.reportError(err)
	}
	if dest == nil {
		return this.reportError(NewError("IllegalArgumentError", "Destination cannot be nil"))
	}
	if this.limit != nil && *this.limit == 0 {
		return this.reportError(NewWrappedError("NotFoundError", mongo.ErrNoDocuments))
	}

	res := coll.FindOne(this.ctx, this.Filter(), this.findOneOptions())
	if decodeErr := res.Decode(dest); decodeErr != nil {
		if errors.Is(decodeErr, mongo.ErrNoDocuments) {
			return this.reportError(NewWrappedError("NotFoundError", decodeErr))
		}
		return this.reportError(NewWrappedError("MongoError", decodeErr))
	}
	return nil
}

// Count returns the number of documents matching the query filter.
func (this *MongoQuery) Count() (int64, Error) {
	coll, err := this.validateExecution()
	if err != nil {
		return 0, this.reportError(err)
	}
	if this.limit != nil && *this.limit == 0 {
		return 0, nil
	}

	countOpts := options.Count()
	if this.limit != nil && *this.limit > 0 {
		countOpts.SetLimit(*this.limit)
	}
	if this.skip != nil && *this.skip > 0 {
		countOpts.SetSkip(*this.skip)
	}
	if this.hint != nil {
		countOpts.SetHint(this.hint)
	}
	if this.maxTime != nil {
		countOpts.SetMaxTime(*this.maxTime)
	}
	if this.collation != nil {
		countOpts.SetCollation(this.collation)
	}

	count, driverErr := coll.CountDocuments(this.ctx, this.Filter(), countOpts)
	if driverErr != nil {
		return 0, this.reportError(NewWrappedError("MongoError", driverErr))
	}
	return count, nil
}

// Exists returns true if at least one document matches the query filter.
func (this *MongoQuery) Exists() (bool, Error) {
	coll, err := this.validateExecution()
	if err != nil {
		return false, this.reportError(err)
	}
	if this.limit != nil && *this.limit == 0 {
		return false, nil
	}

	countOpts := options.Count().SetLimit(1)
	if this.skip != nil && *this.skip > 0 {
		countOpts.SetSkip(*this.skip)
	}
	if this.hint != nil {
		countOpts.SetHint(this.hint)
	}
	if this.maxTime != nil {
		countOpts.SetMaxTime(*this.maxTime)
	}
	if this.collation != nil {
		countOpts.SetCollation(this.collation)
	}

	count, driverErr := coll.CountDocuments(this.ctx, this.Filter(), countOpts)
	if driverErr != nil {
		return false, this.reportError(NewWrappedError("MongoError", driverErr))
	}
	return count > 0, nil
}

// Delete deletes all documents matching the query filter and returns the deleted document count.
func (this *MongoQuery) Delete() (int64, Error) {
	coll, err := this.validateExecution()
	if err != nil {
		return 0, this.reportError(err)
	}
	delOpts := options.Delete()
	if this.hint != nil {
		delOpts.SetHint(this.hint)
	}
	if this.collation != nil {
		delOpts.SetCollation(this.collation)
	}

	res, driverErr := coll.DeleteMany(this.ctx, this.Filter(), delOpts)
	if driverErr != nil {
		return 0, this.reportError(NewWrappedError("MongoError", driverErr))
	}
	return res.DeletedCount, nil
}

// Distinct retrieves distinct values for fieldName matching the query filter and decodes them into dest.
func (this *MongoQuery) Distinct(fieldName string, dest any) Error {
	coll, err := this.validateExecution()
	if err != nil {
		return this.reportError(err)
	}
	if dest == nil {
		return this.reportError(NewError("IllegalArgumentError", "Destination cannot be nil"))
	}

	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Pointer || destVal.Elem().Kind() != reflect.Slice {
		return this.reportError(NewError("IllegalArgumentError", "Destination must be a pointer to a slice"))
	}

	distinctOpts := options.Distinct()
	if this.maxTime != nil {
		distinctOpts.SetMaxTime(*this.maxTime)
	}
	if this.collation != nil {
		distinctOpts.SetCollation(this.collation)
	}

	vals, driverErr := coll.Distinct(this.ctx, fieldName, this.Filter(), distinctOpts)
	if driverErr != nil {
		return this.reportError(NewWrappedError("MongoError", driverErr))
	}

	elemType := destVal.Elem().Type().Elem()
	sliceVal := reflect.MakeSlice(destVal.Elem().Type(), 0, len(vals))
	for _, rawVal := range vals {
		converted, convErr := convertDistinctValue(rawVal, elemType)
		if convErr != nil {
			return this.reportError(convErr)
		}
		sliceVal = reflect.Append(sliceVal, converted)
	}
	destVal.Elem().Set(sliceVal)
	return nil
}

// Aggregate executes a MongoDB aggregation pipeline on the target collection and decodes results into dest.
func (this *MongoQuery) Aggregate(pipeline, dest any) Error {
	coll, err := this.validateExecution()
	if err != nil {
		return this.reportError(err)
	}
	if dest == nil {
		return this.reportError(NewError("IllegalArgumentError", "Destination cannot be nil"))
	}

	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Pointer || destVal.Elem().Kind() != reflect.Slice {
		return this.reportError(NewError("IllegalArgumentError", "Destination must be a pointer to a slice"))
	}

	aggOpts := options.Aggregate()
	if this.maxTime != nil {
		aggOpts.SetMaxTime(*this.maxTime)
	}
	if this.collation != nil {
		aggOpts.SetCollation(this.collation)
	}
	if this.hint != nil {
		aggOpts.SetHint(this.hint)
	}

	cursor, driverErr := coll.Aggregate(this.ctx, pipeline, aggOpts)
	if driverErr != nil {
		return this.reportError(NewWrappedError("MongoError", driverErr))
	}
	defer func() {
		_ = cursor.Close(this.ctx)
	}()

	if driverErr := cursor.All(this.ctx, dest); driverErr != nil {
		return this.reportError(NewWrappedError("MongoError", driverErr))
	}
	return nil
}

func isNumericKind(k reflect.Kind) bool {
	return k >= reflect.Int && k <= reflect.Float64
}

func fitsNumericRange(val reflect.Value, targetType reflect.Type) bool {
	srcKind := val.Kind()
	dstKind := targetType.Kind()

	switch {
	case srcKind >= reflect.Int && srcKind <= reflect.Int64:
		v := val.Int()
		switch dstKind {
		case reflect.Int8:
			return v >= math.MinInt8 && v <= math.MaxInt8
		case reflect.Int16:
			return v >= math.MinInt16 && v <= math.MaxInt16
		case reflect.Int32:
			return v >= math.MinInt32 && v <= math.MaxInt32
		case reflect.Int:
			return v >= math.MinInt && v <= math.MaxInt
		case reflect.Int64, reflect.Float64:
			return true
		case reflect.Uint8:
			return v >= 0 && v <= math.MaxUint8
		case reflect.Uint16:
			return v >= 0 && v <= math.MaxUint16
		case reflect.Uint32:
			return v >= 0 && v <= math.MaxUint32
		case reflect.Uint:
			return v >= 0 && uint64(v) <= math.MaxUint
		case reflect.Uint64:
			return v >= 0
		case reflect.Float32:
			f := float64(v)
			return f >= -math.MaxFloat32 && f <= math.MaxFloat32
		default:
			return false
		}

	case srcKind >= reflect.Uint && srcKind <= reflect.Uint64:
		u := val.Uint()
		switch dstKind {
		case reflect.Int8:
			return u <= math.MaxInt8
		case reflect.Int16:
			return u <= math.MaxInt16
		case reflect.Int32:
			return u <= math.MaxInt32
		case reflect.Int:
			return u <= uint64(math.MaxInt)
		case reflect.Int64:
			return u <= math.MaxInt64
		case reflect.Uint8:
			return u <= math.MaxUint8
		case reflect.Uint16:
			return u <= math.MaxUint16
		case reflect.Uint32:
			return u <= math.MaxUint32
		case reflect.Uint:
			return u <= math.MaxUint
		case reflect.Uint64, reflect.Float64:
			return true
		case reflect.Float32:
			return float64(u) <= math.MaxFloat32
		default:
			return false
		}

	case srcKind == reflect.Float32 || srcKind == reflect.Float64:
		f := val.Float()
		switch dstKind {
		case reflect.Int8:
			return f >= math.MinInt8 && f <= math.MaxInt8
		case reflect.Int16:
			return f >= math.MinInt16 && f <= math.MaxInt16
		case reflect.Int32:
			return f >= math.MinInt32 && f <= math.MaxInt32
		case reflect.Int:
			upper := math.Pow(2, float64(targetType.Bits()-1))
			return f >= -upper && f < upper
		case reflect.Int64:
			return f >= math.MinInt64 && f < 9223372036854775808.0
		case reflect.Uint8:
			return f >= 0 && f <= math.MaxUint8
		case reflect.Uint16:
			return f >= 0 && f <= math.MaxUint16
		case reflect.Uint32:
			return f >= 0 && f <= math.MaxUint32
		case reflect.Uint:
			return f >= 0 && f < math.Pow(2, float64(targetType.Bits()))
		case reflect.Uint64:
			return f >= 0 && f < 18446744073709551616.0
		case reflect.Float32:
			return f >= -math.MaxFloat32 && f <= math.MaxFloat32
		case reflect.Float64:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func convertDistinctValue(val any, targetType reflect.Type) (reflect.Value, Error) {
	if val == nil {
		return reflect.Zero(targetType), nil
	}
	rawVal := reflect.ValueOf(val)
	if rawVal.Type().AssignableTo(targetType) {
		return rawVal, nil
	}
	if isNumericKind(rawVal.Kind()) && isNumericKind(targetType.Kind()) {
		if (rawVal.Kind() == reflect.Float32 || rawVal.Kind() == reflect.Float64) && targetType.Kind() <= reflect.Uint64 {
			f := rawVal.Float()
			if math.Trunc(f) != f {
				return reflect.Value{}, NewError("IllegalArgumentError", fmt.Sprintf("Fractional floating-point value %v cannot be converted to integer type %s", val, targetType.String()))
			}
		}
		if !fitsNumericRange(rawVal, targetType) {
			return reflect.Value{}, NewError("IllegalArgumentError", fmt.Sprintf("Value %v is out of range for destination type %s", val, targetType.String()))
		}
		return rawVal.Convert(targetType), nil
	}
	if rawVal.CanConvert(targetType) && (rawVal.Kind() == reflect.String) == (targetType.Kind() == reflect.String) {
		return rawVal.Convert(targetType), nil
	}

	var out reflect.Value
	if err := Try(func() {
		rawBytes := RaiseVal(bson.Marshal(bson.M{"v": val}))
		wrapperType := reflect.StructOf([]reflect.StructField{
			{
				Name: "V",
				Type: targetType,
				Tag:  `bson:"v"`,
			},
		})
		wrapperPtr := reflect.New(wrapperType)
		RaiseErr(bson.Unmarshal(rawBytes, wrapperPtr.Interface()))
		out = wrapperPtr.Elem().Field(0)
	}); err != nil {
		return reflect.Value{}, NewWrappedError("MongoError", err)
	}
	return out, nil
}

type visitedPointerKey struct {
	typ  reflect.Type
	addr uintptr
}

func copyFilter(val any) any {
	return copyFilterWithVisited(val, make(map[visitedPointerKey]any))
}

func copyFilterWithVisited(val any, visited map[visitedPointerKey]any) any {
	if val == nil {
		return nil
	}
	switch v := val.(type) {
	case bson.M:
		if v == nil {
			return bson.M(nil)
		}
		out := make(bson.M, len(v))
		for k, elem := range v {
			out[k] = copyFilterWithVisited(elem, visited)
		}
		return out
	case map[string]any:
		if v == nil {
			return map[string]any(nil)
		}
		out := make(map[string]any, len(v))
		for k, elem := range v {
			out[k] = copyFilterWithVisited(elem, visited)
		}
		return out
	case bson.D:
		if v == nil {
			return bson.D(nil)
		}
		out := make(bson.D, len(v))
		for i, elem := range v {
			out[i] = bson.E{
				Key:   elem.Key,
				Value: copyFilterWithVisited(elem.Value, visited),
			}
		}
		return out
	case []bson.E:
		if v == nil {
			return []bson.E(nil)
		}
		out := make([]bson.E, len(v))
		for i, elem := range v {
			out[i] = bson.E{
				Key:   elem.Key,
				Value: copyFilterWithVisited(elem.Value, visited),
			}
		}
		return out
	case bson.A:
		if v == nil {
			return bson.A(nil)
		}
		out := make(bson.A, len(v))
		for i, elem := range v {
			out[i] = copyFilterWithVisited(elem, visited)
		}
		return out
	case []any:
		if v == nil {
			return []any(nil)
		}
		out := make([]any, len(v))
		for i, elem := range v {
			out[i] = copyFilterWithVisited(elem, visited)
		}
		return out
	default:
		rv := reflect.ValueOf(val)
		switch rv.Kind() {
		case reflect.Slice:
			if rv.IsNil() {
				return val
			}
			n := rv.Len()
			outSlice := reflect.MakeSlice(rv.Type(), n, n)
			for i := range n {
				copiedElem := copyFilterWithVisited(rv.Index(i).Interface(), visited)
				if copiedElem != nil {
					outSlice.Index(i).Set(reflect.ValueOf(copiedElem))
				}
			}
			return outSlice.Interface()
		case reflect.Map:
			if rv.IsNil() {
				return val
			}
			outMap := reflect.MakeMapWithSize(rv.Type(), rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				copiedVal := copyFilterWithVisited(iter.Value().Interface(), visited)
				var valToSet reflect.Value
				if copiedVal != nil {
					valToSet = reflect.ValueOf(copiedVal)
				} else {
					valToSet = reflect.Zero(rv.Type().Elem())
				}
				outMap.SetMapIndex(iter.Key(), valToSet)
			}
			return outMap.Interface()
		case reflect.Pointer:
			if rv.IsNil() {
				return val
			}
			key := visitedPointerKey{
				addr: rv.Pointer(),
				typ:  rv.Type(),
			}
			if existing, ok := visited[key]; ok {
				return existing
			}
			newPtr := reflect.New(rv.Type().Elem())
			visited[key] = newPtr.Interface()
			copiedElem := copyFilterWithVisited(rv.Elem().Interface(), visited)
			if copiedElem != nil {
				newPtr.Elem().Set(reflect.ValueOf(copiedElem))
			}
			return newPtr.Interface()
		default:
			if bin, ok := val.(primitive.Binary); ok {
				copiedData := make([]byte, len(bin.Data))
				copy(copiedData, bin.Data)
				return primitive.Binary{
					Subtype: bin.Subtype,
					Data:    copiedData,
				}
			}
			return val
		}
	}
}

// Where Mode
type whereModeEnum struct {
	AND,
	OR whereMode
}

type whereMode = Enum[whereModeEnum]

var whereModes = whereMode{}.New()

// Where Operator
type operatorEnum struct {
	IS, EQUALS, NOT,
	GREATERTHAN, AFTER,
	LESSTHAN, BEFORE,
	GREATERTHANOREQUAL, LESSTHANOREQUAL,
	ISNULL, NULL,
	ISNOTNULL, NOTNULL,
	BETWEEN,
	LIKE, NOTLIKE,
	REGEX, MATCHES, NOTREGEX,
	CONTAINING, NOTCONTAINING,
	STARTINGWITH, ENDINGWITH,
	IN, NOTIN,
	TRUE, ISTRUE,
	FALSE, ISFALSE operator
}

type operator = Enum[operatorEnum]

var operators = operator{}.New()

// Order sort
type orderSortingEnum struct {
	ASC,
	DESC orderSorting
}

type orderSorting = Enum[orderSortingEnum]

var orderSortings = orderSorting{}.New()

type queryField struct {
	fieldType reflect.Type
	camelName string
	bsonPath  string
}

type whereQuery struct {
	field    queryField
	mode     whereMode
	operator operator
}

func (this *whereQuery) NumArgs() int {
	switch this.operator {
	case operators.ISNULL, operators.ISNOTNULL, operators.NULL, operators.NOTNULL,
		operators.TRUE, operators.ISTRUE, operators.FALSE, operators.ISFALSE:
		return 0
	case operators.BETWEEN:
		return 2
	default:
		return 1
	}
}

func (this *whereQuery) OperatorName() string {
	return this.operator.String()
}

func (this *whereQuery) Filter(args ...any) bson.M {
	field := this.field.bsonPath
	switch this.operator {
	case operators.NOT:
		return bson.M{field: bson.M{"$ne": args[0]}}
	case operators.GREATERTHAN, operators.AFTER:
		return bson.M{field: bson.M{"$gt": args[0]}}
	case operators.GREATERTHANOREQUAL:
		return bson.M{field: bson.M{"$gte": args[0]}}
	case operators.LESSTHAN, operators.BEFORE:
		return bson.M{field: bson.M{"$lt": args[0]}}
	case operators.LESSTHANOREQUAL:
		return bson.M{field: bson.M{"$lte": args[0]}}
	case operators.ISNULL, operators.NULL:
		return bson.M{field: nil}
	case operators.ISNOTNULL, operators.NOTNULL:
		return bson.M{field: bson.M{"$ne": nil}}
	case operators.BETWEEN:
		return bson.M{field: bson.M{"$gte": args[0], "$lte": args[1]}}
	case operators.LIKE:
		pattern, _ := args[0].(string)
		return bson.M{field: primitive.Regex{Pattern: likeToRegex(pattern)}}
	case operators.NOTLIKE:
		pattern, _ := args[0].(string)
		return bson.M{field: bson.M{bsonOpNot: primitive.Regex{Pattern: likeToRegex(pattern)}}}
	case operators.REGEX, operators.MATCHES:
		pattern, _ := args[0].(string)
		return bson.M{field: primitive.Regex{Pattern: pattern}}
	case operators.NOTREGEX:
		pattern, _ := args[0].(string)
		return bson.M{field: bson.M{bsonOpNot: primitive.Regex{Pattern: pattern}}}
	case operators.CONTAINING:
		substr, _ := args[0].(string)
		return bson.M{field: primitive.Regex{Pattern: regexp.QuoteMeta(substr)}}
	case operators.NOTCONTAINING:
		substr, _ := args[0].(string)
		return bson.M{field: bson.M{bsonOpNot: primitive.Regex{Pattern: regexp.QuoteMeta(substr)}}}
	case operators.STARTINGWITH:
		prefix, _ := args[0].(string)
		return bson.M{field: primitive.Regex{Pattern: "^" + regexp.QuoteMeta(prefix)}}
	case operators.ENDINGWITH:
		suffix, _ := args[0].(string)
		return bson.M{field: primitive.Regex{Pattern: regexp.QuoteMeta(suffix) + "$"}}
	case operators.IN:
		return bson.M{field: bson.M{"$in": toBsonArray(args[0])}}
	case operators.NOTIN:
		return bson.M{field: bson.M{"$nin": toBsonArray(args[0])}}
	case operators.TRUE, operators.ISTRUE:
		return bson.M{field: true}
	case operators.FALSE, operators.ISFALSE:
		return bson.M{field: false}
	default:
		// Handles operators.IS and operators.EQUALS
		return bson.M{field: args[0]}
	}
}

func toBsonArray(val any) bson.A {
	if val == nil {
		return bson.A{}
	}
	rv := reflect.ValueOf(val)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return bson.A{}
		}
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		arr := make(bson.A, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			arr[i] = rv.Index(i).Interface()
		}
		return arr
	}
	return bson.A{val}
}

func likeToRegex(pattern string) string {
	var b strings.Builder
	b.WriteByte('^')
	escaped := false
	for _, r := range pattern {
		if escaped {
			b.WriteString(regexp.QuoteMeta(string(r)))
			escaped = false
			continue
		}
		switch r {
		case '\\':
			escaped = true
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteByte('.')
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	if escaped {
		b.WriteString(regexp.QuoteMeta(`\`))
	}
	b.WriteByte('$')
	return b.String()
}

type orderByQuery struct {
	field queryField
	order orderSorting
}

func (this *orderByQuery) Query() MongoQueryFunc {
	desc := this.order.Compare(orderSortings.DESC) == 0
	field := this.field.bsonPath
	return func(q *MongoQuery) *MongoQuery {
		if desc {
			return q.OrderByDesc(field)
		}
		return q.OrderByAsc(field)
	}
}
