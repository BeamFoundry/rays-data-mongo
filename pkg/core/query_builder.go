package core

import (
	"encoding"
	"reflect"
	"slices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	. "github.com/BeamFoundry/rays-data-mongo/pkg/tx"
	. "github.com/BeamFoundry/rays/pkg/core"
	. "github.com/BeamFoundry/rays/pkg/lang"
	. "github.com/BeamFoundry/rays/pkg/util"
	. "github.com/BeamFoundry/rays/pkg/util/stream"
)

type queryKind int

const (
	queryKindFind queryKind = iota
	queryKindCount
	queryKindExists
	queryKindDelete
)

var (
	objectIDType        = reflect.TypeFor[primitive.ObjectID]()
	timeType            = reflect.TypeFor[time.Time]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
	bsonMarshalerType   = reflect.TypeFor[bson.Marshaler]()
)

type mongoQueryBuilder[T any, ID comparable] struct {
	logger    Logging
	fields    []queryField
	operators []string
}

func (this mongoQueryBuilder[T, ID]) New(logger Logging) *mongoQueryBuilder[T, ID] {
	this.logger = logger
	entity := reflect.TypeFor[T]()
	this.fields = collectQueryFields(entity, "", "", map[reflect.Type]bool{})

	// Sort longest field names first so nested/longer names (e.g. AddressCity, UserName)
	// match before prefixes (e.g. Address, User).
	slices.SortFunc(this.fields, func(a, b queryField) int {
		if len(a.camelName) != len(b.camelName) {
			return len(b.camelName) - len(a.camelName)
		}
		return strings.Compare(b.camelName, a.camelName)
	})

	ops := []string{
		"GreaterThanOrEqual", "LessThanOrEqual",
		"NotContaining", "StartingWith", "EndingWith",
		"Containing", "IsNotNull", "NotNull",
		"GreaterThan", "LessThan",
		"NotLike", "NotRegex", "Between",
		"Equals", "IsNull", "NotIn",
		"IsTrue", "IsFalse",
		"Matches", "Regex",
		"After", "Before", "Null", "Like",
		"True", "False",
		"Not", "In", "Is",
	}
	slices.SortFunc(ops, func(a, b string) int {
		if len(a) != len(b) {
			return len(b) - len(a)
		}
		return strings.Compare(b, a)
	})
	this.operators = ops
	return &this
}

func collectQueryFields(t reflect.Type, camelPrefix, bsonPrefix string, visited map[reflect.Type]bool) []queryField {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	if visited[t] {
		return nil
	}
	visited[t] = true
	defer delete(visited, t)

	var fields []queryField
	for field := range t.Fields() {
		if !field.IsExported() {
			continue
		}
		bsonName, inline, skip := parseBSONTag(&field)
		if skip {
			continue
		}

		if inline {
			sub := collectQueryFields(field.Type, camelPrefix, bsonPrefix, visited)
			fields = append(fields, sub...)
			continue
		}

		camel := camelPrefix + field.Name
		path := bsonPrefix + bsonName
		fields = append(fields, queryField{
			camelName: camel,
			bsonPath:  path,
			fieldType: field.Type,
		})

		if isNestedDocumentStruct(field.Type) {
			sub := collectQueryFields(field.Type, camel, path+".", visited)
			fields = append(fields, sub...)
		}
	}
	return fields
}

func parseBSONTag(field *reflect.StructField) (name string, inline, skip bool) {
	tag, ok := field.Tag.Lookup("bson")
	if !ok {
		return strings.ToLower(field.Name), false, false
	}
	if tag == "-" {
		return "", false, true
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	for _, opt := range parts[1:] {
		if opt == "inline" {
			inline = true
		}
	}
	if name == "" && !inline {
		name = strings.ToLower(field.Name)
	}
	return name, inline, false
}

func isNestedDocumentStruct(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	if t == objectIDType || t == timeType {
		return false
	}
	if t.Implements(textUnmarshalerType) || reflect.PointerTo(t).Implements(textUnmarshalerType) {
		return false
	}
	if t.Implements(bsonMarshalerType) || reflect.PointerTo(t).Implements(bsonMarshalerType) {
		return false
	}
	return true
}

func (this *mongoQueryBuilder[T, ID]) canParseAsOrder(str string) bool {
	s := str
	for s != "" {
		order, rest := this.parseOrderByQuery(s)
		if order == nil {
			return false
		}
		s = rest
	}
	return true
}

func (this *mongoQueryBuilder[T, ID]) parseOrderByQuery(s string) (query *orderByQuery, rest string) {
	for _, field := range this.fields {
		if !String(s).StartsWith(field.camelName) {
			continue
		}
		afterField := s[len(field.camelName):]
		if String(afterField).StartsWith("Asc") {
			rem := afterField[3:]
			if rem == "" || this.canParseAsOrder(rem) {
				return &orderByQuery{field: field, order: orderSortings.ASC}, rem
			}
		} else if String(afterField).StartsWith("Desc") {
			rem := afterField[4:]
			if rem == "" || this.canParseAsOrder(rem) {
				return &orderByQuery{field: field, order: orderSortings.DESC}, rem
			}
		}
		if afterField == "" || this.canParseAsOrder(afterField) {
			return &orderByQuery{field: field, order: orderSortings.ASC}, afterField
		}
	}
	return nil, ""
}

func (this *mongoQueryBuilder[T, ID]) parseWhereQuery(s string) (*whereQuery, string, Error) {
	w := &whereQuery{
		mode:     whereModes.AND,
		operator: operators.IS,
	}
	switch {
	case String(s).StartsWith("By"):
		s = s[2:]
	case String(s).StartsWith("Or"):
		s = s[2:]
		w.mode = whereModes.OR
	case String(s).StartsWith("And"):
		s = s[3:]
	default:
		return nil, "", NewError("IllegalArgumentError", "Condition segment must start with 'By', 'And', or 'Or'")
	}

	hadFieldMatch := false
	for _, field := range this.fields {
		if !String(s).StartsWith(field.camelName) {
			continue
		}
		hadFieldMatch = true
		rest := s[len(field.camelName):]
		w.field = field
		if rest == "" || String(rest).StartsWith("And") || String(rest).StartsWith("Or") {
			return w, rest, nil
		}
		for _, opStr := range this.operators {
			if String(rest).StartsWith(opStr) {
				rest = rest[len(opStr):]
				var op operator
				if err := (&op).From(String(opStr).ToUpperCase().String()); err == nil {
					w.operator = op
				}
				return w, rest, nil
			}
		}
		// Suffix did not form a valid condition for this field; continue trying shorter field matches.
	}
	if hadFieldMatch {
		return nil, "", NewError("IllegalArgumentError", "Invalid operator")
	}
	return nil, "", NewError("IllegalArgumentError", "Could not match a valid T field for condition")
}

func (this *mongoQueryBuilder[T, ID]) Build(function *reflect.StructField) (*mongoCompiledQuery[T, ID], Error) {
	name := CaseUtils(function.Name).Capitalize().String()

	// 1. Verify Prefix
	var kind queryKind
	switch {
	case String(name).StartsWith("Find"), String(name).StartsWith("Query"),
		String(name).StartsWith("List"), String(name).StartsWith("Stream"):
		kind = queryKindFind
	case String(name).StartsWith("Count"):
		kind = queryKindCount
	case String(name).StartsWith("Exists"):
		kind = queryKindExists
	case String(name).StartsWith("Delete"):
		kind = queryKindDelete
	default:
		return nil, NewError("IllegalArgumentError", "Function name must start with Find, Query, List, Stream, Count, Exists, or Delete")
	}

	// 2. Find the "By" separator
	byIndex := Strings(name).Index("By")
	if byIndex == -1 {
		return nil, NewError("IllegalArgumentError", "Function name must contain 'By'")
	}

	// 3. Extract the Condition and OrderBy segments
	predicateStr := name[byIndex:]
	conditionStr := predicateStr
	orderStr := ""

	// Check if a string can be completely resolved into valid condition tokens without OrderBy
	canParseAsConditions := func(str string) bool {
		s := str
		for s != "" {
			_, rest, err := this.parseWhereQuery(s)
			if err != nil {
				return false
			}
			s = rest
		}
		return true
	}

	if !canParseAsConditions(predicateStr) {
		curr := predicateStr
		for {
			idx := Strings(curr).LastIndex("OrderBy")
			if idx == -1 {
				break
			}
			candCond := predicateStr[:idx]
			candOrder := predicateStr[idx+7:]
			if canParseAsConditions(candCond) && candOrder != "" && this.canParseAsOrder(candOrder) {
				conditionStr = candCond
				orderStr = candOrder
				break
			}
			curr = predicateStr[:idx]
		}
	}

	query := &mongoCompiledQuery[T, ID]{
		logger:   this.logger,
		function: function,
		kind:     kind,
	}

	this.logger.Debug("Analyzing function", "name", name, "predicate", predicateStr, "condition", conditionStr, "order", orderStr)

	// 4. Parse Conditions
	for conditionStr != "" {
		condition, rest, err := this.parseWhereQuery(conditionStr)
		if err != nil {
			return nil, err
		}

		conditionStr = rest
		this.logger.Debug("Added Condition", "field", condition.field.bsonPath, "operator", condition.OperatorName())
		query.conditions = append(query.conditions, condition)
	}

	// 5. Parse Orders
	for orderStr != "" {
		order, rest := this.parseOrderByQuery(orderStr)
		orderStr = rest
		this.logger.Debug("Added ordering", "field", order.field.bsonPath, "order", order.order.String())
		query.orders = append(query.orders, order)
	}

	if err := query.validate(); err != nil {
		return nil, err
	}

	return query, nil
}

type mongoCompiledQuery[T any, ID comparable] struct {
	logger     Logging
	function   *reflect.StructField
	conditions []*whereQuery
	orders     []*orderByQuery
	kind       queryKind
}

func (this *mongoCompiledQuery[T, ID]) validate() Error {
	entity := reflect.TypeFor[T]()

	var expectedTypes []reflect.Type
	for _, cond := range this.conditions {
		fieldType := cond.field.fieldType
		switch cond.operator.Name() {
		case "ISNULL", "NULL", "ISNOTNULL", "NOTNULL",
			"TRUE", "ISTRUE", "FALSE", "ISFALSE":
			// 0 arguments
		case "BETWEEN":
			expectedTypes = append(expectedTypes, fieldType, fieldType)
		case "IN", "NOTIN":
			expectedTypes = append(expectedTypes, reflect.SliceOf(fieldType))
		case "LIKE", "NOTLIKE", "REGEX", "MATCHES", "NOTREGEX",
			"CONTAINING", "NOTCONTAINING", "STARTINGWITH", "ENDINGWITH":
			expectedTypes = append(expectedTypes, reflect.TypeFor[string]())
		default:
			expectedTypes = append(expectedTypes, fieldType)
		}
	}

	numIn := this.function.Type.NumIn()
	if numIn == len(expectedTypes)+1 &&
		this.function.Type.IsVariadic() &&
		this.function.Type.In(len(expectedTypes)).Elem() == reflect.TypeFor[MongoQueryFunc]() {
		numIn--
	}

	if len(expectedTypes) != numIn {
		return NewError("IllegalArgumentError", "Argument count mismatch between function definition and parsed queries")
	}

	for i, expected := range expectedTypes {
		if this.function.Type.In(i) != expected {
			return NewError("IllegalArgumentError", "Argument type mismatch for condition parameters")
		}
	}

	numOut := this.function.Type.NumOut()
	if numOut < 1 || numOut > 2 {
		return NewError("IllegalArgumentError", "Invalid return count; must return 1 value or (value, Error)")
	}
	if numOut == 2 && this.function.Type.Out(1) != reflect.TypeFor[Error]() {
		return NewError("IllegalArgumentError", "Second return value must be Error")
	}

	retType := this.function.Type.Out(0)
	switch this.kind {
	case queryKindCount:
		if retType != reflect.TypeFor[int64]() {
			return NewError("IllegalArgumentError", "Count query must return int64 or (int64, Error)")
		}
	case queryKindExists:
		if retType != reflect.TypeFor[bool]() {
			return NewError("IllegalArgumentError", "Exists query must return bool or (bool, Error)")
		}
	case queryKindDelete:
		validErrorOnly := numOut == 1 && retType == reflect.TypeFor[Error]()
		if !validErrorOnly && retType != reflect.TypeFor[int64]() {
			return NewError("IllegalArgumentError", "Delete query must return Error, int64, or (int64, Error)")
		}
	default:
		ptrTType := reflect.PointerTo(entity)
		sliceTType := reflect.SliceOf(entity)
		optionalTType := reflect.TypeFor[Optional[*T]]()
		queryType := reflect.TypeFor[*MongoQuery]()
		streamTType := reflect.TypeFor[Stream[T]]()

		if retType != ptrTType && retType != sliceTType && retType != optionalTType && retType != queryType && retType != streamTType {
			return NewError("IllegalArgumentError", "Invalid return type; must be *T, []T, Optional[*T], Stream[T], or *MongoQuery")
		}
	}

	this.logger.Info("Created repository function", "entity", entity, "function", this.function.Name)
	return nil
}

func (this *mongoCompiledQuery[T, ID]) buildFilter(in []reflect.Value) (filter any, consumed int) {
	var orGroups []bson.A
	var currentAnd bson.A

	for _, condition := range this.conditions {
		args := make([]any, 0, condition.NumArgs())
		for i := consumed; i < consumed+condition.NumArgs(); i++ {
			args = append(args, in[i].Interface())
		}
		this.logger.Trace("Adding condition", "field", condition.field.bsonPath, "args", args)
		f := condition.Filter(args...)
		consumed += condition.NumArgs()

		if condition.mode == whereModes.OR && len(currentAnd) > 0 {
			orGroups = append(orGroups, currentAnd)
			currentAnd = bson.A{f}
		} else {
			currentAnd = append(currentAnd, f)
		}
	}
	if len(currentAnd) > 0 {
		orGroups = append(orGroups, currentAnd)
	}

	if len(orGroups) == 1 {
		return collapseAnd(orGroups[0]), consumed
	}

	orSlice := make(bson.A, 0, len(orGroups))
	for _, grp := range orGroups {
		orSlice = append(orSlice, collapseAnd(grp))
	}
	return bson.M{bsonOpOr: orSlice}, consumed
}

func collapseAnd(group bson.A) any {
	if len(group) == 1 {
		return group[0]
	}
	return bson.M{bsonOpAnd: group}
}

func (this *mongoCompiledQuery[T, ID]) Query(
	db *mongo.Database,
	tx MongoTransactional,
	collectionName string,
) func(in []reflect.Value) []reflect.Value {
	output := this.function.Type.Out(0)
	multi := output.Kind() == reflect.Slice
	optional := output == reflect.TypeFor[Optional[*T]]()
	stream := output == reflect.TypeFor[Stream[T]]()
	errorOnly := this.kind == queryKindDelete && output == reflect.TypeFor[Error]()

	hasError := this.function.Type.NumOut() > 1 && this.function.Type.Out(1) == reflect.TypeFor[Error]()
	formatResult := func(val reflect.Value, err Error) []reflect.Value {
		ret := []reflect.Value{val}
		if hasError {
			if err != nil {
				ret = append(ret, reflect.ValueOf(err))
			} else {
				ret = append(ret, reflect.Zero(reflect.TypeFor[Error]()))
			}
		}
		return ret
	}

	return func(in []reflect.Value) []reflect.Value {
		q := NewMongoQuery(tx.Get(), db, collectionName)

		filter, index := this.buildFilter(in)
		if filter != nil {
			q = q.Where(filter)
		}

		for _, order := range this.orders {
			q = q.Scopes(order.Query())
		}

		if this.function.Type.NumIn() == index+1 && this.function.Type.IsVariadic() {
			if this.function.Type.In(index).Elem() == reflect.TypeFor[MongoQueryFunc]() {
				if customFuncs, ok := in[index].Interface().([]MongoQueryFunc); ok {
					this.logger.Trace("Function is Variadic", "function", this.function.Type)
					q = q.Scopes(customFuncs...)
				}
			}
		}

		if output == reflect.TypeFor[*MongoQuery]() {
			return formatResult(reflect.ValueOf(q), q.Error())
		}

		switch this.kind {
		case queryKindCount:
			count, err := q.Count()
			return formatResult(reflect.ValueOf(count), err)
		case queryKindExists:
			exists, err := q.Exists()
			return formatResult(reflect.ValueOf(exists), err)
		case queryKindDelete:
			deleted, err := q.Delete()
			if errorOnly {
				if err != nil {
					return []reflect.Value{reflect.ValueOf(err)}
				}
				return []reflect.Value{reflect.Zero(reflect.TypeFor[Error]())}
			}
			return formatResult(reflect.ValueOf(deleted), err)
		default:
			val, err := this.executeFind(q, multi, stream, optional)
			return formatResult(val, err)
		}
	}
}

func (this *mongoCompiledQuery[T, ID]) executeFind(
	q *MongoQuery,
	multi, stream, optional bool,
) (reflect.Value, Error) {
	switch {
	case multi:
		var t []T
		err := q.Find(&t)
		return reflect.ValueOf(t), err
	case stream:
		// Stream query methods buffer all matching results via q.Find before
		// constructing the Stream via Stream[T]{}.FromSlice(t).
		var t []T
		err := q.Find(&t)
		s := Stream[T]{}.FromSlice(t)
		return reflect.ValueOf(s), err
	default:
		var t T
		err := q.First(&t)
		switch {
		case optional:
			var opt Optional[*T]
			if err == nil {
				(&opt).SetValue(&t)
			}
			return reflect.ValueOf(opt), err
		case err == nil:
			return reflect.ValueOf(&t), nil
		default:
			return reflect.Zero(reflect.TypeFor[*T]()), err
		}
	}
}
