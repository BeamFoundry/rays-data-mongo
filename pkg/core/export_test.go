package core

import (
	"context"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	. "github.com/BeamFoundry/rays/pkg/lang"
)

// ExportToBsonArray exposes toBsonArray for package core_test.
func ExportToBsonArray(val any) bson.A {
	return toBsonArray(val)
}

// ExportCopyFilter exposes copyFilter for package core_test.
func ExportCopyFilter(val any) any {
	return copyFilter(val)
}

// ExportExtractValidator exposes extractValidator for package core_test.
func ExportExtractValidator(ctx context.Context) interface{ Validate() Error } {
	return extractValidator(ctx)
}

// ExportConvertDistinctValue exposes convertDistinctValue for package core_test.
func ExportConvertDistinctValue(val any, targetType reflect.Type) (reflect.Value, Error) {
	return convertDistinctValue(val, targetType)
}

// ExportFitsNumericRange exposes fitsNumericRange for package core_test.
func ExportFitsNumericRange(val reflect.Value, targetType reflect.Type) bool {
	return fitsNumericRange(val, targetType)
}

// ExportEntityIDStatus exposes entityIDStatus for testing.
func ExportEntityIDStatus(object any) (val any, hasEncoded bool, err error) {
	st, encodeErr := entityIDStatus(nil, object)
	if encodeErr != nil {
		return nil, false, encodeErr
	}
	return st.val, st.hasEncoded, nil
}

// ExportAssignGeneratedID exposes assignGeneratedID for testing.
func ExportAssignGeneratedID(idField reflect.Value) bool {
	return assignGeneratedID(idField)
}

// ExportMarshalWithCollection exposes marshalWithCollection for testing.
func ExportMarshalWithCollection(coll *mongo.Collection, val any) ([]byte, error) {
	return marshalWithCollection(coll, val)
}

// ExportParseBSONTag exposes parseBSONTag for testing.
func ExportParseBSONTag(field *reflect.StructField, useJSONTags ...bool) (name string, inline, skip bool) {
	return parseBSONTag(field, useJSONTags...)
}

// ExportCollectionUseJSONStructTags exposes collectionUseJSONStructTags for testing.
func ExportCollectionUseJSONStructTags(coll *mongo.Collection) bool {
	return collectionUseJSONStructTags(coll)
}

// ExportFindIDField exposes findIDField for testing.
func ExportFindIDField(v reflect.Value, useJSONTags ...bool) reflect.Value {
	return findIDField(v, useJSONTags...)
}

// ExportFindIDPath exposes findIDPath for testing.
func ExportFindIDPath(t reflect.Type, useJSON bool) []int {
	return findIDPath(t, useJSON)
}

// ExportExtractObjectID exposes extractObjectID for testing.
func ExportExtractObjectID(idField reflect.Value) (primitive.ObjectID, bool) {
	return extractObjectID(idField)
}

// ExportUndoAllocations exposes undoAllocations for testing.
func ExportUndoAllocations(allocated []reflect.Value) {
	undoAllocations(allocated)
}

// ExportTerminalIDType exposes terminalIDType for testing.
func ExportTerminalIDType(t reflect.Type, path []int) reflect.Type {
	return terminalIDType(t, path)
}

// ExportSupportsGeneratedID exposes supportsGeneratedID for testing.
func ExportSupportsGeneratedID(t reflect.Type) bool {
	return supportsGeneratedID(t)
}
