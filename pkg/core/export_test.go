package core

import (
	"reflect"

	"go.mongodb.org/mongo-driver/bson"

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

// ExportConvertDistinctValue exposes convertDistinctValue for package core_test.
func ExportConvertDistinctValue(val any, targetType reflect.Type) (reflect.Value, Error) {
	return convertDistinctValue(val, targetType)
}

// ExportFitsNumericRange exposes fitsNumericRange for package core_test.
func ExportFitsNumericRange(val reflect.Value, targetType reflect.Type) bool {
	return fitsNumericRange(val, targetType)
}

// ExportEntityIDStatus exposes entityIDStatus for testing.
func ExportEntityIDStatus(object any) (reflect.Value, any, bool) {
	return entityIDStatus(object)
}

// ExportAssignGeneratedID exposes assignGeneratedID for testing.
func ExportAssignGeneratedID(idField reflect.Value) bool {
	return assignGeneratedID(idField)
}
