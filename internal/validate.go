package internal

import "reflect"

// verifySlice makes sure results parameter is a slice of Structs.
func VerifySlice(results interface{}) reflect.Value {
	resultv := reflect.ValueOf(results)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Slice {
		panic("result argument must be a slice address")
	}
	slicev := resultv.Elem()
	elemt := slicev.Type().Elem()
	if elemt.Kind() != reflect.Struct {
		panic("result slice's type should be struct")
	}
	verifyStruct(elemt)

	return slicev
}

// verifyStruct makes sure result parameter is a valid Struct pointer.
// It returns the pointer type of the given struct.
func VerifyStructPointer(result interface{}) reflect.Type {
	resultv := reflect.ValueOf(result)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Struct {
		panic("Result is not a pointer to a struct")
	}
	verifyStruct(resultv.Elem().Type())

	return resultv.Type()
}

// verifyStruct verifies whether passed elemt has necessary fields.
func verifyStruct(elemt reflect.Type) {
	fld, ok := elemt.FieldByName("Id")
	if !ok {
		panic("result slice's elements should have an ID field")
	}
	if fld.Type.String() != "bson.ObjectId" {
		panic("ID field should be of type bson.ObjectId")
	}
}
