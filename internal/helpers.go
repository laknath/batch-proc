package internal

import (
	"encoding/hex"
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"time"
)

// ObjectIdFromString returns a bson.ObjectId from a given
// string representation of the ObjectId.
func ObjectIdFromString(s string) bson.ObjectId {
	return bson.ObjectIdHex(hex.EncodeToString([]byte(s)))
}

// NewPointerToSlice returns a new pointer to an empty Slice.
func NewPointerToSlice(elemt reflect.Type) reflect.Value {
	slicev := reflect.MakeSlice(reflect.SliceOf(elemt), 0, 0)
	slicep := reflect.New(slicev.Type())
	slicep.Elem().Set(slicev)
	return slicep
}

// newTicker returns a ticker and its time channel using Milliseconds as the unit
// of duration
func NewTicker(millisecs uint) (*time.Ticker, <-chan time.Time) {
	ticker := time.NewTicker(time.Duration(millisecs) * time.Millisecond)
	return ticker, ticker.C
}
