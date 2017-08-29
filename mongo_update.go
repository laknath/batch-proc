package mongobatch

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"time"
)

// UpdateBatch returns a channel of incoming objects (mongodb documents)
// whose state will be updated to procesed using strategy defined.
func UpdateBatch(conf *Configuration, result interface{}) (chan interface{}, error) {
	elemt := verifyStructPointer(result)

	session, err := mgo.Dial(connectString(conf))
	if err != nil {
		return nil, err
	}
	defer session.Close()
	// query db
	c := session.DB(conf.Database).C(conf.Collection)

	ch := make(chan interface{})
	s := reflect.MakeSlice(reflect.SliceOf(elemt), 0, 0)
	var t <-chan time.Time

	if conf.UpdateStrategy.UseTimeInterval {
		t = time.Tick(time.Duration(conf.UpdateStrategy.MaxInterval) * time.Millisecond)
	}

	go func() {
		for {
			select {
			case v := <-ch: // receiving stream
				reflect.Append(s, reflect.ValueOf(v))
			case _ = <-t: // the max interval to go without updating records. nil channel if disabled.
				updateRecords(conf, s.Addr(), c)
			}
		}
	}()

	return ch, nil
}

func updateRecords(conf *Configuration, results interface{}, c *mgo.Collection) error {
	ids := fetchIds(conf, results)
	// update all matching documents to processing
	_, err := c.UpdateAll(bson.M{"_id": bson.M{"$in": ids}}, bson.M{"$set": bson.M{conf.StateFld: conf.ProcessedState}})
	return err
}

// verifySlice makes sure results parameter is a slice of Structs.
func verifySlice(results interface{}) reflect.Value {
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
func verifyStructPointer(result interface{}) reflect.Type {
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
