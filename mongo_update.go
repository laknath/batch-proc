package mongobatch

import (
	"github.com/laknath/go-mongo-batch/internal"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"reflect"
	"time"
)

// UpdateBatch returns a channel of incoming objects (mongodb documents)
// whose state will be updated to procesed using strategy defined.
func UpdateBatch(conf *Configuration, result interface{}) (chan interface{}, error) {
	elemt := internal.VerifyStructPointer(result)

	session, err := mgo.Dial(connectString(conf))
	if err != nil {
		return nil, err
	}
	// query db
	c := session.DB(conf.Database).C(conf.Collection)

	ch := make(chan interface{})
	s := reflect.MakeSlice(reflect.SliceOf(elemt), 0, 0)

	var ticker *time.Ticker
	var t <-chan time.Time
	if conf.UpdateStrategy.UseMinRecords && conf.UpdateStrategy.UseTimeInterval {
		ticker, t = internal.NewTicker(conf.UpdateStrategy.MaxInterval)
	}

	go func() {
		defer session.Close()
		minr := int(conf.UpdateStrategy.MinRecords)

		for {
			select {
			case v := <-ch: // receiving stream
				if !conf.UpdateStrategy.UseMinRecords {
					//update as they arrive
					go updateSingleRecord(conf, v, c)
				} else {
					rv := reflect.ValueOf(v)
					if rv.Type() == elemt {
						s = reflect.Append(s, rv)
					}

					if s.Len() >= minr {
						s = updateAndClear(conf, s, c)

						// reset the timer
						if conf.UpdateStrategy.UseTimeInterval {
							if ticker != nil {
								ticker.Stop()
							}
							ticker, t = internal.NewTicker(conf.UpdateStrategy.MaxInterval)
						}
					}
				}

			case <-t: // the max interval to go without updating records. nil channel if disabled.
				if s.Len() > 0 {
					s = updateAndClear(conf, s, c)
				}
			}
		}
	}()

	return ch, nil
}

// updateAndClear updates mongo records and empties the slice if update was successful.
func updateAndClear(conf *Configuration, slicev reflect.Value, c *mgo.Collection) reflect.Value {
	if err := updateRecords(conf, slicev, c); err != nil {
		log.Printf("Not updated. %v", err)
		return slicev
	}

	return reflect.MakeSlice(slicev.Type(), 0, 0)
}

// updateRecords updates mongo records to processed state.
func updateRecords(conf *Configuration, slicev reflect.Value, c *mgo.Collection) error {
	ids := fetchIds(conf, slicev)
	// update all matching documents to processing
	_, err := c.UpdateAll(
		bson.M{"_id": bson.M{"$in": ids}},
		bson.M{"$set": bson.M{conf.StateFld: conf.ProcessedState, conf.ProcessedTimeFld: time.Now().Unix()}},
	)
	return err
}

// updateSingleRecord verifies the validity of v and updates the state of the document
// to processed in mongo.
func updateSingleRecord(conf *Configuration, v interface{}, c *mgo.Collection) {
	resultv := reflect.ValueOf(v)
	if resultv.Kind() != reflect.Ptr || resultv.Elem().Kind() != reflect.Struct {
		log.Printf("Not updated. Record is not a pointer to a struct %v", v)
		return
	}
	id := resultv.Elem().FieldByName("Id")
	if id.IsValid() {
		oid := internal.ObjectIdFromString(id.String())
		c.UpdateId(oid, bson.M{"$set": bson.M{conf.StateFld: conf.ProcessedState, conf.ProcessedTimeFld: time.Now().Unix()}})
	} else {
		log.Printf("Not updated. Struct doesn't have an ID field %v", v)
	}
}
