package mongobatch

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

// UpdateBatch returns a channel of incoming objects (mongodb documents)
// whose state will be updated to procesed using strategy defined.
func UpdateBatch(conf *Configuration) (<-chan interface{}, error) {
	session, err := mgo.Dial(connectString(conf))
	if err != nil {
		return nil, err
	}
	defer session.Close()
	// query db
	c := session.DB(conf.Database).C(conf.Collection)

	ch := make(<-chan interface{})
	s := make([]interface{}, conf.UpdateStrategy.MinRecords)
	var t <-chan time.Time

	if conf.UpdateStrategy.UseTimeInterval {
		t = time.Tick(time.Duration(conf.UpdateStrategy.MaxInterval) * time.Millisecond)
	}

	go func() {
		for {
			select {
			case v := <-ch: // receiving stream
				s = append(s, v)
				fmt.Println(v)
			case _ = <-t: // the max interval to go without updating records
				updateRecords(conf, s, c)
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
