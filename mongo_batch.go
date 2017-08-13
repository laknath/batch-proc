package mongobatch

import (
	"gopkg.in/mgo.v2"
)

// FetchInput fetches defined lengths of batches from a Mongo collection.
// The retrieved batch will be marked processing.If a record is not marked
// as "processed" within a given time interval,the record will be reverted
// back to the initial non-processed state.This workflow supports concurrent processing.

// The conf argument is a Configuration object initialized with values necessary for
// the mongo connection.

// The result argument must be the address for a slice. It will hold the resulting result set.

// For instance:
//
//    var result []struct{ Value int }
//	  config := NewConfiguration("localhost", 27017, "salaries", "batch")
//    err := FetchInput(config, &results)
//    if err != nil {
//        return err
//    }
//
func FetchInput(conf *Configuration, result interface{}) error {
	//TODO
	//use a distributed lock for mutual exclusion
	session, err := mgo.Dial(connectString(conf))
	if err != nil {
		return err
	}
	defer session.Close()
	// query db
	c := session.DB(conf.Database).C(conf.Collection)
	iter := c.Find(conf.FetchQuery).Limit(conf.FetchLimit).Sort(conf.FetchOrder).Iter()

	err = iter.All(result)

	return err
}
