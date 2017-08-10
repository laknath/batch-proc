package mongobatch

import (
	"github.com/laknath/go-mongo-batch/internal"
	"gopkg.in/mgo.v2"
)

func FetchInput(conf *internal.Configuration, result interface{}) (interface{}, error) {
	//TODO
	//use a distributed lock for atomicity
	session, err := mgo.Dial(internal.ConnectString(conf))
	if err != nil {
		return nil, err
	}
	defer session.Close()
	// query db
	c := session.DB(conf.Database).C(conf.Collection)
	iter := c.Find(conf.FetchQuery).Limit(conf.FetchLimit).Sort(conf.FetchOrder).Iter()

	err = iter.All(&result)

	return result, err
}
