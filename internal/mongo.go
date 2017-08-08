package internal

import (
	"fmt"
	"gopkg.in/mgo.v2/bson"
)

type Configuration struct {
	Host        string
	Port        uint
	Database    string
	Collection  string
	StateFld    string
	FetchOrder  string
	FetchQuery  bson.M
	UpdateQuery bson.M
	ResetQuery  bson.M
}

// NewConfiguration creates a new Configuration object with default values.
func NewConfiguration(host string, port uint, db string, col string, s string, ord string) Configuration {
	conf := Configuration{
		Host:       host,
		Port:       port,
		Database:   db,
		Collection: col,
		StateFld:   s,
		FetchOrder: ord,
	}

	// set the default field for state
	if len(s) == 0 {
		conf.StateFld = "state"
	}

	// set the default FetchOrder field to "CreatedAt", oldest first
	if len(ord) == 0 {
		conf.FetchOrder = "CreatedAt"
	}

	conf.FetchQuery = bson.M{conf.StateFld: bson.M{"$ne": "processed"}}
	conf.UpdateQuery = bson.M{conf.StateFld: "processed"}
	conf.ResetQuery = bson.M{conf.StateFld: "reattempt"}

	return conf
}

// ConnectString creates a Mongo Connection string.
func ConnectString(config *Configuration) string {
	return fmt.Sprintf("%s:%d", config.Host, config.Port)
}
