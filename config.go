package mongobatch

import (
	"fmt"
	"gopkg.in/mgo.v2/bson"
)

const (
	DefaultStateFld        = "state"
	DefaultFetchOrder      = "updatedat"
	DefaultFetchLimit      = 10
	DefaultProcessingState = "processing"
	DefaultProcessedState  = "processed"
	DefaultErrorSleep      = 1000 // 1 second
	DefaultNoRecordSleep   = 5000 // 5 seconds

	DefaultMaxInterval = 10000 // 10 seconds
	DefaultMinRecords  = 100
)

type Configuration struct {
	Host            string
	Port            uint
	Database        string
	Collection      string
	StateFld        string
	FetchOrder      string
	FetchLimit      int
	FetchQuery      bson.M
	UpdateQuery     bson.M
	ResetQuery      bson.M
	ProcessingState string
	ProcessedState  string
	ErrorSleep      uint
	NoRecordSleep   uint
	UpdateStrategy  UpdateStrategy
}

type UpdateStrategy struct {
	UseTimeInterval bool
	UseMinRecords   bool
	MaxInterval     uint
	MinRecords      uint
}

// NewConfiguration creates a new Configuration object with default values.
func NewConfiguration(host string, port uint, db string, col string) *Configuration {
	conf := &Configuration{
		Host:            host,
		Port:            port,
		Database:        db,
		Collection:      col,
		StateFld:        DefaultStateFld,
		FetchOrder:      DefaultFetchOrder,
		FetchLimit:      DefaultFetchLimit,
		ProcessingState: DefaultProcessingState,
		ProcessedState:  DefaultProcessedState,
		ErrorSleep:      DefaultErrorSleep,
		NoRecordSleep:   DefaultNoRecordSleep,

		UpdateStrategy: UpdateStrategy{
			UseTimeInterval: true,
			UseMinRecords:   true,
			MaxInterval:     DefaultMaxInterval,
			MinRecords:      DefaultMinRecords,
		},
	}

	conf.FetchQuery = bson.M{
		conf.StateFld: bson.M{
			"$nin": []interface{}{conf.ProcessingState, conf.ProcessedState},
		},
	}
	conf.UpdateQuery = bson.M{conf.StateFld: conf.ProcessedState}
	//conf.ResetQuery = bson.M{conf.StateFld: "reattempt"}

	return conf
}

// ConnectString creates a Mongo Connection string.
func connectString(config *Configuration) string {
	return fmt.Sprintf("%s:%d", config.Host, config.Port)
}
