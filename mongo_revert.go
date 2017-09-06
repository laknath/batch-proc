package mongobatch

import (
	"github.com/laknath/go-mongo-batch/internal"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

// BatchTimeout creates a long running go routine that will call
// revertExpired in every Configuration.CronInterval seconds.

// It returns a close channel that can be used to cancel the process.
func BatchTimeout(conf *Configuration) <-chan bool {
	quit := make(chan bool)
	ticker, t := internal.NewTicker(conf.CronInterval * 1000)

	go func() {
		defer ticker.Stop()

		select {
		case <-t:
			revertExpired(conf)
		case <-quit:
			break
		}
	}()

	return quit
}

// revertExpired Updates records in processing state with a time
// interval larger than Configuration.VisibilityTimeout to initial
// state.
func revertExpired(conf *Configuration) error {
	session, err := mgo.Dial(connectString(conf))
	if err != nil {
		return err
	}
	timeout := time.Now().Unix() - int64(conf.VisibilityTimeout)
	// query db
	c := session.DB(conf.Database).C(conf.Collection)
	_, err = c.UpdateAll(
		bson.M{conf.StateFld: conf.ProcessingState, conf.ProcessingTimeFld: bson.M{"$lt": timeout}},
		bson.M{
			"$set": bson.M{conf.StateFld: nil, conf.RevertedTimeFld: time.Now().Unix()},
			"$inc": bson.M{"reattempts": 1},
		},
	)

	return err
}
