package internal

import (
	"encoding/hex"
	"gopkg.in/mgo.v2/bson"
)

func ObjectIdFromString(s string) bson.ObjectId {
	return bson.ObjectIdHex(hex.EncodeToString([]byte(s)))
}
