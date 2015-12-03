package store

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
	"github.com/garyburd/redigo/redis"
)

// Storer is a interface for a interacting with a database for storing keys, values,
// and expiration times
type Storer interface {
	Store(key string, value interface{}, time int) error // stores a cookie key and value, and expiration time
	Retrieve(key string, value interface{}) error        // retrieves a value by key, if it has not expired yet
}

type storer struct {
	pool *redis.Pool
}

func (s *storer) Store(key string, value interface{}, time int) error {
	conn := s.pool.Get()
	defer conn.Close()
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	conn.Do("SETEX", key, time, data)
	return nil
}

func (s *storer) Retrieve(key string, value interface{}) error {
	conn := s.pool.Get()
	defer conn.Close()
	data, err := conn.Do("GET", key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data.([]byte), value)
}

func newPool(server string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func New(address string) Storer {
	if address == "" {
		db, err := bolt.Open("my.db", 0600, nil)
		if err != nil {
			log.Fatal(err)
		}
		return &embedDB{db: db}
	} else {
		return &storer{newPool(address)}
	}
}

// a wrapper around the boldDB database type
type embedDB struct {
	db *bolt.DB
}

// newEmbedDB creates an embeded database into which cookies, keys, and expiration datas will
// be stored
func newEmbedDB() embedDB {
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	return embedDB{db: db}
}

func (db *embedDB) Store(key string, value interface{}, ttd int) error {
	// update the "data" bucket
	myDB := db.db
	var err error
	err = myDB.Update(
		func(tx *bolt.Tx) error {
			dataBucket, err := tx.CreateBucketIfNotExists([]byte("data"))
			if err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}
			data, err := json.Marshal(value)
			if err != nil {
				return err
			}
			err = dataBucket.Put([]byte(key), data)
			return err
		},
	)

	// update the "expTimes" bucket
	err = myDB.Update(
		func(tx *bolt.Tx) error {
			expTimes, err := tx.CreateBucketIfNotExists([]byte("expTimes"))
			if err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}
			// current time in seconds from the epoch plus the seconds for expiration
			expTime := time.Now().Unix() + int64(ttd)
			timeStr := []byte(strconv.FormatInt(expTime, 10))
			if err != nil {
				return err
			}
			err = expTimes.Put([]byte(key), timeStr)
			return err
		},
	)

	if err != nil {
		return fmt.Errorf("updating database: %s", err)
	}
	return nil

}

func (db *embedDB) Retrieve(key string, value interface{}) error {
	myDB := db.db
	var myData []byte
	err := myDB.View(func(tx *bolt.Tx) error {
		data := tx.Bucket([]byte("data"))
		times := tx.Bucket([]byte("times"))
		expTime := times.Get([]byte(key))
		timeStr, err := strconv.ParseInt(string(expTime), 10, 64)
		expSec := time.Unix(timeStr, 0)
		if time.Now().After(expSec) {
			myData = data.Get([]byte(key))

		}
		return err
	})
	if err != nil {
		return err
	}
	return json.Unmarshal(myData, value)
}
