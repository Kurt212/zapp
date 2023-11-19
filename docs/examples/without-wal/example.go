package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Kurt212/zapp"
)

func main() {
	dir := os.TempDir()

	params := zapp.NewParamsBuilder(dir).
		SegmentsNum(32).
		SyncPeriod(time.Minute).
		SyncPeriodDeltaMax(time.Second * 30).  // sync period will be random for each segment in range [1m, 1.5m]
		RemoveExpiredPeriod(time.Minute * 10). // remove expired will be exactly the same for each segment 10m
		// RemoveExpiredDeltaMax() not set
		Params()

	db, err := zapp.New(params)
	if err != nil {
		log.Fatalln(err)
	}

	defer db.Close()

	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err = db.Set(key, []byte(value), 0)
		if err != nil {
			log.Fatalln(err)
		}
	}

	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key-%d", i)
		val, err := db.Get(key)
		if err != nil {
			log.Fatalln(err)
		}

		fmt.Printf("%s = %s\n", key, string(val))
	}
}
