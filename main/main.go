package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/Kurt212/zapp"
	"github.com/tidwall/lotsa"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	test()

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

}
func test() {
	err := os.RemoveAll("data")
	if err != nil {
		panic(err)
	}

	db, err := zapp.New()
	if err != nil {
		log.Fatalln(err)
	}

	seed := int64(1570109110136449000)
	rng := rand.New(rand.NewSource(seed))
	/////////////////////////////////
	fmt.Println("Testing correctess")

	type d struct {
		k string
		v []byte
	}
	testData := make([]d, 0, 1000)
	for i := 0; i < 1000; i++ {
		key := string(randKey(rng, 20))

		valueSize := int(rng.Int31())%980 + 10
		value := randKey(rng, valueSize)

		testData = append(testData, d{key, value})
	}

	for i := 0; i < 500; i++ {
		kv := testData[i]
		k, v := kv.k, kv.v

		err := db.Set(k, v, 0)
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < 500; i++ {
		kv := testData[i]
		k, v := kv.k, kv.v

		data, err := db.Get(k)
		if err != nil {
			panic(err)
		}

		if !bytes.Equal(v, data) {
			panic(fmt.Errorf("%s is not equal to %s", string(v), string(data)))
		}
	}

	for i := 500; i < 1000; i++ {
		kv := testData[i]
		k, v := kv.k, kv.v

		err := db.Set(k, v, 0)
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < 1000; i++ {
		kv := testData[i]
		k, v := kv.k, kv.v

		data, err := db.Get(k)
		if err != nil {
			panic(err)
		}

		if !bytes.Equal(v, data) {
			panic(fmt.Errorf("%s is not equal to %s", string(v), string(data)))
		}
	}

	for i := 0; i < 1000; i++ {
		kv := testData[i]
		err = db.Delete(kv.k)
		if err != nil {
			panic(fmt.Errorf("when Delete key '%s' got error: '%v'", kv.k, err))
		}
	}

	for i := 0; i < 1000; i++ {
		kv := testData[i]
		_, err = db.Get(kv.k)
		if err != zapp.ErrNotFound {
			panic(fmt.Errorf("when Get deleted key got error %v other ErrNotFound", err))
		}
	}

	for i := 0; i < 1000; i++ {
		kv := testData[i]
		err = db.Delete(kv.k)
		if err != zapp.ErrNotFound {
			panic(fmt.Errorf("when Delete deleted key got error %v other ErrNotFound", err))
		}
	}

	fmt.Printf("Finished testing correctess. Test is passed\n\n\n")
	/////////////////////////////////
	fmt.Println("Testing durability")

	fmt.Println("Closing old db and opening again...")
	err = db.Close()
	if err != nil {
		panic(err)
	}

	db, err = zapp.New()
	if err != nil {
		panic(err)
	}

	fmt.Println("Filling some data...")

	for i := 0; i < 1000; i++ {
		kv := testData[i]
		k, v := kv.k, kv.v

		err := db.Set(k, v, 0)
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("Closing old db and opening again...")
	err = db.Close()
	if err != nil {
		panic(err)
	}

	db, err = zapp.New()
	if err != nil {
		panic(err)
	}

	fmt.Println("Checking data...")

	for i := 0; i < 1000; i++ {
		kv := testData[i]
		k, v := kv.k, kv.v

		data, err := db.Get(k)
		if err != nil {
			panic(err)
		}

		if !bytes.Equal(v, data) {
			panic(fmt.Errorf("%s is not equal to %s", string(v), string(data)))
		}
	}

	fmt.Printf("Finished testing durability. Test is passed\n\n\n")
	/////////////////////////////////
	fmt.Println("Checking expiry")

	ttl := time.Second * 5

	for i := 0; i < 100; i++ {
		kv := testData[i]
		k, v := kv.k, kv.v

		err := db.Set(k, v, ttl)
		if err != nil {
			panic(err)
		}
	}

	for i := 0; i < 100; i++ {
		kv := testData[i]
		k, v := kv.k, kv.v

		data, err := db.Get(k)
		if err != nil {
			panic(err)
		}

		if !bytes.Equal(v, data) {
			panic(fmt.Errorf("%s is not equal to %s", string(v), string(data)))
		}
	}

	time.Sleep(ttl + time.Second)

	for i := 0; i < 100; i++ {
		kv := testData[i]

		_, err := db.Get(kv.k)
		if !errors.Is(err, zapp.ErrNotFound) {
			panic(fmt.Errorf("expected NotFound error, but got: %w", err))
		}
	}

	err = db.Close()
	if err != nil {
		panic(err)
	}

	db, err = zapp.New()
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		kv := testData[i]

		_, err := db.Get(kv.k)
		if !errors.Is(err, zapp.ErrNotFound) {
			panic(fmt.Errorf("expected NotFound error, but got: %w", err))
		}
	}

	fmt.Printf("Finished checking expiry. Test is passed\n\n\n")
	/////////////////////////////////
	fmt.Println("Testing performance:")

	fmt.Println("Closing old db and opening again...")
	err = db.Close()
	if err != nil {
		panic(err)
	}

	err = os.RemoveAll("data")
	if err != nil {
		panic(err)
	}

	db, err = zapp.New()
	if err != nil {
		panic(err)
	}

	fmt.Println("Set new keys operation:")

	N := 10_000_000
	K := 10
	CPUs := 4

	keysm := make(map[string]bool, N)
	for len(keysm) < N {
		keysm[string(randKey(rng, K))] = true
	}
	keys := make([]string, 0, N)
	for key := range keysm {
		keys = append(keys, key)
	}

	lotsa.Output = os.Stdout
	lotsa.MemUsage = true

	lotsa.Ops(N, CPUs, func(i, _ int) {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		err := db.Set(keys[i], b, 0)
		if err != nil {
			panic(err)
		}
	})

	fmt.Println("Replace the same keys operation:")

	lotsa.Ops(N, CPUs, func(i, _ int) {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i)+1)
		err := db.Set(keys[i], b, 0)
		if err != nil {
			panic(err)
		}
	})

	fmt.Printf("Get operation. Retrieving each of %d keys 10 times:\n", N)

	lotsa.Ops(N*10, CPUs, func(i, _ int) {
		i = i % N
		_, err := db.Get(keys[i])
		if err != nil {
			panic(err)
		}
	})

	fmt.Println("Delete operation:")

	lotsa.Ops(N, CPUs, func(i, _ int) {
		err := db.Delete(keys[i])
		if err != nil {
			panic(err)
		}
	})
}

func randKey(rnd *rand.Rand, n int) []byte {
	s := make([]byte, n)
	rnd.Read(s)
	for i := 0; i < n; i++ {
		s[i] = 'a' + (s[i] % 26)
	}
	return s
}
