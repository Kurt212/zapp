package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"

	"github.com/Kurt212/zapp"
	"github.com/tidwall/lotsa"
)

func main() {
	db, err := zapp.New()
	if err != nil {
		log.Fatalln(err)
	}

	seed := int64(1570109110136449000)
	rng := rand.New(rand.NewSource(seed))
	/////////////////////////////////
	fmt.Println("Testing correctess and durability")

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

		err := db.Set(k, v)
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

		err := db.Set(k, v)
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

	fmt.Println("Finished testing correctess and durability")
	fmt.Println("Test is passed")
	/////////////////////////////////
	fmt.Println("Testing performance:")

	fmt.Println("Set operation:")

	N := 10_000_000
	K := 10

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

	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		err := db.Set(keys[i], b)
		if err != nil {
			panic(err)
		}
	})

	fmt.Println("Get operation:")

	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		_, err := db.Get(keys[i])
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
