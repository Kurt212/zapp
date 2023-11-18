package main

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"

	"github.com/Kurt212/zapp"
	"github.com/tidwall/lotsa"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

var testPerf = flag.Bool("test-perf", false, "")
var testPerfPresets = flag.String("test-perf-presets", "", "")

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

	if *testPerf {
		if *testPerfPresets == "" {
			log.Fatal("test-perf-presets is not provided")
		}
		err := testPerformance(*testPerfPresets)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		test()
	}

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

func newZapp() *zapp.DB {
	b := zapp.NewParamsBuilder("./data").
		SegmentsNum(8).
		SyncPeriod(time.Minute).
		SyncPeriodDeltaMax(time.Minute). // for randomness in sync periods
		RemoveExpiredPeriod(time.Minute).
		RemoveExpiredDeltaMax(time.Second * 10). // for randomness in expire checks
		UseWAL(false)

	z, err := zapp.New(b.Params())
	if err != nil {
		log.Fatalln(err)
	}

	return z
}

var (
	seed = int64(212)
	rng  = rand.New(rand.NewSource(seed))
)

func test() {
	err := os.RemoveAll("data")
	if err != nil {
		panic(err)
	}

	db := newZapp()

	/////////////////////////////////
	fmt.Println("Testing correctess")

	type d struct {
		k string
		v []byte
	}
	testData := make([]d, 0, 1000)
	for i := 0; i < 1000; i++ {
		key := string(randBytes(rng, 20))

		valueSize := int(rng.Int31())%980 + 10
		value := randBytes(rng, valueSize)

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
	db.Close()

	db = newZapp()

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
	db.Close()

	db = newZapp()

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

	db.Close()

	db = newZapp()

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
	db.Close()

	err = os.RemoveAll("data")
	if err != nil {
		panic(err)
	}

	db = newZapp()

	fmt.Println("Set new keys operation:")

	N := 1_000_000
	K := 10
	CPUs := 4

	keysm := make(map[string]bool, N)
	for len(keysm) < N {
		keysm[string(randBytes(rng, K))] = true
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

func testPerformance(presetsFileName string) error {
	type preset struct {
		segments  int
		threads   int
		keyNumber int
		valueSize int
		wal       bool
	}

	file, err := os.Open(presetsFileName)
	if err != nil {
		return err
	}

	csvReader := csv.NewReader(file)
	csvReader.Comma = '\t'

	lines, err := csvReader.ReadAll()
	if err != nil {
		return err
	}
	lines = lines[1:]

	var presets []preset

	for _, line := range lines {
		segments, err := strconv.Atoi(line[0])
		if err != nil {
			return err
		}
		threads, err := strconv.Atoi(line[1])
		if err != nil {
			return err
		}
		keyNumber, err := strconv.Atoi(line[2])
		if err != nil {
			return err
		}
		valueSize, err := strconv.Atoi(line[3])
		if err != nil {
			return err
		}
		wal, err := strconv.Atoi(line[4])
		if err != nil {
			return err
		}

		presets = append(presets, preset{
			segments:  segments,
			threads:   threads,
			keyNumber: keyNumber,
			valueSize: valueSize,
			wal:       wal == 1,
		})
	}

	testPerf := func(pr preset) (setQPS, resetQPS, getQPS, delQPS int) {
		err := os.RemoveAll("data")
		if err != nil {
			panic(err)
		}

		measure := func(itterations int, f func(i int)) (duration time.Duration, QPS int) {
			wg := sync.WaitGroup{}
			wg.Add(pr.threads)
			start := time.Now()
			for threadId := 0; threadId < pr.threads; threadId++ {
				s, e := itterations/pr.threads*threadId, itterations/pr.threads*(threadId+1)
				if threadId == pr.threads-1 {
					e = itterations
				}
				go func(s, e int) {
					defer wg.Done()
					for i := s; i < e; i++ {
						f(i)
					}
				}(s, e)
			}
			wg.Wait()
			elapsed := time.Since(start)

			return elapsed, int(float64(itterations) / elapsed.Seconds())
		}

		b := zapp.NewParamsBuilder("./data").
			SegmentsNum(pr.segments).
			SyncPeriod(time.Minute).
			SyncPeriodDeltaMax(time.Minute). // for randomness in sync periods
			RemoveExpiredPeriod(time.Minute).
			RemoveExpiredDeltaMax(time.Minute). // for randomness in expire checks
			UseWAL(pr.wal)

		db, err := zapp.New(b.Params())
		if err != nil {
			log.Fatalln(err)
		}

		defer db.Close()

		log.Println("generating keys")

		KeySize := 10

		keysm := make(map[string]struct{}, pr.keyNumber)
		for len(keysm) < pr.keyNumber {
			keysm[string(randBytes(rng, KeySize))] = struct{}{}
		}
		keys := make([]string, 0, pr.keyNumber)
		for key := range keysm {
			keys = append(keys, key)
		}

		valuesCount := 10_000
		if pr.keyNumber < valuesCount {
			valuesCount = pr.keyNumber
		}

		values := make([][]byte, valuesCount)
		for i := 0; i < valuesCount; i++ {
			values[i] = randBytes(rng, pr.valueSize)
		}

		lotsa.Output = os.Stdout
		lotsa.MemUsage = true

		log.Println("set")

		_, setQPS = measure(pr.keyNumber, func(i int) {
			b := values[i%valuesCount]
			err := db.Set(keys[i], b, 0)
			if err != nil {
				panic(err)
			}
		})

		log.Println("shuffle keys")
		keys = shuffleKeys(keys)

		log.Println("reset")

		_, resetQPS = measure(pr.keyNumber, func(i int) {
			b := values[(i+1)%valuesCount]
			err := db.Set(keys[i], b, 0)
			if err != nil {
				panic(err)
			}
		})

		log.Println("shuffle keys")
		keys = shuffleKeys(keys)

		log.Println("get")

		_, getQPS = measure(pr.keyNumber*10, func(i int) {
			i = i % 10
			_, err := db.Get(keys[i])
			if err != nil {
				panic(err)
			}
		})

		log.Println("delete")

		_, delQPS = measure(pr.keyNumber, func(i int) {
			err := db.Delete(keys[i])
			if err != nil {
				panic(err)
			}
		})

		log.Println("finished")

		return setQPS, resetQPS, getQPS, delQPS
	}

	csvWriter := csv.NewWriter(os.Stdout)

	csvWriter.Write([]string{
		"segments",
		"threads",
		"keys",
		"value_size",
		"wal",
		"set",
		"reset",
		"get",
		"delete",
	})

	csvWriter.Flush()

	for i := 0; i < len(presets); i++ {
		pr := presets[i]

		log.Printf("segments=%d threads=%d keys=%s value_size=%d wal=%t\n", pr.segments, pr.threads, commaize(pr.keyNumber), pr.valueSize, pr.wal)
		set, reset, get, delete := testPerf(pr)
		log.Printf("SET=%s/sec ReSET=%s/sec GET=%s/sec DEL=%s/sec\n", commaize(set), commaize(reset), commaize(get), commaize(delete))
		log.Println()

		isWAL := 0
		if pr.wal {
			isWAL = 1
		}

		csvWriter.Write([]string{
			fmt.Sprint(pr.segments),
			fmt.Sprint(pr.threads),
			fmt.Sprint(pr.keyNumber),
			fmt.Sprint(pr.valueSize),
			fmt.Sprint(isWAL),
			fmt.Sprint(set),
			fmt.Sprint(reset),
			fmt.Sprint(get),
			fmt.Sprint(delete),
		})
		csvWriter.Flush()
	}

	return nil
}

func commaize(n int) string {
	s1, s2 := fmt.Sprintf("%d", n), ""
	for i, j := len(s1)-1, 0; i >= 0; i, j = i-1, j+1 {
		if j%3 == 0 && j != 0 {
			s2 = "," + s2
		}
		s2 = string(s1[i]) + s2
	}
	return s2
}

func shuffleKeys(src []string) []string {
	dest := make([]string, len(src))
	perm := rand.Perm(len(src))
	for i, v := range perm {
		dest[v] = src[i]
	}

	return dest
}

func randBytes(rnd *rand.Rand, n int) []byte {
	s := make([]byte, n)
	rnd.Read(s)
	for i := 0; i < n; i++ {
		s[i] = 'a' + (s[i] % 26)
	}
	return s
}
