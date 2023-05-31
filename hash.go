package zepp

import (
	"github.com/spaolacci/murmur3"
)

func hash(data []byte) uint32 {
	return murmur3.Sum32(data)
}
