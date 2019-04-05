package coordinator

import "encoding/json"

type Sharding interface {
	Encode() []byte
	Decode([]byte)
}

type SimpleSharding map[string][]int64

func (sharding SimpleSharding) Encode() []byte {
	data, _ := json.Marshal(sharding)
	return data
}

func (sharding SimpleSharding) Decode(data []byte) {
	_ = json.Unmarshal(data, &sharding)
}
