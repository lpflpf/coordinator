package coordinator

import "encoding/json"

/*
 Sharding need convert to []byte, save at zookeeper response node.
*/
type Sharding interface {
	Encode() []byte
	Decode([]byte)
	Clear()
}

/*
a simple sharding using map, encode/decode by json.
*/
type SimpleSharding map[string][]int64

func (sharding SimpleSharding) Encode() []byte {
	data, _ := json.Marshal(sharding)
	return data
}

func (sharding SimpleSharding) Decode(data []byte) {
	sharding.Clear()
	_ = json.Unmarshal(data, &sharding)
}

func (sharding SimpleSharding) Clear() {
	for key, _ := range sharding {
		delete(sharding, key)
	}
}
