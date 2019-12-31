package coordinator

import "encoding/json"

// Sharding need convert to []byte, save at zookeeper response node.
type Sharding interface {
	Encode() []byte               // 编码和解码，保存于zk broadcast中
	Decode([]byte) Sharding       // 需要函数返回 （用于前后diff 版本）
	Equal(sharding Sharding) bool // diff sharding 是否相同， 如果相同则不再reBalance
	Version() string              // 版本， 若更新了shard内容，则需要更新版本
}

// 一个简单的Sharding的实现，包含了版本信息和Sharding 的数据信息
type SimpleSharding struct {
	Data map[string][]int
	Ver  string
}

func (sharding *SimpleSharding) Encode() []byte {
	data, _ := json.Marshal(sharding)
	return data
}

func (sharding *SimpleSharding) Decode(data []byte) Sharding {
	result := SimpleSharding{}
	_ = json.Unmarshal(data, &result)
	return &result
}

func (sharding *SimpleSharding) Version() string {
	return sharding.Ver
}

func (sharding *SimpleSharding) Equal(sharding2 Sharding) bool {

	if len(sharding.Data) != len(sharding2.(*SimpleSharding).Data) {
		return false
	}

	s2 := sharding2.(*SimpleSharding).Data

	for k, v := range sharding.Data {
		v2, ok := s2[k]
		if !ok || len(v) != len(v2) {
			return false
		}
	}
	return true
}
