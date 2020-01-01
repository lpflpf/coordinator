package coordinator

import (
	"encoding/json"
	"sort"
)

// Sharding need convert to []byte, save at zookeeper response node.
type Sharding interface {
	Encode() []byte               // 编码和解码，保存于zk broadcast中
	Decode([]byte) Sharding       // 需要函数返回 （用于前后diff 版本）
	Equal(sharding Sharding) bool // diff sharding 是否相同， 如果相同则不再reBalance
	Version() string              // 版本， 若更新了shard内容，则需要更新版本
	ReBalance([]string) Sharding  // 重新分配任务
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

func (sharding *SimpleSharding) ReBalance(currentLiveNodes []string) Sharding {
	newSimpleSharding := SimpleSharding{
		Ver:  sharding.Version(),
		Data: map[string][]int{},
	}
	for _, node := range currentLiveNodes {
		newSimpleSharding.Data[node] = []int{}
	}

	var allJobs []int
	for _, jobs := range sharding.Data {
		allJobs = append(allJobs, jobs...)
	}

	sort.Ints(allJobs)
	sort.Strings(currentLiveNodes)

	Logger.Println("CURRENT JOB: ", allJobs)
	min := len(allJobs) / len(currentLiveNodes)
	shift := 0
	for iter, node := range currentLiveNodes {
		if iter+1+len(currentLiveNodes)*min > len(allJobs) {
			newSimpleSharding.Data[node] = append(newSimpleSharding.Data[node], allJobs[shift:min+shift]...)
			shift += min
		} else {
			newSimpleSharding.Data[node] = append(newSimpleSharding.Data[node], allJobs[shift:min+shift+1]...)
			shift += min + 1
		}
	}

	Logger.Println("RE_BALANCE OLD SHARDING", sharding, " NEW SHARDING: ", newSimpleSharding)
	return &newSimpleSharding
}
