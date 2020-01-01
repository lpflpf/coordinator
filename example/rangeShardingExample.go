package main

import (
	"encoding/json"
	"github.com/lpflpf/coordinator"
	"sort"
)
import "fmt"
import "time"
import "strconv"
import "os"

type Range struct {
	Begin int
	End   int
}
type RangeSharding struct {
	version   string
	Data      map[string]Range
	RangeSize int
}

// 编码和解码，保存于zk broadcast中
func (sharding *RangeSharding) Encode() []byte {
	data, _ := json.Marshal(sharding)
	return data
}

func (sharding *RangeSharding) Decode(data []byte) coordinator.Sharding {
	result := RangeSharding{}
	_ = json.Unmarshal(data, &result)
	return &result
}

func (sharding *RangeSharding) Equal(sharding2 coordinator.Sharding) bool {
	if sharding.Version() != sharding2.Version() || len(sharding.Data) != len(sharding2.(*RangeSharding).Data) {
		return false
	}

	for k := range sharding.Data {
		if _, ok := sharding2.(*RangeSharding).Data[k]; !ok {
			return false
		}
	}
	return true
}

func (sharding *RangeSharding) Version() string {
	return sharding.version
}

func (sharding *RangeSharding) ReBalance(currentLiveNodes []string) coordinator.Sharding {
	sort.Strings(currentLiveNodes)

	newSharding := &RangeSharding{
		version:   sharding.Version(),
		Data:      map[string]Range{},
		RangeSize: sharding.RangeSize,
	}

	remainder := sharding.RangeSize % len(currentLiveNodes)
	step := sharding.RangeSize / len(currentLiveNodes)
	var shift, begin int

	for idx, nodeId := range currentLiveNodes {
		shift = step
		if idx == 0 {
			shift = remainder + step
		}

		newSharding.Data[nodeId] = Range{
			Begin: begin,
			End:   begin + shift,
		}
		begin += shift
	}

	return newSharding
}

var running bool

func main() {
	zkServer := []string{"127.0.0.1:10690", "127.0.0.1:10693", "127.0.0.1:10696"}
	sharding := &RangeSharding{version: "v1.0", RangeSize: 24}
	hostname, _ := os.Hostname()
	nodeId := hostname + "_" + strconv.Itoa(os.Getpid())
	node := &coordinator.Node{
		Id:       nodeId,
		ZkPath:   "/engineMsg",
		Sharding: sharding,
		WaitAckTimeout: 5 * time.Second,
	}

	_ = node.Start(zkServer, 5*time.Second)

	go func() {
		event := node.Listener()
		for shard := range event {
			if shard.Status == coordinator.RUNNING {
				sharding = shard.NewSharding.(*RangeSharding)
				fmt.Println("CLIENT | update sharding ", shard.NewSharding.(*RangeSharding))
				running = true
				// 更新sharding 后，响应ack
			} else if shard.Status == coordinator.STOP {
				fmt.Println("CLIENT | update status, sharding stop")
				// when job stop, response channel 【对于暂停任务较慢的服务比较重要】
				running = false
			}

			time.Sleep(time.Second)
			// 等待服务stop，
			shard.Resp <- coordinator.None{}
		}
	}()

	iter := 0
	for {
		if running {
			if val, ok := sharding.Data[nodeId]; ok {
				fmt.Println(val)
			}
		}
		time.Sleep(time.Second)
		iter++

		if iter > 100 {
			node.Close()
			break
		}
	}

}
