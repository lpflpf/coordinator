package main

import "github.com/lpflpf/coordinator"
import "fmt"
import "time"
import "strconv"
import "os"

var running bool

func main() {
	zkServer := []string{"127.0.0.1:10690", "127.0.0.1:10693", "127.0.0.1:10696"}
	sharding := &coordinator.SimpleSharding{Ver: "1.2", Data: map[string][]int{
		"node1": {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30},
	}}
	hostname, _ := os.Hostname()
	nodeId := hostname + "_" + strconv.Itoa(os.Getpid())
	node := &coordinator.Node{
		Id:             nodeId,
		ZkPath:         "/engineMsg",
		Sharding:       sharding,
		WaitAckTimeout: 5 * time.Second,
	}

	_ = node.Start(zkServer, 5*time.Second)
	go func() {
		event := node.Listener()
		for shard := range event {
			if shard.Status == coordinator.RUNNING {
				sharding = shard.NewSharding.(*coordinator.SimpleSharding)
				fmt.Println("CLIENT | update sharding ", shard.NewSharding.(*coordinator.SimpleSharding))
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

		if iter > 1000 {
			node.Close()
			break
		}
	}

}
