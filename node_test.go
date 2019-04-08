package coordinator

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"testing"
	"time"
)

func getZkConn() *zk.Conn {
	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, 30*time.Second)

	if err != nil {
		fmt.Println("---------", err)
	}
	return conn
}

func TestNode_Start(t *testing.T) {
	conn := getZkConn()
	event := make(chan struct{})

	node := Node{
		Id:     "1",
		Conn:   conn,
		ZkPath: "/tmp_test",
		Sharding: SimpleSharding(map[string][]int64{
			"": {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}),
		Event: event,
	}
	var strategy SimpleStrategy
	node.Start(strategy, time.Second)

	for {
		select {
		case <-event:
			fmt.Println(node.Sharding)
		}
	}
}
