package coordinator

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func getZkConn() *zk.Conn {
	conn, _, _ := zk.Connect([]string{"127.0.0.1:2181"}, 30*time.Second)
	return conn
}

func envInit(conn *zk.Conn, zkPath string) {

	root := ZK_PATH(zkPath)

	deletePath := func(path string) {
		_, state, _ := conn.Get(path)
		_ = conn.Delete(root.responseNode(path), state.Version)
	}
	deleteDir := func(path string) {
		if children, _, err := conn.Children(path); err != nil {
			for _, child := range children {
				deletePath(path + "/" + child)
			}
		}
		deletePath(path)
	}

	deleteDir(root.response())
	deleteDir(root.registerCenter())
	deleteDir(root.broadCast())
	deleteDir(root.version())
	deleteDir(root.coordinatorInitLock())
	deleteDir(root.coordinatorLock())
	deleteDir(root.root())
}

func startNode(conn *zk.Conn, id string, end chan none) {
	event := make(chan struct{})

	node := Node{
		Id:     strconv.Itoa(rand.Int()),
		Conn:   conn,
		ZkPath: "/tmp_test",
		Sharding: SimpleSharding(map[string][]int64{
			"": {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}),
		Event: event,
	}
	var strategy SimpleStrategy
	node.Start(strategy, 10*time.Second)
response:
	for {
		select {
		case <-event:
			fmt.Println("reBalance")
			fmt.Println(node.Sharding)
		case <-end:
			break response
		}
	}
}

func TestNode_Start(t *testing.T) {
	conn := getZkConn()
	envInit(conn, "/tmp_test")
	end := make(chan none)
	startNode(conn, strconv.Itoa(rand.Int()%100000), end)
}
