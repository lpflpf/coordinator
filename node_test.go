package coordinator

import (
	"github.com/qiniu/log"
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

func envInit(zkPath string) {
	conn := getZkConn()
	defer conn.Close()
	root := ZK_PATH(zkPath)

	deletePath := func(path string) {
		_, state, err := conn.Get(path)
		Logger.Println("GET PATH ", path, err)
		err = conn.Delete(path, state.Version)
		Logger.Println("DELETE PATH ", path, err)
	}
	deleteDir := func(path string) {
		if children, _, err := conn.Children(path); err == nil {
			for _, child := range children {
				deletePath(path + "/" + child)
			}
		} else {
			Logger.Println(err)
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

func newNode() Node {
	id := strconv.Itoa(rand.Int() % 1000000)
	return Node{
		Id:     id,
		Conn:   getZkConn(),
		ZkPath: "/tmp_test",
		Sharding: SimpleSharding(map[string][]int64{
			id: {1, 2, 3, 4, 5, 6, 7, 8, 9, 0},
		}),
		Event: make(chan struct{}),
	}
}

func compareSimpleSharding(s1, s2 SimpleSharding) bool {
	if len(s1) != len(s2) {
		return false
	}

	for key, val1 := range s1 {
		if val2, ok := s2[key]; !ok {
			return false
		} else if len(val1) != len(val2) {
			return false
		} else {
			for idx, val11 := range val1 {
				if val2[idx] != val11 {
					return false
				}
			}
		}
	}
	return true
}

func TestNode_Start(t *testing.T) {
	envInit("/tmp_test")

	// sharding 0-9
	node1 := newNode()
	node2 := newNode()

	var strategy SimpleStrategy
	node1.Start(strategy, 10*time.Second)
	<-node1.Event
	node1Sharding := node1.Sharding.(SimpleSharding)
	if len(node1Sharding) == 1 && len(node1Sharding[node1.Id]) == 10 {
		t.Logf("node 1 add succeed. get sharding: %v", node1.Sharding.(SimpleSharding)[node1.Id])
	} else {
		t.Errorf("node 1 add failed. get sharding: %v", node1.Sharding.(SimpleSharding))
	}
	node2.Start(strategy, 10*time.Second)
	<-node2.Event
	<-node1.Event

	node1Sharding = node1.Sharding.(SimpleSharding)
	node2Sharding := node2.Sharding.(SimpleSharding)

	if compareSimpleSharding(node1Sharding, node2Sharding) {
		t.Logf("after node 2 add, node 1 sharding equal node 2 sharding.")
		if len(node1Sharding) == 2 && len(node1Sharding[node1.Id]) == 5 && len(node1Sharding[node2.Id]) == 5 {
			t.Logf("afeer node 2 add, node 1 get sharding succeed. node 1 sharding %v, node 2 sharding %v ", node1Sharding, node2Sharding)
		} else {
			t.Errorf("after node 2 add, node 1 get sharding failed. %v", node1Sharding)
		}

		if len(node2Sharding) == 2 && len(node2Sharding[node1.Id]) == 5 && len(node2Sharding[node2.Id]) == 5 {
			t.Logf("afeer node 2 add, node 2 get sharding succeed. node 1 sharding %v, node 2 sharding %v ", node1Sharding, node2Sharding)
		} else {
			t.Errorf("after node 2 add, node 2 get sharding failed. %v", node2Sharding)
		}
	} else {
		t.Errorf("after node 2 add, node 1 sharding not equal node 2 sharding.")
	}
}

func TestSimpleSharding_Decode(t *testing.T) {
	dataset := []SimpleSharding{
		SimpleSharding(map[string][]int64{"node1": {1, 2, 3, 4, 5, 6, 7, 8, 9, 0}}),
		{},
	}

	for _, simpleSharding := range dataset {
		curr := simpleSharding
		data := simpleSharding.Encode()
		simpleSharding.Decode(data)

		if compareSimpleSharding(simpleSharding, curr) {
			log.Info("simple sharding encode/decode ok", curr)
		} else {
			log.Info("simple sharding encode/decode data not identical.")
		}
	}

}
