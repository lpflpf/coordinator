package coordinator

import (
	"github.com/samuel/go-zookeeper/zk"
	"os"
	"testing"
	"time"
)

func TestNode_Start(t *testing.T) {
	ts, err := zk.StartTestCluster(1, nil, os.Stdout)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Stop() }()
	conn, _, err := ts.ConnectAll()

	node := &Node{
		Id:       "",
		Conn:     conn,
		ZkPath:   "",
		Sharding: SimpleSharding(map[string][]int64{}),
	}

	node.Start(SimpleStrategy{}, 30*time.Second)

}
