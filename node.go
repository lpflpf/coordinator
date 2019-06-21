package coordinator

import (
	"log"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// Using Node, just create a Node struct, and call start method.
type Node struct {
	Id       string
	Conn     *zk.Conn
	ZkPath   ZkPath
	Sharding Sharding
	Event    chan none
}

// start method will create a coordinator, listen coordinator broadcast.
// strategy must be implement by user.
func (node *Node) Start(strategy Strategy, timeout time.Duration) {
	coordinator := coordinator{
		node:            node,
		strategy:        strategy,
		sharding:        node.Sharding,
		responseTimeout: timeout,
	}
	coordinator.createPath()
	afterListen := make(chan none)
	go coordinator.listen(afterListen)
	<-afterListen
	go node.listenBroadCast()
}

func (node *Node) registerCenter() {
	_, err := node.Conn.Create(node.ZkPath.registerCenterNode(node.Id), []byte(node.Id), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	Logger.Println("register center")
	if err == zk.ErrNodeExists || err == nil {
		return
	} else {
		log.Fatalf("register center failed.%v", err)
	}
}

func (node *Node) listenBroadCast() {
	once := sync.Once{}

	for {
		_, _, event, err := node.Conn.GetW(node.ZkPath.broadCast())
		Logger.Printf("Id: %s listen %s\n", node.Id, node.ZkPath.broadCast())
		if err != nil {
			continue
		}

		once.Do(func() { node.registerCenter() })
		select {
		case <-event:
			data, _, err := node.Conn.Get(node.ZkPath.broadCast())
			if err != nil {
				break
			}

			Logger.Println("get broadcast message.", string(data))
			node.Sharding.Decode(data)
			node.response()
			node.Event <- none{}
		}
	}
}

func (node *Node) response() {
	if path, err := node.Conn.Create(node.ZkPath.responseNode(node.Id), nil, 0, zk.WorldACL(zk.PermAll)); err != nil {
		Logger.Fatalf("create path %v error: %v", path, err)
	}
}
