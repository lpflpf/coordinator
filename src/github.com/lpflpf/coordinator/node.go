package coordinator

import (
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"sync"
	"time"
)

/**
 * 一个节点，应该有Zk，Id
 */
type Node struct {
	Id       string
	Conn     *zk.Conn
	ZkPath   ZK_PATH
	Sharding Sharding
	event    chan struct{}
}

func (node *Node) Start(strategy Strategy, responseTimeout time.Duration) {
	coordinator := coordinator{
		node:            node,
		strategy:        strategy,
		sharding:        node.Sharding,
		responseTimeout: responseTimeout,
	}
	go coordinator.listen()
	go node.listenBroadCast()
}

func (node *Node) registerCenter() {
	_, err := node.Conn.Create("", []byte(node.Id), zk.FlagEphemeral, nil)

	if err != nil {
		log.Fatalf("register center failed.%v", err)
	}
}

func (node *Node) listenBroadCast() {
	once := sync.Once{}

	for {
		_, _, event, err := node.Conn.GetW(node.ZkPath.broadCast())
		if err != nil {
			continue
		}

		once.Do(func() { node.registerCenter() })
		select {
		case <-event:
			data, _, err := node.Conn.Get("/broadcast")
			if err != nil {
				break
			}

			node.Sharding.Decode(data)
			node.event <- struct{}{}
			node.response()
		}
	}
}

func (node *Node) response() {
	if path, err := node.Conn.Create(node.ZkPath.responseNode(node.Id), nil, zk.FlagSequence, zk.WorldACL(zk.PermAll)); err != nil {
		Logger.Fatalf("create path %v error: %v", path, err)
	}
}
