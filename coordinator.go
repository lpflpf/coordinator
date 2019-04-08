package coordinator

import (
	"errors"
	"github.com/samuel/go-zookeeper/zk"
	"log"
	"os"
	"strconv"
	"time"
)

var Logger = log.New(os.Stdout, "", log.LstdFlags)

// 一个协调器应具备如下功能：
// 首先，一个协调器是一个节点
// 其次，协调器应具备策略、分片的功能
type coordinator struct {
	node            *Node
	strategy        Strategy
	sharding        Sharding
	broadcast       broadcast
	responseTimeout time.Duration
}

type broadcast struct {
	version int32
}

func (c *coordinator) createPath() {
	lock := zk.NewLock(c.node.Conn, c.node.ZkPath.coordinatorInitLock(), zk.WorldACL(zk.PermAll))
	_ = lock.Lock()
	defer func() { _ = lock.Unlock() }()

	createNodeFunc := func(zkPath string, data []byte) {
		if path, err := c.node.Conn.Create(zkPath, data, zk.FlagSequence, zk.WorldACL(zk.PermAll)); err == nil {
			Logger.Printf("create path: %s succeed.", path)
		} else {
			Logger.Fatalf("create path: %s err: %v", path, err)
		}
	}

	if isExist, _, err := c.node.Conn.Exists(c.node.ZkPath.broadCast()); err != nil {
		Logger.Fatalf("exists %s method err: %v", c.node.ZkPath.broadCast(), err)
	} else if !isExist {
		createNodeFunc(c.node.ZkPath.broadCast(), c.node.Sharding.Encode())
		createNodeFunc(c.node.ZkPath.response(), []byte{})
		createNodeFunc(c.node.ZkPath.version(), []byte("0"))
		createNodeFunc(c.node.ZkPath.registerCenter(), []byte{})
	}
}

func (c *coordinator) listen() {
	c.createPath()
	for {
		_, state, e, err := c.node.Conn.ChildrenW(c.node.ZkPath.registerCenter())
		if err != nil {
			Logger.Fatal(err)
			continue
		}
		select {
		case event := <-e:
			if event.Type == zk.EventNodeChildrenChanged {
				c.reBalance(int(state.Cversion))
			}
		}
	}
}

func (c *coordinator) newLock() *zk.Lock {
	return zk.NewLock(c.node.Conn, c.node.ZkPath.coordinatorLock(), zk.WorldACL(zk.PermAll))
}

func (c *coordinator) reBalance(version int) {
	lock := c.newLock()
	defer func() {
		if err := lock.Unlock(); err != nil {
			Logger.Fatal(err)
		}
	}()
	if c.checkIsOk(version) {
		return
	}
	nodes := c.getCurrentLiveNode()
	c.loadSharding()
	c.broadCastSharding(c.strategy.ReBalance(c.sharding, nodes))
	if err := c.waitingResponse(nodes); err == nil {
		if err := c.updateVersion(version); err != nil {
			Logger.Fatalf("rebalance failed, update version failed. %v", err)
		}
	} else {
		Logger.Fatalf("rebalance failed, %v", err)
	}
}

func (c *coordinator) getCurrentLiveNode() []string {
	children, _, err := c.node.Conn.Children(c.node.ZkPath.registerCenter())
	if err != nil {
		Logger.Fatal(err)
	}

	return children
}

func (c *coordinator) loadSharding() {
	// 若不存在broadcast，则以node 节点传入的sharding 为准（第一次初始化）
	if isExist, _, err := c.node.Conn.Exists(c.node.ZkPath.broadCast()); err == nil && isExist {
		data, stat, err := c.node.Conn.Get(c.node.ZkPath.broadCast())
		if err != nil {
			Logger.Fatal(err)
		}

		c.broadcast.version = stat.Version
		c.sharding.Decode(data)
	}
}

func (c *coordinator) broadCastSharding(sharding Sharding) {
	_, err := c.node.Conn.Set(c.node.ZkPath.broadCast(), sharding.Encode(), c.broadcast.version)

	if err != nil {
		Logger.Fatal(err)
	}
}

func (c *coordinator) clearResponse() {
	children, _, _ := c.node.Conn.Children(c.node.ZkPath.response())

	for _, nodeId := range children {
		childPath := c.node.ZkPath.responseNode(nodeId)
		_, state, err := c.node.Conn.Get(childPath)
		if err != nil {
			Logger.Fatal(err)
		}
		if err := c.node.Conn.Delete(childPath, state.Version); err != nil {
			Logger.Fatal(err)
		}
	}
}

func (c *coordinator) waitingResponse(nodes []string) error {
	timer := time.NewTimer(c.responseTimeout)

	for {
		_, _, event, err := c.node.Conn.ChildrenW(c.node.ZkPath.response())
		if err != nil {
			Logger.Fatal(err)
		}
		select {
		case <-timer.C:
			return errors.New("cannot get all response.")
		case <-event:
			children, _, err := c.node.Conn.Children(c.node.ZkPath.response())
			if err != nil {
				return err
			}
			if len(children) < len(nodes) {
				continue
			} else {
				childMap := map[string]struct{}{}

				for _, child := range children {
					childMap[child] = struct{}{}
				}
				for _, node := range nodes {
					if _, ok := childMap[node]; !ok {
						return errors.New("no")
					}
				}
				return nil
			}
		}
	}
}

func (c *coordinator) updateVersion(version int) error {
	lock := zk.NewLock(c.node.Conn, c.node.ZkPath.version(), zk.WorldACL(zk.PermAll))
	if err := lock.Lock(); err != nil {
		return err
	}
	defer func() { _ = lock.Unlock() }()

	data, state, err := c.node.Conn.Get(c.node.ZkPath.version())

	strVersion := strconv.Itoa(int(version))
	if string(data) == strVersion {
		return nil
	}

	_, err = c.node.Conn.Set(c.node.ZkPath.version(), []byte(strVersion), state.Version)
	return err
}

func (c *coordinator) checkIsOk(version int) bool {
	data, _, err := c.node.Conn.Get(c.node.ZkPath.version())

	if err != nil {
		Logger.Fatal(err)
	}

	curVersion, _ := strconv.Atoi(string(data))
	return curVersion >= version
}
