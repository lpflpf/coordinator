package coordinator

import (
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var Logger = log.New(os.Stdout, "", log.Llongfile|log.LstdFlags)

type JobStatus int8

const (
	RUNNING JobStatus = iota
	STOP
)

type None struct{}
type Event struct {
	NewSharding Sharding
	Resp        chan None
	Status      JobStatus
}

// Using Node, just create a Node struct, and call start method.
type Node struct {
	Id             string         // 标记一个节点
	ZkPath         ZkPath         // 项目zookeeper 路径
	Sharding       Sharding       // 定义的Sharding, Job 也由此传入
	WaitAckTimeout time.Duration  // 等待ack 超时时间， 默认5s
	event          chan Event     // 下发的Sharding变更消息
	conn           *zk.Conn       // zookeeper 连接
	close          chan None      // 关闭信号
	wg             sync.WaitGroup // 等待协程关闭
}

func (node *Node) Listener() chan Event {
	return node.event
}

// start method will create a coordinator, listen coordinator broadcast.
// strategy must be implement by user.
func (node *Node) Start(zkServers []string, timeout time.Duration) (err error) {
	node.close = make(chan None)
	node.event = make(chan Event)
	Logger.SetPrefix("COORDINATOR | ")
	if node.WaitAckTimeout == 0 {
		node.WaitAckTimeout = 5 * time.Second
	}
	if node.conn, err = node.newConn(zkServers, timeout); err != nil {
		return err
	}

	node.createPath()

	//go node.listenBroadCast()
	go node.diagnosis()

	// coordinator
	go node.listen()
	//<-afterListen

	node.registerCenter()
	return nil
}

// 在广播之前，需要清除ack 节点下所有记录
func (node *Node) clearAckData() error {
	ackChildren, _, err := node.conn.Children(node.ZkPath.ack())
	if err != nil {
		return err
	}

	for _, child := range ackChildren {
		path := node.ZkPath.ackChild(child)
		_, stat, err := node.conn.Get(path)
		if err != nil {
			return err
		}
		err = node.conn.Delete(path, stat.Version)
		if err != nil {
			return err
		}
	}
	return nil
}

func (node *Node) getAckNodeName(nodeId string, version int32) string {
	return strconv.Itoa(int(version)) + "_" + nodeId
}

// 等待 node 的返回
func (node *Node) waitAck(nodes []string, version int32) bool {
	for i := 0; time.Duration(i)*time.Second < node.WaitAckTimeout; i++ {
		time.Sleep(time.Second)
		ackChildren, _, err := node.conn.Children(node.ZkPath.ack())
		if err != nil {
			Logger.Println("WAIT ACK FAILED, ERR: ", err)
			continue
		}
		if len(ackChildren) == len(nodes) {
			{ // 增加版本的校验，防止高延迟的节点第二次返回，ack 混合
				sort.Strings(ackChildren)
				sort.Strings(nodes)
				for i, nodeId := range nodes {
					if node.getAckNodeName(nodeId, version) != ackChildren[i] {
						return false
					}
				}
			}
			return true
		}
	}
	return false
}

// 消息广播到各节点，并等待ack 返回
func (node *Node) broadcast(data []byte, nodes []string) (bool, error) {
	path := node.ZkPath.broadCast()
	_, stat, err := node.conn.Get(path)
	if err != nil {
		return false, err
	}

	if err = node.clearAckData(); err != nil {
		return false, err
	}
	if stat, err = node.conn.Set(path, data, stat.Version); err != nil {
		return false, err
	}

	return node.waitAck(nodes, stat.Version), err
}

func (node *Node) newConn(zkServer []string, timeout time.Duration) (*zk.Conn, error) {
	zkClient, _, err := zk.Connect(zkServer, timeout, zk.WithLogInfo(false))
	return zkClient, err
}

//  服务关闭
func (node *Node) Close() {
	close(node.close)
	node.wg.Wait()
	node.conn.Close()
	close(node.event)
}

func (node *Node) diagnosis() {
	node.wg.Add(1)
	defer node.wg.Done()
	path := node.ZkPath.broadCast()
	once := sync.Once{}
	for {
		data, state, eventWatcher, err := node.conn.GetW(path)
		if err != nil {
			Logger.Printf("Path:%v, Err: %v", path, err)
			continue
		}

		once.Do(func() {
			if len(data) != 0 {
				newSharding := node.Sharding.Decode(data)
				if newSharding.Version() == node.Sharding.Version() {
					ack := make(chan None)
					node.event <- Event{NewSharding: node.Sharding, Resp: ack, Status: RUNNING}
					<-ack
					//close(ack)
				}
			}
		})

		select {
		case event := <-eventWatcher:
			if event.Err != nil { // 其他数据的变更
				//node.Stop = true
				ack := make(chan None)
				node.event <- Event{Resp: ack, Status: STOP}
				<-ack
				Logger.Println("Err Event: ", event.Err)
				//Logger.Fatalln("Err Event: ", event.Err)
			} else if event.Type == zk.EventNodeDataChanged {
				data, _, err := node.conn.Get(path)
				if err != nil {
					break
				}
				ack := make(chan None)
				if string(data) == "stop" {
					//node.Stop = true
					node.event <- Event{Resp: ack, Status: STOP}
				} else {
					//node.Stop = false
					node.Sharding = node.Sharding.Decode(data)
					node.event <- Event{NewSharding: node.Sharding, Resp: ack, Status: RUNNING}
				}
				<-ack

				ackPath := node.ZkPath.ackChild(node.getAckNodeName(node.Id, state.Version+1))
				node.createOnePath(ackPath, nil)
			}
		case <-node.close:
			return
		}
	}
}

func (node *Node) registerCenter() {
	_, err := node.conn.Create(node.ZkPath.registerCenterNode(node.Id), nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err == zk.ErrNodeExists {
		log.Fatalf("id (%s) has been register by other node. (zkPath:%s)", node.Id,
			node.ZkPath.registerCenterNode(node.Id))
	} else if err != nil {
		log.Fatalf("register center failed.%v, Path:%s", err, node.ZkPath.registerCenterNode(node.Id))
	}
}

func (node *Node) createOnePath(zkPath string, data []byte) {
	if isExist, _, err := node.conn.Exists(zkPath); err != nil {
		Logger.Fatalf("exists %s method err: %v", zkPath, err)
	} else if isExist {
		return
	}

	if path, err := node.conn.Create(zkPath, data, 0, zk.WorldACL(zk.PermAll)); err != nil {
		Logger.Fatalf("create path: %s err: %v", path, err)
	}
}

// 初始化路径
func (node *Node) createPath() {
	lock := zk.NewLock(node.conn, node.ZkPath.pathInitLock(), zk.WorldACL(zk.PermAll))
	_ = lock.Lock()
	node.createOnePath(node.ZkPath.root(), []byte{})
	node.createOnePath(node.ZkPath.registerCenter(), []byte{})
	node.createOnePath(node.ZkPath.broadCast(), []byte{})
	node.createOnePath(node.ZkPath.ack(), []byte{})
	_ = lock.Unlock()
}

func (node *Node) listen() {
	node.wg.Add(1)
	defer node.wg.Done()
	lock := zk.NewLock(node.conn, node.ZkPath.coordinatorLock(), zk.WorldACL(zk.PermAll))

	if err := lock.Lock(); err != nil {
		Logger.Fatalf("lock err: %v", err)
	}

	defer func() {
		if err := lock.Unlock(); err != nil {
			Logger.Fatal(err)
		}
	}()
	Logger.Println("\033[41;36m This Node is Coordinator \033[0m")
	once := sync.Once{}
	ticker := time.Tick(30 * time.Second)
	for {
		_, _, e, err := node.conn.ChildrenW(node.ZkPath.registerCenter())
		if err != nil {
			Logger.Println(err)
			continue
		}
		//Logger.Println("LISTEN REGISTER_CENTER: ", node.ZkPath.registerCenter())

		once.Do(func() {
			node.reBalance()
		})

		select {
		case event := <-e:
			if event.Err != nil { // 其他数据的变更
				//node.Stop = true
				ack := make(chan None)
				node.event <- Event{Resp: ack, Status: STOP}
				//close(ack)
				<-ack
				Logger.Println("Err Event: ", event.Err)
				//Logger.Panic()
			} else if event.Type == zk.EventNodeChildrenChanged {
				Logger.Println("EVENT RE_BALANCE")
				node.reBalance()
			}
		case <-ticker:
			Logger.Println("TICKER RE_BALANCE")
			node.reBalance()
		case <-node.close:
			return
		}
	}
}

// reBalance
func (node *Node) reBalance() {
	// 可使用的nodes
	nodes := node.getComputeNodes()
	newSharding := node.Sharding.ReBalance(nodes)

	// 若调整后的sharding 和现有sharding 一样，则无需reBalance
	if newSharding.Equal(node.Sharding) {
		return
	}

	Logger.Println("BROADCAST DATA:", "stop")
	// balance 分两个阶段，暂停和开启； 防止直接sharding时，同一个时间有两个节点执行同一个任务
	if ok, err := node.broadcast([]byte("stop"), nodes); !ok {
		Logger.Println("STOP NODE Job FAILED. ERR: ", err)
		return
	}

	Logger.Println("BROADCAST DATA:", string(newSharding.Encode()))
	if ok, err := node.broadcast(newSharding.Encode(), nodes); !ok {
		Logger.Println("BROADCAST SHARDING FAILED. ERR: ", err)
		return
	}
}

// 获取当前计算节点
func (node *Node) getComputeNodes() []string {
	children, _, err := node.conn.Children(node.ZkPath.registerCenter())
	if err != nil {
		Logger.Fatal(err)
	}

	Logger.Println("CURRENT NODES: ", children)
	return children
}
