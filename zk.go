package coordinator

import "strings"

const ZkPathRc = "/REGISTER_CENTER"
const ZkPathBc = "/BROADCAST"
const ZkPathCoordinateLock = "/COORDINATOR"
const ZkPathVersion = "/PATH_INIT"
const ZKPathACK = "/ACK"

type ZkPath string

func (prefix ZkPath) trim() string {
	return strings.TrimRight(string(prefix), "/")
}

func (prefix ZkPath) ack() string {
	return prefix.trim() + ZKPathACK
}

func (prefix ZkPath) ackChild(nodeId string) string {
	return prefix.ack() + "/" + nodeId
}

// /
func (prefix ZkPath) root() string {
	return prefix.trim()
}

// /VERSION
func (prefix ZkPath) pathInitLock() string {
	return prefix.trim() + ZkPathVersion
}

// /REGISTER_CENTER
func (prefix ZkPath) registerCenter() string {
	return prefix.trim() + ZkPathRc
}

// /REGISTER_CENTER/nodeId
func (prefix ZkPath) registerCenterNode(nodeId string) string {
	return prefix.trim() + ZkPathRc + "/" + nodeId
}

// /BROADCAST
func (prefix ZkPath) broadCast() string {
	return prefix.trim() + ZkPathBc
}

// /LOCK_COORDINATOR
func (prefix ZkPath) coordinatorLock() string {
	return prefix.trim() + ZkPathCoordinateLock
}
