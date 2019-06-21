package coordinator

const ZkPathRc = "REGISTER_CENTER"
const ZkPathBc = "BROADCAST"
const ZkPathRe = "RESPONSE"
const ZkPathCoorLock = "LOCK_COORDINATOR"
const ZkPathVersion = "VERSION"
const ZkPathInitLock = "LOCK_INIT"

type ZkPath string

func (prefix ZkPath) root() string {
	return string(prefix)
}

func (prefix ZkPath) registerCenter() string {
	return string(prefix) + "/" + ZkPathRc
}

func (prefix ZkPath) registerCenterNode(nodeId string) string {
	return string(prefix) + "/" + ZkPathRc + "/" + nodeId
}

func (prefix ZkPath) broadCast() string {
	return string(prefix) + "/" + ZkPathBc
}

func (prefix ZkPath) response() string {
	return string(prefix) + "/" + ZkPathRe
}

func (prefix ZkPath) responseNode(nodeId string) string {
	return string(prefix) + "/" + ZkPathRe + "/" + nodeId
}

func (prefix ZkPath) coordinatorLock() string {
	return string(prefix) + "/" + ZkPathCoorLock
}

func (prefix ZkPath) version() string {
	return string(prefix) + "/" + ZkPathVersion
}

func (prefix ZkPath) coordinatorInitLock() string {
	return string(prefix) + "/" + ZkPathInitLock
}
