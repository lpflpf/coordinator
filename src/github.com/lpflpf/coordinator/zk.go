package coordinator

const ZK_PATH_RC = "REGISTER_CENTER"
const ZK_PATH_BC = "BROADCAST"
const ZK_PATH_RE = "RESPONSE"
const ZK_PATH_COOR = "LOCK_COORDINATOR"
const ZK_PATH_VERSION = "VERSION"

type ZK_PATH string

func (prefix ZK_PATH) registerCenter() string {
	return string(prefix) + "/" + ZK_PATH_RC
}

func (prefix ZK_PATH) broadCast() string {
	return string(prefix) + "/" + ZK_PATH_BC
}

func (prefix ZK_PATH) response() string {
	return string(prefix) + "/" + ZK_PATH_RE
}

func (prefix ZK_PATH) responseNode(nodeId string) string {
	return string(prefix) + "/" + ZK_PATH_RE + "/" + nodeId
}

func (prefix ZK_PATH) coordinatorLock() string {
	return string(prefix) + "/" + ZK_PATH_COOR
}

func (prefix ZK_PATH) version() string {
	return string(prefix) + "/" + ZK_PATH_VERSION
}
