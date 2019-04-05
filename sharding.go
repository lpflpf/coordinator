package coordinator

type Sharding interface {
	Encode() []byte
	Decode([]byte)
}
