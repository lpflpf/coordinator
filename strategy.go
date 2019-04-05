package coordinator

type Strategy interface {
	ReBalance(sharding Sharding, currentLiveNodes []string) Sharding
}
