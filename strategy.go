package coordinator

import "sort"

// 策略是可以定制的
type Strategy interface {
	ReBalance(sharding Sharding, currentLiveNodes []string) Sharding
}

// 和SimpleSharding 对应的实现了一个基于SimpleSharding 的策略分配规则
type SimpleStrategy struct{}

func (strategy SimpleStrategy) ReBalance(sharding Sharding, currentLiveNodes []string) Sharding {
	simpleSharding := sharding.(*SimpleSharding)

	newSimpleSharding := SimpleSharding{
		Ver:  sharding.Version(),
		Data: map[string][]int{},
	}
	for _, node := range currentLiveNodes {
		newSimpleSharding.Data[node] = []int{}
	}

	var allJobs []int
	for _, jobs := range simpleSharding.Data {
		allJobs = append(allJobs, jobs...)
	}

	sort.Ints(allJobs)
	sort.Strings(currentLiveNodes)

	Logger.Println("COORDINATOR | CURRENT JOB: ", allJobs)
	min := len(allJobs) / len(currentLiveNodes)
	shift := 0
	for iter, node := range currentLiveNodes {
		if iter+1+len(currentLiveNodes)*min > len(allJobs) {
			newSimpleSharding.Data[node] = append(newSimpleSharding.Data[node], allJobs[shift:min+shift]...)
			shift += min
		} else {
			newSimpleSharding.Data[node] = append(newSimpleSharding.Data[node], allJobs[shift:min+shift+1]...)
			shift += min + 1
		}
	}

	Logger.Println("COORDINATOR | RE_BALANCE OLD SHARDING", sharding, " NEW SHARDING: ", newSimpleSharding)
	return &newSimpleSharding
}
