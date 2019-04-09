package coordinator

type Strategy interface {
	ReBalance(sharding Sharding, currentLiveNodes []string) Sharding
}

type SimpleStrategy struct{}

func (strategy SimpleStrategy) ReBalance(sharding Sharding, currentLiveNodes []string) Sharding {
	simpleSharding := sharding.(SimpleSharding)

	//if len(currentLiveNodes) == len(simpleSharding) {
	//	return sharding
	//}

	newSimpleSharding := SimpleSharding{}
	for _, node := range currentLiveNodes {
		newSimpleSharding[node] = []int64{}
	}

	var allJobs []int64
	for _, jobs := range simpleSharding {
		allJobs = append(allJobs, jobs...)
	}

	iter := 0

	Logger.Println("current Job: ", allJobs)
	for _, job := range allJobs {
		iter = (iter) % len(currentLiveNodes)
		node := currentLiveNodes[iter]
		newSimpleSharding[node] = append(newSimpleSharding[node], job)
		iter++
	}

	Logger.Println("NEW SHARDING: ", newSimpleSharding)
	return newSimpleSharding
}
