package coordinator

import "testing"

func TestSimpleStrategy_ReBalance(t *testing.T) {
	var simpleStrategy SimpleStrategy
	dataset := []struct {
		Input struct {
			sharding SimpleSharding
			nodeIds  []string
		}
		Expect SimpleSharding
	}{
		{
			Input: struct {
				sharding SimpleSharding
				nodeIds  []string
			}{
				sharding: SimpleSharding(map[string][]int64{"node1": {1, 2, 3, 4, 5, 6, 7, 8, 9, 0}}),
				nodeIds:  []string{"node1", "node2", "node3", "node4"},
			},
			Expect: SimpleSharding(map[string][]int64{"node1": {1, 5, 9}, "node2": {2, 6, 0}, "node3": {3, 7}, "node4": {4, 8}}),
		},
		{
			Input: struct {
				sharding SimpleSharding
				nodeIds  []string
			}{
				sharding: map[string][]int64{"node1": {1, 5, 9}, "node2": {2, 6, 0}, "node3": {3, 7}, "node4": {4, 8}},
				nodeIds:  []string{"node1"},
			},
			Expect: SimpleSharding(map[string][]int64{"node1": {1, 2, 3, 4, 5, 6, 7, 8, 9, 0}}),
		},
	}

	for _, data := range dataset {
		result := simpleStrategy.ReBalance(data.Input.sharding, data.Input.nodeIds)

		if compareSimpleSharding(result.(SimpleSharding), data.Expect) {
			t.Logf("rebalance ok, input %v, %v, output: %v", data.Input.sharding, data.Input.nodeIds, data.Expect)
		} else {
			t.Errorf("rebalance failed, input %v, %v, output: %v, result: %v", data.Input.sharding, data.Input.nodeIds, data.Expect, result.(SimpleSharding))
		}
	}
}
