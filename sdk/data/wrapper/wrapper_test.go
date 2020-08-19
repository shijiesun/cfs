package wrapper

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func swap(s []*DataPartition, i int, j int) {
	s[i], s[j] = s[j], s[i]
}

func partByPrivot(partitions []*DataPartition, low, high int) int {
	var i, j int
	for {
		for i = low + 1; i < high; i++ {
			if partitions[i].GetAvgWrite() > partitions[low].GetAvgWrite() {
				break
			}
		}
		for j = high; j > low; j-- {
			if partitions[j].GetAvgWrite() <= partitions[low].GetAvgWrite() {
				break
			}
		}
		if i >= j {
			break
		}
		swap(partitions, i, j)
	}
	if low != j {
		swap(partitions, low, j)
	}
	return j
}

func selectKminDataPartition(partitions []*DataPartition, k int) int {
	low, high := 0, len(partitions) - 1
	for {
		privot := partByPrivot(partitions, low, high)
		if privot < k {
			low = privot + 1
		} else if privot > k {
			high = privot - 1
		} else {
			return k
		}
	}
}

func TestKmin(t *testing.T) {
	partitions := make([]*DataPartition, 0)

	for i := 0; i <= 50; i++ {
		rand.Seed(time.Now().UnixNano())
		i := rand.Int63n(100)

		dp := new(DataPartition)
		dp.Metrics = new(DataPartitionMetrics)
		dp.Metrics.AvgWriteLatencyNano = i
		partitions = append(partitions, dp)
	}

	kth := selectKminDataPartition(partitions, 40)

	for _, v := range partitions[:kth] {
		fmt.Printf("%v ", v.GetAvgWrite())
	}
	fmt.Println()

	for _, v := range partitions[kth:len(partitions)] {
		fmt.Printf("%v ", v.GetAvgWrite())
	}
	fmt.Println()
}