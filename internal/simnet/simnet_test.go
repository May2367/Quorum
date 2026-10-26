package simnet

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestSeededRaftSafetyManySeeds(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			cluster := NewSeeded(seed, 1, 2, 3)
			rng := rand.New(rand.NewSource(seed))
			for step := 0; step < 250; step++ {
				switch rng.Intn(10) {
				case 0, 1, 2:
					cluster.Tick(uint64(rng.Intn(3) + 1))
				case 3, 4, 5:
					cluster.DeliverRandom()
				case 6:
					if leader, ok := cluster.Leader(); ok {
						cluster.Propose(leader, []byte(fmt.Sprintf("seed=%d step=%d", seed, step)))
					}
				case 7:
					cluster.DropRandomQueued()
				case 8:
					partitionRandomPair(cluster, rng)
				case 9:
					if rng.Intn(3) == 0 {
						id := uint64(rng.Intn(3) + 1)
						cluster.Crash(id)
						cluster.Restart(id)
					} else {
						cluster.HealAll()
					}
				}
				if err := cluster.CheckInvariants(); err != nil {
					t.Fatalf("step %d: %v", step, err)
				}
			}
			cluster.HealAll()
			for i := 0; i < 50; i++ {
				cluster.TickAll()
				cluster.DeliverRandom()
			}
			if err := cluster.CheckInvariants(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFigureEightStyleLeaderChurnSafety(t *testing.T) {
	cluster := NewSeeded(8, 1, 2, 3, 4, 5)
	for round := 0; round < 8; round++ {
		cluster.HealAll()
		for i := 0; i < 15; i++ {
			cluster.TickAll()
			cluster.DeliverRandom()
		}
		if leader, ok := cluster.Leader(); ok {
			cluster.Propose(leader, []byte(fmt.Sprintf("round=%d leader=%d", round, leader)))
		}
		cluster.Drain()
		if err := cluster.CheckInvariants(); err != nil {
			t.Fatalf("round %d before partition: %v", round, err)
		}

		if round%2 == 0 {
			cluster.PartitionGroups([]uint64{1, 2}, []uint64{3, 4, 5})
		} else {
			cluster.PartitionGroups([]uint64{1, 3, 5}, []uint64{2, 4})
		}
		for i := 0; i < 20; i++ {
			cluster.TickAll()
			cluster.DeliverRandom()
			if leader, ok := cluster.Leader(); ok && i%5 == 0 {
				cluster.Propose(leader, []byte(fmt.Sprintf("partition round=%d i=%d", round, i)))
			}
		}
		if err := cluster.CheckInvariants(); err != nil {
			t.Fatalf("round %d partitioned: %v", round, err)
		}
	}

	cluster.HealAll()
	for i := 0; i < 100; i++ {
		cluster.TickAll()
		cluster.DeliverRandom()
	}
	if err := cluster.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func partitionRandomPair(cluster *Cluster, rng *rand.Rand) {
	a := uint64(rng.Intn(3) + 1)
	b := uint64(rng.Intn(3) + 1)
	if a == b {
		return
	}
	cluster.Partition(a, b)
}
