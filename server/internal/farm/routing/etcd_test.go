package routing

import (
	"testing"

	"github.com/photon/farm-server/server/pkg/discovery"
)

func TestBucketForStableAndBounded(t *testing.T) {
	for _, farmID := range []int64{1, 2, 99, 1<<62 - 1} {
		first := BucketFor(farmID, 1024)
		if first < 0 || first >= 1024 {
			t.Fatalf("farm %d bucket=%d", farmID, first)
		}
		if got := BucketFor(farmID, 1024); got != first {
			t.Fatalf("unstable bucket: %d != %d", got, first)
		}
	}
}

func TestDesiredOwnerDeterministicAndDistributes(t *testing.T) {
	instances := []discovery.Endpoint{{InstanceID: "farm-a"}, {InstanceID: "farm-b"}, {InstanceID: "farm-c"}}
	counts := map[string]int{}
	for bucket := 0; bucket < 1024; bucket++ {
		one := desiredOwner(bucket, instances)
		two := desiredOwner(bucket, []discovery.Endpoint{instances[2], instances[0], instances[1]})
		if one.InstanceID != two.InstanceID {
			t.Fatalf("bucket %d changed with input order", bucket)
		}
		counts[one.InstanceID]++
	}
	for _, instance := range instances {
		if counts[instance.InstanceID] < 250 {
			t.Fatalf("poor distribution: %#v", counts)
		}
	}
}

type fixedResolver struct {
	route Route
	err   error
}

func (r fixedResolver) Resolve(int64) (Route, error) { return r.route, r.err }
