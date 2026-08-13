package shard

import (
	"database/sql"
	"testing"
)

func TestDBProviderRoutesToMatchingPool(t *testing.T) {
	r, err := NewRouter([]string{"shard-0", "shard-1"})
	if err != nil {
		t.Fatal(err)
	}
	p0, p1 := &sql.DB{}, &sql.DB{}
	p, err := NewDBProvider(r, map[string]*sql.DB{"shard-0": p0, "shard-1": p1})
	if err != nil {
		t.Fatal(err)
	}
	name, db, err := p.DBForUserID(2)
	if err != nil {
		t.Fatal(err)
	}
	if name != "shard-0" || db != p0 {
		t.Fatalf("route = %s/%p, want shard-0/%p", name, db, p0)
	}
}

func TestDBProviderRejectsIncompletePools(t *testing.T) {
	r, _ := NewRouter([]string{"shard-0", "shard-1"})
	if _, err := NewDBProvider(r, map[string]*sql.DB{"shard-0": &sql.DB{}}); err == nil {
		t.Fatal("incomplete pool set unexpectedly succeeded")
	}
}
