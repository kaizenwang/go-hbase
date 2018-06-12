package thrift

import (
	"testing"
)

func TestOpen(t *testing.T) {
	factory := Open("192.168.115.170", "9090")
	pool, err := NewHbaseClientPool(5, 30, factory)
	if err != nil {
		t.Error(err)
	}
	client, err := pool.Get()
	if err != nil {
		t.Error(err)
	}
	err = client.Put("ad_ssp_test", "12345678", map[string]string{
		"cf:name": "123456",
	})
	if err != nil {
		t.Error(err)
	}
	t.Logf("Len: %v\n", pool.Len())
}
