package thrift

import (
	"sync"
)

type PoolHbaseClient struct {
	HbaseClient
	mu       sync.RWMutex
	h        *hbaseClientPool
	unusable bool
}

func (p *PoolHbaseClient) Close() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.unusable {
		if p.trans.Conn() != nil {
			return p.trans.Conn().Close()
		}
		return nil
	}
	return p.h.put(p.HbaseClient)
}

func (p *PoolHbaseClient) MarkUnusable() {
	p.mu.Lock()
	p.unusable = true
	p.mu.Unlock()
}

func (h *hbaseClientPool) wrapConn(conn HbaseClient) HbaseClient {
	p := &PoolHbaseClient{h: h}
	p.HbaseClient = conn
	return p.HbaseClient
}
