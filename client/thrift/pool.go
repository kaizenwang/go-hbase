package thrift

import (
	"errors"
	"fmt"
	"sync"
)

var ErrClosed = errors.New("pool is closed")

type Pool interface {
	Get() (HbaseClient, error)

	Close()

	Len() int
}

type Factory func() (HbaseClient, error)

type hbaseClientPool struct {
	mu    sync.RWMutex
	conns chan HbaseClient

	factory Factory
}

func NewHbaseClientPool(initialCap, maxCap int, factory Factory) (Pool, error) {
	if initialCap < 0 || maxCap <= 0 || initialCap > maxCap {
		return nil, errors.New("invalid capacity settings")
	}

	h := &hbaseClientPool{
		conns:   make(chan HbaseClient, maxCap),
		factory: factory,
	}

	for i := 0; i < initialCap; i++ {
		conn, err := factory()
		if err != nil {
			return nil, fmt.Errorf("connection is not able not fill the pool: %s", err)
		}
		h.conns <- conn
	}

	return h, nil
}

func (h *hbaseClientPool) getConnsAndConnection() (chan HbaseClient, Factory) {
	h.mu.RLock()
	conns := h.conns
	connection := h.factory
	h.mu.RUnlock()
	return conns, connection
}

func (h *hbaseClientPool) Get() (HbaseClient, error) {
	var c HbaseClient
	conns, factory := h.getConnsAndConnection()
	if conns == nil {
		return c, ErrClosed
	}

	select {
	case conn := <-conns:
		if conn.trans.Conn() == nil {
			return c, ErrClosed
		}
		return h.wrapConn(conn), nil
	default:
		conn, err := factory()
		if err != nil {
			return c, err
		}
		return h.wrapConn(conn), nil
	}
}

func (h *hbaseClientPool) put(conn HbaseClient) error {
	if conn.trans.Conn() == nil {
		return errors.New("connection is nil, rejecting")
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.conns == nil {
		return nil
	}

	select {
	case h.conns <- conn:
		return nil
	default:
		return nil
	}
}

func (h *hbaseClientPool) Close() {
	h.mu.Lock()
	conns := h.conns
	h.conns = nil
	h.factory = nil
	h.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for conn := range conns {
		conn.trans.Conn().Close()
	}
}

func (h *hbaseClientPool) Len() int {
	conns, _ := h.getConnsAndConnection()
	return len(conns)
}
