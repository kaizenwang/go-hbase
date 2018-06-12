package thrift

import (
	"context"
	"errors"
	"net"

	hb "github.com/wangkai668/go-hbase/hbase-thrift"

	"git.apache.org/thrift.git/lib/go/thrift"
)

var defaultCtx = context.Background()

type HbaseClient struct {
	*hb.HbaseClient
	trans *thrift.TSocket
}

func Open(host, port string) Factory {
	return func() (HbaseClient, error) {
		ret := HbaseClient{}
		trans, err := thrift.NewTSocket(net.JoinHostPort(host, port))
		if err != nil {
			return ret, err
		}
		client := hb.NewHbaseClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
		err = trans.Open()
		ret.HbaseClient = client
		ret.trans = trans
		return ret, err
	}
}

func (h *HbaseClient) Put(tableName, rowKey string, data map[string]string) error {
	if len(tableName) <= 0 || len(rowKey) <= 0 {
		return errors.New("tableName or rowKey is nil")
	}
	if len(data) <= 0 {
		return errors.New("data is nil")
	}
	columns := []hb.Text{}
	values := []hb.Text{}
	for k, v := range data {
		columns = append(columns, []byte(k))
		values = append(values, []byte(v))
	}
	_, err := h.Append(defaultCtx, &hb.TAppend{
		Table:   []byte(tableName),
		Row:     []byte(rowKey),
		Columns: columns,
		Values:  values,
	})
	return err
}
