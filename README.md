# Go hbase thrift

使用 [apache thrift](https://github.com/apache/thrift) 编译 [apache hbase](https://github.com/apache/hbase) 中的 [hbase-thrift](https://github.com/apache/hbase/tree/master/hbase-thrift)

具体可操作 API 可以查看 client 的方法，API 的使用与介绍需要查看 hbase 官网
项目中的 thrift 与 thrift2 方法有个别不同, 使用哪种取决于 hbase-thrift-server 是使用 thrift 还是 thrift2

```golang
package main

import (
	"context"
	"fmt"
	"log"
	"net"

	hbase "github.com/wangkai668/go-hbase/hbase-thrift"

	"git.apache.org/thrift.git/lib/go/thrift"
)

func main() {

	trans, err := thrift.NewTSocket(net.JoinHostPort("localhost", "9090"))
	if err != nil {
		log.Fatalln(err)
	}
	client := hbase.NewHbaseClientFactory(trans, thrift.NewTBinaryProtocolFactoryDefault())
	err = trans.Open()
	defer func() {
		err := trans.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}()
	if err != nil {
		log.Fatalln(err)
	}
	tables, err := client.GetTableNames(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	for _, v := range tables {
		fmt.Println(string(v))
	}
}

```