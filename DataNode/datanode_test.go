package main

import (
	"context"
	"datanode/DNServer"
	"fmt"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
)

var ctx = context.Background()

func GetClient(addr string) *DNServer.ServerClient {
	var transport thrift.TTransport
	var err error
	transport, err = thrift.NewTSocket(addr)
	if err != nil {
		fmt.Println("Error opening socket:", err)
	}

	//protocol
	var protocolFactory thrift.TProtocolFactory
	protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()

	//no buffered
	var transportFactory thrift.TTransportFactory
	transportFactory = thrift.NewTTransportFactory()

	transport, err = transportFactory.GetTransport(transport)
	if err != nil {
		fmt.Println("error running client:", err)
	}

	if err := transport.Open(); err != nil {
		fmt.Println("error running client:", err)
	}

	iprot := protocolFactory.GetProtocol(transport)
	oprot := protocolFactory.GetProtocol(transport)

	client := DNServer.NewServerClient(thrift.NewTStandardClient(iprot, oprot))
	return client
}

func TestPutChunk(t *testing.T) {
	addr := "localhost:9091"
	// conf := &thrift.TConfiguration{
	// 	ConnectTimeout: time.Second, // Use 0 for no timeout
	// 	SocketTimeout:  time.Second, // Use 0 for no timeout
	// }
	client := GetClient(addr)
	chunk := &DNServer.Chunk{
		ID:   "asdf",
		Size: 32,
		Seq:  0,
		Data: make([]byte, 4),
	}
	rep, err := client.PutChunk(ctx, chunk)
	if err != nil {
		t.Errorf("thrift err: %v\n", err)
	} else {
		t.Logf("Recevied: %v\n", rep)
	}
}
