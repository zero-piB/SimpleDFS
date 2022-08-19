package main

import (
	"context"
	"fmt"
	"namenode/DNServer"
	"namenode/NNServer"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
)

var ctx = context.Background()

func GetClient(addr string) *NNServer.ClientServerClient {
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

	client := NNServer.NewClientServerClient(thrift.NewTStandardClient(iprot, oprot))
	return client
}

//PutFile
func TestPutFile(t *testing.T) {
	addr := "localhost:9090"
	// conf := &thrift.TConfiguration{
	// 	ConnectTimeout: time.Second, // Use 0 for no timeout
	// 	SocketTimeout:  time.Second, // Use 0 for no timeout
	// }
	client := GetClient(addr)
	file := &DNServer.File{}
	file.FileName = "thrift"
	file.Size = 23
	rep, err := client.PutFile(ctx, file)
	if err != nil {
		t.Errorf("thrift err: %v\n", err)
	} else {
		t.Logf("Recevied: %v\n", rep)
	}
}

//Stat
func TestStat(t *testing.T) {
	addr := "localhost:9090"
	// conf := &thrift.TConfiguration{
	// 	ConnectTimeout: time.Second, // Use 0 for no timeout
	// 	SocketTimeout:  time.Second, // Use 0 for no timeout
	// }
	client := GetClient(addr)
	file := &DNServer.File{}
	file.FileName = "thrift"
	file.Size = 23

	rep, err := client.Stat(ctx, file)
	if err != nil {
		t.Errorf("thrift err: %v\n", err)
	} else {
		t.Logf("Recevied: %v\n", rep)
	}
}
