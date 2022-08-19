package main

import (
	"context"
	"datanode/DNServer"
	"flag"
	"fmt"
	"os"

	"github.com/apache/thrift/lib/go/thrift"
)

func Usage() {
	fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], ":\n")
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, "\n")
}

//定义服务
type Server struct {
}

// Parameters:
//  - Chunk
func (this *Server) PutChunk(ctx context.Context, chunk *DNServer.Chunk) (_r *DNServer.Resp, _err error) {
	fmt.Println(chunk)
	info := "adsfa"
	resp := &DNServer.Resp{
		Info: &info,
	}
	return resp, nil
}

// Parameters:
//  - ID
func (this *Server) GetChunk(ctx context.Context, id string) (_r *DNServer.Resp, _err error) {
	resp := &DNServer.Resp{}
	return resp, nil
}
func main() {
	//命令行参数
	flag.Usage = Usage
	protocol := flag.String("P", "binary", "Specify the protocol (binary, compact, json, simplejson)")
	framed := flag.Bool("framed", false, "Use framed transport")
	buffered := flag.Bool("buffered", false, "Use buffered transport")
	addr := flag.String("addr", "localhost:9091", "Address to listen to")

	flag.Parse()

	//protocol
	var protocolFactory thrift.TProtocolFactory
	switch *protocol {
	case "compact":
		protocolFactory = thrift.NewTCompactProtocolFactory()
	case "simplejson":
		protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
	case "json":
		protocolFactory = thrift.NewTJSONProtocolFactory()
	case "binary", "":
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	default:
		fmt.Fprint(os.Stderr, "Invalid protocol specified", protocol, "\n")
		Usage()
		os.Exit(1)
	}

	//buffered
	var transportFactory thrift.TTransportFactory
	if *buffered {
		transportFactory = thrift.NewTBufferedTransportFactory(8192)
	} else {
		transportFactory = thrift.NewTTransportFactory()
	}

	//framed
	if *framed {
		transportFactory = thrift.NewTFramedTransportFactory(transportFactory)
	}

	//handler
	handler := &Server{}

	//transport,no secure
	var err error
	var transport thrift.TServerTransport
	transport, err = thrift.NewTServerSocket(*addr)
	if err != nil {
		fmt.Println("error running server:", err)
	}

	//client processor

	clientProcessor := DNServer.NewServerProcessor(handler)
	// processor2 := NNServer.NewDNServiceProcessor()

	fmt.Println("Starting the namenode server... on ", *addr)

	//start tcp server
	server := thrift.NewTSimpleServer4(clientProcessor, transport, transportFactory, protocolFactory)
	err = server.Serve()

	if err != nil {
		fmt.Println("error running server:", err)
	}
}
