package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"namenode/DNServer"
	"namenode/NNServer"
	"os"

	"github.com/apache/thrift/lib/go/thrift"
	uuid "github.com/satori/go.uuid"
)

func Usage() {
	fmt.Fprint(os.Stderr, "Usage of ", os.Args[0], ":\n")
	flag.PrintDefaults()
	fmt.Fprint(os.Stderr, "\n")
}

type DN struct {
	addr         string
	port         string
	name         string
	StorageTotal int
	StorageAvail int
}

//定义服务
type ClientServer struct {
	chunkSize int //每个chunk的大小：KB
	DNlist    []*DN
}

func (this *ClientServer) initConf() {
	this.chunkSize = 128
}
func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

//实现IDL里定义的接口
func (this *ClientServer) PutFile(ctx context.Context, remoteFile *DNServer.File) (_r []*NNServer.ChunkInfo, _err error) {
	avail_chunks := make([]*NNServer.ChunkInfo, 0)

	needSize := int(remoteFile.Size)
	for i := 0; i < len(this.DNlist); i++ {
		n := min(needSize, int(this.chunkSize))
		if n > 0 && this.DNlist[i].StorageAvail >= n {
			avail_chunks = append(avail_chunks, &NNServer.ChunkInfo{
				ID:   uuid.NewV4().String(),
				Addr: this.DNlist[i].addr,
				Port: this.DNlist[i].port,
			})
		}
	}
	if needSize > 0 {
		return make([]*NNServer.ChunkInfo, 0), errors.New("Not Enough Space")
	}

	return avail_chunks, nil
}
func (this *ClientServer) GetFile(ctx context.Context, remoteFile *DNServer.File) (_r []*NNServer.ChunkInfo, _err error) {
	return make([]*NNServer.ChunkInfo, 0), nil
}

// Parameters:
//  - RemoteFile
func (this *ClientServer) Stat(ctx context.Context, remoteFile *DNServer.File) (_r *DNServer.File, _err error) {
	file := &DNServer.File{
		FileName: "asdf",
		Size:     12312,
	}
	return file, nil
}

// Parameters:
//  - Path
func (this *ClientServer) DeleteFile(ctx context.Context, Path string) (_r *DNServer.Resp, _err error) {
	resp := DNServer.Resp{}
	return &resp, nil
}

// Parameters:
//  - OldName
//  - NewName_
func (this *ClientServer) RenameFile(ctx context.Context, oldName string, newName string) (_r *DNServer.Resp, _err error) {
	resp := DNServer.Resp{}
	return &resp, nil
}

// Parameters:
//  - Path
func (this *ClientServer) Mkdir(ctx context.Context, path string) (_r *DNServer.Resp, _err error) {
	resp := DNServer.Resp{}
	return &resp, nil
}

// Parameters:
//  - Path
func (this *ClientServer) List(ctx context.Context, path string) (_r *NNServer.Node, _err error) {
	resp := NNServer.Node{}
	return &resp, nil
}

func main() {
	//命令行参数
	flag.Usage = Usage
	protocol := flag.String("P", "binary", "Specify the protocol (binary, compact, json, simplejson)")
	framed := flag.Bool("framed", false, "Use framed transport")
	buffered := flag.Bool("buffered", false, "Use buffered transport")
	addr := flag.String("addr", "localhost:9090", "Address to listen to")

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
	handler := &ClientServer{}
	handler.initConf()

	//transport,no secure
	var err error
	var transport thrift.TServerTransport
	transport, err = thrift.NewTServerSocket(*addr)
	if err != nil {
		fmt.Println("error running server:", err)
	}

	//processor
	processor := NNServer.NewClientServerProcessor(handler)

	fmt.Println("Starting the simple server... on ", *addr)

	//start tcp server
	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)
	err = server.Serve()

	if err != nil {
		fmt.Println("error running server:", err)
	}
}
