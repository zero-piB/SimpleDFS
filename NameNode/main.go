package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"namenode/NNServer"
	"os"
	"sync"

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
	id           int
	StorageTotal int
	StorageAvail int
}

//定义服务
type Server struct {
	chunkSize int //每个chunk的大小：KB

	//1.可用DataNode的列表
	dn_lock sync.Locker
	DNlist  []*DN
}

func (this *Server) initConf() {
	this.chunkSize = 128
}

//putFile
func (this *Server) PutFile(ctx context.Context, remoteFile *NNServer.File) (_r []*NNServer.ChunkInfo, _err error) {
	avail_chunks := make([]*NNServer.ChunkInfo, 0)
	avail_id := make([]int, 0)
	needSize := int(remoteFile.Size)

	this.dn_lock.Lock()
	defer this.dn_lock.Unlock()

	for i := 0; i < len(this.DNlist); i++ {
		n := min(needSize, int(this.chunkSize))
		if n > 0 && this.DNlist[i].StorageAvail >= n {
			avail_chunks = append(avail_chunks, &NNServer.ChunkInfo{
				ID:   uuid.NewV4().String(),
				Addr: this.DNlist[i].addr,
				Port: this.DNlist[i].port,
			})
			avail_id = append(avail_id, this.DNlist[i].id)
		}
	}
	if needSize > 0 {
		return make([]*NNServer.ChunkInfo, 0), errors.New("has not enough space")
	}
	//avail_id会减去已经分配的空间
	return avail_chunks, nil
}

func (this *Server) PutFileOk(ctx context.Context, file *NNServer.File, chunks []*NNServer.ChunkInfo) (_r *NNServer.Resp, _err error) {
	resp := NNServer.Resp{}
	return &resp, nil
}
func (this *Server) GetFile(ctx context.Context, remoteFile *NNServer.File) (_r []*NNServer.ChunkInfo, _err error) {
	return make([]*NNServer.ChunkInfo, 0), nil
}

// Parameters:
//  - RemoteFile
func (this *Server) Stat(ctx context.Context, remoteFile *NNServer.File) (_r *NNServer.File, _err error) {
	file := &NNServer.File{
		FileName: "asdf",
		Size:     12312,
	}
	return file, nil
}

// Parameters:
//  - Path
func (this *Server) DeleteFile(ctx context.Context, Path string) (_r *NNServer.Resp, _err error) {
	resp := NNServer.Resp{}
	return &resp, nil
}

// Parameters:
//  - OldName
//  - NewName_
func (this *Server) RenameFile(ctx context.Context, oldName string, newName string) (_r *NNServer.Resp, _err error) {
	resp := NNServer.Resp{}
	return &resp, nil
}

// Parameters:
//  - Path
func (this *Server) Mkdir(ctx context.Context, path string) (_r *NNServer.Resp, _err error) {
	resp := NNServer.Resp{}
	return &resp, nil
}

// Parameters:
//  - Path
func (this *Server) List(ctx context.Context, path string) (_r *NNServer.Node, _err error) {
	resp := NNServer.Node{}
	return &resp, nil
}

//DN server
func (this *Server) Register(ctx context.Context, dninfo *NNServer.DN) (_r *NNServer.Resp, _err error) {
	fmt.Println(dninfo)
	resp := NNServer.Resp{}
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
	handler := &Server{}
	handler.initConf()

	//transport,no secure
	var err error
	var transport thrift.TServerTransport
	transport, err = thrift.NewTServerSocket(*addr)
	if err != nil {
		fmt.Println("error running server:", err)
	}

	//client processor
	clientProcessor := NNServer.NewServerProcessor(handler)
	// processor2 := NNServer.NewDNServiceProcessor()

	fmt.Println("Starting the namenode server... on ", *addr)

	//start tcp server
	server := thrift.NewTSimpleServer4(clientProcessor, transport, transportFactory, protocolFactory)
	err = server.Serve()

	if err != nil {
		fmt.Println("error running server:", err)
	}
}
