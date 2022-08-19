namespace go DNServer

include "File.thrift"
include "Msg.thrift"

//1. 维护DN的信息
struct DN {
    1: string ip;
    2: string port;
    3: string name;
    4: i64 StorageTotal;
    5: i64 StorageAvail;
}

//1.给Client提供的服务
service ClientServer {
    Msg.Resp putChunk(1: File.Chunk chunk)
    Msg.Resp getChunk(1: string id)
}