namespace go DataNode

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

service ClientServer {
    
}