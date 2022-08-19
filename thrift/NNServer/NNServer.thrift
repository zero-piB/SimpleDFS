namespace go NNServer
include "File.thrift"
include "Msg.thrift"


//1. Node Unix文件系统的信息，是目录的逻辑结构的结点信息, 用于List接口
struct Node {
    1: string name;
    2: bool isDir;
}

//chunk存放所在的位置信息, chunkId
struct ChunkInfo {
    1: string id;
    2: string addr;
    3: string port;
    4: i32 dnId;
    5: i32 chunkSize;
}

//2. 维护DN的信息
struct DN {
    1: string ip;
    2: string port;
    3: string name;
    4: i64 StorageTotal;
    5: i64 StorageAvail;
}


service Server {
    //1.给client提供的服务
    list<ChunkInfo> PutFile(1: File.File remoteFile)
    Msg.Resp PutFileOk(1: File.File file, 2: list<ChunkInfo> chunks)
    
    list<ChunkInfo> GetFile(1: File.File remoteFile)
    File.File Stat(1:File.File remoteFile)
    Msg.Resp DeleteFile(1: string Path)
    Msg.Resp RenameFile(1: string oldName, 2: string newName)
    Msg.Resp Mkdir(1: string path)
    Node List(1: string path)


    //2. 给datanode提供的服务
    Msg.Resp Register(1: DN dninfo)
    
}   

