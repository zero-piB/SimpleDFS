namespace go NNServer
include "File.thrift"
include "Msg.thrift"


//1. Node Unix文件系统的信息，是目录的逻辑结构的结点信息, 用于List接口
struct Node {
    1: string name;
    2: bool isDir;
}

//chunk存放所在的位置信息
struct ChunkInfo {
    1: string id;
    2: string addr;
    3: string port;
}

//2.给client提供的服务
service ClientServer {
    list<ChunkInfo> PutFile(1: File.File remoteFile)
    list<ChunkInfo> GetFile(1: File.File remoteFile)
    File.File Stat(1:File.File remoteFile)
    Msg.Resp DeleteFile(1: string Path)
    Msg.Resp RenameFile(1: string oldName, 2: string newName)
    Msg.Resp Mkdir(1: string path)
    Node List(1: string path)

}

//3. 给datanode提供的服务
service DNService {
    

}