namespace go NameNode
include "NNFile.thrift"
include "Msg.thrift"

struct FileResp {
    1: required i32 errCode; 
    2: required string errMsg; 
    3: NNFile.FileChunks FileInfo; 
}

//1. Node Unix文件系统的信息，是目录的逻辑结构的结点信息, 用于List接口
struct Node {
    1: string name;
    2: bool isDir;
}

//1.给client提供的服务
service ClientServer {
    FileResp PutFile(1: NNFile.File remoteFile)
    FileResp GetFile(1: NNFile.File remoteFile)
    string Stat(1:NNFile.File remoteFile)
    Msg.Resp DeleteFile(1: NNFile.File remoteFile)
    Msg.Resp RenameFile(1: NNFile.File remoteFile)
    Msg.Resp Mkdir(1: string path)
    NNFile.Node List(1: string path)
}

//2. 给datanode提供的服务
service DNService {
    
}