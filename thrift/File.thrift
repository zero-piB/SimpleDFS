
namespace go NameNode

//1. 文件的元信息，参考Unix文件信息
struct File {
    1: required string fileName;
    2: required i32 size;
}


//2. 文件存放的基本单元
struct Chunk {
    1: string id;
    2: i32 size;
    3: i32 seq;  //文件的第几块
    4: binary data; //数据
}