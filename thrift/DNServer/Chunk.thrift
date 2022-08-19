namespace go DNServer
//1. 文件存放的基本单元
struct Chunk {
    1: string id;
    2: i32 size;
    3: i32 seq;  //文件的第几块
    4: binary data; //数据
}