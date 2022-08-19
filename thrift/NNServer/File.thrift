
namespace go NNServer

//1. 文件的元信息，参考Unix文件信息
struct File {
    1: required string fileName;
    2: required i32 size;
}

