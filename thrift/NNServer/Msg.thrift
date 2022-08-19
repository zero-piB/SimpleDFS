namespace go NNServer
//rpc 报文的基本信息

struct Resp {
    1:required i32 errCode, //错误码
    2:required string errMsg, //错误信息
    3:optional string info,
}