namespace go DNServer

include "Chunk.thrift"
include "Msg.thrift"

//DN提供的服务
service Server {
    Msg.Resp putChunk(1: Chunk.Chunk chunk)
    Chunk.Chunk getChunk(1: string id)
}