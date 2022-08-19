# -*- coding: gbk -*-
from NNServer import Server as NNServer
from NNServer.ttypes import Node
from NNServer.File.ttypes import File

from DNServer import ClientServer as DNServer
from DNServer.Chunk.ttypes import Chunk

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from sys import stdin


def list(remote_path, NNclient):
    node = NNclient.List(remote_path)
    print(node.name, end=" ")
    if node.isDir:
        print("")
    else:
        print("")


def delete(remote_path, NNclient):
    NNclient.DeleteFile(remote_path);


def stat(remote_path, NNclient):
    NNclient.Stat(remote_path)


def mkdir(remote_path, NNclient):
    msg = NNclient.Mkdir(remote_path)


def putfile(loc_path, remote_path, NNclient):
    # 读取文件
    with open(loc_path, "rb") as file_obj:
        contents = file_obj.read()
    # 和NN交互获取DN位置和Chunk的id
    name = loc_path.split('\\')[-1]  # 切割后获取文件名称
    file = File(name, len(contents))
    list_chunkinfo = NNclient.PutFile(file)  # 获取ChunkInfo

    # 向DN交互发送文件分片的Chunk
    cnt = 0
    for x in list_chunkinfo:
        # Make socket
        transport = TSocket.TSocket(x.addr, x.port)
        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        # Create a client to use the protocol encoder
        DNclient = DNServer.Client(protocol)
        # Connect!
        transport.open()

        content = contents[cnt, cnt + x.chunkSize]
        cnt += x.chunkSize
        onechunk = Chunk(x.id, x.chunkSize, x.dnId, content)
        msg = DNclient.putChunk(onechunk)

        transport.close()

    # 发送完成,向NN报告发送完成
    msg = NNclient.PutFileOk(file, list_chunkinfo)


def getfile(remote_path, loc_path, NNclient):
    # 和NN交互获取DN位置和Chunk的id
    list_chunkinfo = NNclient.Get(remote_path)
    # 向DN交互接受Chunk
    for x in list_chunkinfo:
        # Make socket
        transport = TSocket.TSocket(x.addr, x.port)
        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        # Create a client to use the protocol encoder
        DNclient = DNServer.Client(protocol)
        # Connect!
        transport.open()

        contend = DNclient.getChunk(x.id)

        transport.close()

        # 写入文件
        with open(loc_path, "wb") as file_obj:
            file_obj.write(contend)
        pass
    pass


def rename(old_name, new_name, NNclient):
    msg = NNclient.RenameFile(old_name, new_name)
    print("%s 成功改名为 %s", old_name, new_name)


def main():
    # Make socket
    transport = TSocket.TSocket('127.0.0.1', 9090)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = NNServer.Client(protocol)

    # Connect!
    transport.open()

    for line in stdin:
        # 识别命令
        list1 = line.split(" ")
        list2 = [x.strip() for x in list1 if x.strip() != '']

        if len(list2) == 0:
            print("command shouldn't be null")
        # 命令含有一个参数
        elif len(list2) == 2:
            path1 = "./" + list2[1];
            if list2[0] == "delete":
                delete(path1, client)
            elif list2[0] == "stat":
                stat(path1, client)
            elif list2[0] == "mkdir":
                list(path1, client);
            elif list2[0] == "list":
                mkdir(path1, client);
            else:
                print("no such command")
        # 命令含有两个参数
        elif len(list2) == 3:
            path1 = "./" + list2[1];
            path2 = "./" + list2[2];
            if list2[0] == "put":
                putfile(path1, path2, client)
            elif list2[0] == "get":
                getfile(path1, path2, client)
            elif list2[0] == "rename":
                rename(path1, path2, client)
            else:
                print("no such command")
        else:
            print("no such command")
    # Close!
    transport.close()


if __name__ == "__main__":
    main()
