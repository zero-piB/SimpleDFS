# -*- coding: gbk -*-
import NNServer.Server as NNServer
from NNServer.ttypes import Node, ChunkInfo
from File.ttypes import File

from DNServer import Server as DNServer
from Chunk.ttypes import Chunk

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from sys import stdin


def list(remote_path, nnclient):
    node = nnclient.List(remote_path)
    if node.isDir:
        print("name: %s," % node.name, "isDir: True")
    else:
        print("name: %s," % node.name, "isDir: False")


def delete(remote_path, nnclient):
    msg = nnclient.DeleteFile(remote_path)
    print("%s is deleted" % remote_path)


def stat(remote_path, nnclient):
    file = nnclient.Stat(remote_path)
    print("filename:%s" % file.fileName + ", filesize: %d" % file.size)


def mkdir(remote_path, nnclient):
    msg = nnclient.Mkdir(remote_path)
    print("%s is created" % remote_path)


def putfile(loc_path, remote_path, nnclient):
    # ��ȡ�ļ�
    with open(loc_path, "rb") as file_obj:
        contents = file_obj.read()
    # ��NN������ȡDNλ�ú�Chunk��id
    name = loc_path.split('\\')[-1]  # �и���ȡ�ļ�����
    file = File(name, len(contents))
    list_chunkinfo = nnclient.PutFile(file)  # ��ȡChunkInfo

    # ��DN���������ļ���Ƭ��Chunk
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

        content = contents[cnt: cnt + x.chunkSize]
        cnt += x.chunkSize
        onechunk = Chunk(x.id, x.chunkSize, x.dnId, content)
        msg = DNclient.putChunk(onechunk)

        while False:
            msg = DNclient.putChunk(onechunk)

        transport.close()

    # �������,��NN���淢�����
    msg = nnclient.PutFileOk(file, list_chunkinfo)
    print("putfile is finished successfully")


def getfile(remote_path, loc_path, nnclient):
    # ��NN������ȡDNλ�ú�Chunk��id
    list_chunkinfo = nnclient.GetFile(remote_path)
    # ��DN��������Chunk

    with open(loc_path, "wb") as file_obj:
        for x in list_chunkinfo:
            transport = TSocket.TSocket(x.addr, x.port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            DNclient = DNServer.Client(protocol)
            transport.open()

            contend = DNclient.getChunk(x.id)

            transport.close()
            # д���ļ�
            file_obj.write(contend.data)
            pass
    print("getfile is finished successfully")


def rename(old_name, new_name, nnclient):
    msg = nnclient.RenameFile(old_name, new_name)
    print("%s is renamed to %s", old_name, new_name)


def main(ip, port):
    # Make socket
    transport = TSocket.TSocket(ip, port)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = NNServer.Client(protocol)

    # Connect!
    transport.open()
    print("start client ...")
    print(">>", end="")
    for line in stdin:
        # ʶ������
        list1 = line.split(" ")
        list2 = [x.strip() for x in list1 if x.strip() != '']

        if len(list2) == 0:
            print("command shouldn't be null")
        # ����Ϊexitʱ�˳�
        elif len(list2) == 1:
            if list2[0] == "exit":
                print("client closed")
                break
            else:
                print("no such command")
        # �����һ������
        elif len(list2) == 2:
            path1 = list2[1]
            if list2[0] == "delete":
                delete(path1, client)
            elif list2[0] == "stat":
                stat(path1, client)
            elif list2[0] == "mkdir":
                mkdir(path1, client)
            elif list2[0] == "list":
                list(path1, client)
            else:
                print("no such command")
        # �������������
        elif len(list2) == 3:
            path1 = list2[1]
            path2 = list2[2]
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
        print(">>", end="")
    # Close!
    transport.close()


if __name__ == "__main__":
    main('127.0.0.1', 9090)
