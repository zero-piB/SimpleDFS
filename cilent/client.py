# -*- coding: gbk -*-
import NNServer.Server as NNServer
from NNServer.ttypes import Node, ChunkInfo
from File.ttypes import File

from DNServer import Server as DNServer
from Chunk.ttypes import Chunk
from Msg.ttypes import Resp

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from sys import stdin
import os

def stat(remote_path, nnclient):
    file = nnclient.Stat(remote_path)
    print("filename:%s" % file.fileName + ", filesize: %d" % file.size)

def delete(remote_path, nnclient):
    resp = nnclient.DeleteFile(remote_path)
    if resp.errCode == 200:
        print("%s is deleted" % remote_path)
    elif resp.errCode == 404:
        print(resp.errMsg)
    else:
        print("error message")

def rename(old_name, new_name, nnclient):
    resp = nnclient.RenameFile(old_name, new_name)
    if resp.errCode == 200:
        print("%s is renamed to %s", old_name, new_name)
    elif resp.errCode == 404:
        print(resp.errMsg)
    else:
        print("error message")

def mkdir(remote_path, nnclient):
    resp = nnclient.Mkdir(remote_path)
    if resp.errCode == 200:
        print("%s is created" % remote_path)
    elif resp.errCode == 404:
        print(resp.errMsg)
    else:
        print("error message")

def list(remote_path, nnclient):
    node = nnclient.List(remote_path)
    if node.isDir:
        print("name: %s," % node.name, "isDir: True")
    else:
        print("name: %s," % node.name, "isDir: False")

def putdir(dir_path, remote_path, nnclient):
    print("*****starting put %s*****" % dir_path)
    if os.path.isdir(dir_path):
        files = os.listdir(dir_path)  # �õ��ļ����µ������ļ�����
        # �ݹ鷢���ļ���������ļ�
        for file in files:
            if os.path.isdir(file):
                putdir(dir_path + "/" + file, remote_path, nnclient)
            else:
                putfile(dir_path + "/" + file, remote_path, nnclient)
    elif os.path.isfile(dir_path):
        putfile(dir_path, remote_path, nnclient)
    print("*****end putfile*****")

def putfile(file_path, remote_path, nnclient):
    # ��ȡ�ļ�
    try:
        with open(file_path, "rb") as file_obj:
            contents = file_obj.read()
        # ��NN������ȡDNλ�ú�Chunk��id
        name = file_path.split('/')[-1]  # �и���ȡ�ļ�����
        file = File(name, len(contents))
        list_chunkinfo = nnclient.PutFile(file)  # ��ȡChunkInfo
    except BaseException as message:
        print(message)
        print("fail put %s"%file_path)
        return

    print("-----begin put %s-----" % file_path)
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
        resp = DNclient.putChunk(onechunk)

        while resp.errCode == 404:
            print(resp.errMsg)
            resp = DNclient.putChunk(onechunk)

        print("chunk-%s is put successfully" % x.id)
        transport.close()

    # �������,��NN���淢�����
    resp = nnclient.PutFileOk(file, list_chunkinfo)
    print("-----finish put %s-----" % file_path)

def getfile(remote_path, loc_path, nnclient):
    # ��NN������ȡDNλ�ú�Chunk��id
    list_chunkinfo = nnclient.GetFile(remote_path)
    # ��DN��������Chunk

    # �ж��ļ��Ƿ����
    if os.path.exists(loc_path):
        print("error:local file has existed")
        return

    # �ж��ļ��ĸ�Ŀ¼�Ƿ����
    try:
        with open(loc_path, "wb") as file_obj:
            pass
    except FileNotFoundError:
        name = loc_path.split('/')[-1]
        dir = loc_path.rsplit(name)
        try:
            os.makedirs(dir[0])
        except:
            print("fail create directory")

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
        try:
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
                if not os.path.isdir(path1) and not os.path.isfile(path1):
                    print("file is not exit")
                elif list2[0] == "put":
                    putdir(path1, path2, client)
                elif list2[0] == "get":
                    getfile(path1, path2, client)
                elif list2[0] == "rename":
                    rename(path1, path2, client)
                else:
                    print("no such command")
            else:
                print("no such command")
            print(">>", end="")
        except BaseException as message:
            print(message)
            continue
    # Close!
    transport.close()

if __name__ == "__main__":
    main('127.0.0.1', 9090)
