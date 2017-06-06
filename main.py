#!/usr/bin/env python3
# coding:utf-8

'''
title:      抓取斗鱼弹幕
create:     2017-5-8
modified:   2017-5-29
'''

__author__ = 'zzzzer'

import re
import socket
import struct
import time
from datetime import datetime
from threading import Thread

import requests
import pymongo


def get_room_info(uid):
    '''根据主播的uid（url上的），获取纯数字的room_id和主播中文名'''

    url = 'http://www.douyu.com/{}'.format(uid)
    r = requests.get(url)
    pattern = re.compile(r'"room_id":(\d+)')
    room_id = pattern.findall(r.text)[0]
    pattern = re.compile(r'<span class="nn fl">(.+?)</span>')
    name = pattern.findall(r.text)[0]
    return room_id, name


def send_msg(msg):
    '''发给斗鱼服务器所有的包都得加上消息头，格式见斗鱼弹幕手册'''

    content = msg.encode()
    # 消息长度，这里加8而不是加12。所以实际tcp数据部分长度会比斗鱼协议头长度大4
    length = len(content) + 8
    # 689代表客户端向服务器发送的数据，690代表服务器向客户端发送的数据
    code = 689
    head = struct.pack('i', length) + struct.pack('i', length) + struct.pack('i', code)
    sk_client.sendall(head + content)


def get_dm(room_id):
    '''接受服务器消息，并提取弹幕信息'''

    with pymongo.MongoClient('192.168.47.1') as ct:
        db = ct['douyu']
        tb = db[room_id]

        pattern = re.compile(b'type@=chatmsg/.+?/nn@=(.+?)/txt@=(.+?)/.+?/level@=(.+?)/.+?/\x00')
        while True:
            # 接收的包有可能被分割，需要把它们重新合并起来，不然信息可能会缺失
            buffer = b''
            while True:
                recv_data = sk_client.recv(1024)
                buffer += recv_data
                if recv_data.endswith(b'\x00'):
                    break
            now = datetime.now()
            for nn, txt, level in pattern.findall(buffer):
                try:
                    print("[lv.{:0<2}][{}]: {}".format(level.decode(), nn.decode(), txt.decode()))
                    item = {
                        'time': now,
                        'level': level.decode(),
                        'nn': nn.decode(),
                        'txt': txt.decode(),
                    }
                    tb.insert_one(item)
                except UnicodeDecodeError:
                    # 斗鱼有些表情会引发unicode编码错误
                    pass


def keep_live():
    '''每隔40s发送心跳包'''

    while True:
        time.sleep(40)
        msg_keep = 'type@=mrkl/\x00'
        send_msg(msg_keep)


def main():
    uid = input('请输入主播uid：')
    room_id, name = get_room_info(uid)
    print('你进入了{}的直播间，房间id是{}'.format(name, room_id))

    global sk_client
    sk_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # 这个服务器是我测试正常的 2017.5.28
    # 有些服务器很奇怪，虽然能接收弹幕，但wireshark分析，会出现一大堆TCP Retransmission
    # 并且接收了一段时间后就会停止接收，即使发了心跳包
    hosts = '223.99.254.250'
    port = 12601
    sk_client.connect((hosts, port))

    # loginreq中的req指requests 还需要紧接着发下面一个包，服务器会返回loginres
    msg_login = 'type@=loginreq/username@=/password@=/roomid@={}/\x00'.format(room_id)
    send_msg(msg_login)
    # 直觉认为这里暂停一秒比较好。
    time.sleep(1)
    # gid=-9999 代表接收海量弹幕，发完下面这个包，服务器才会向客户端发送弹幕
    msg_join = 'type@=joingroup/rid@={}/gid@=-9999/\x00'.format(room_id)
    send_msg(msg_join)

    # 这里用多线程而不使用多进程的原因是，用wrieshark分析时发现，多进程会造成3次“三次握手”
    # 貌似是全局变量的原因
    t1 = Thread(target=get_dm, args=(room_id,))
    t2 = Thread(target=keep_live)
    t1.start()
    t2.start()


if __name__ == '__main__':
    main()
