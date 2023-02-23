# type:ignore
# -*- coding: utf-8 -*-
# Copyright 2018 Martin Bammer. All Rights Reserved.
# Licensed under MIT license.
# cython: language_level=3, boundscheck=False

"""Network logging support."""

import gc
import socket
import struct

# c cdef gc
import time
from collections import deque
from threading import Event
from threading import Lock
from threading import Thread

import msgpack


class LoggingServer(Thread):  # pragma: no cover
    def __init__(
        self,
        logger,
        address,
        port,
        maxMsgSize=4096,
        cbAccept=None,
        cbAuth=None,
        cbDecode=None,
    ):
        super(LoggingServer, self).__init__(
            name="LoggingServer_%s:%d" % (address, port), daemon=True
        )
        self.logger = logger
        self.address = address
        self.port = port
        self.maxMsgSize = maxMsgSize
        self.cbAccept = cbAccept
        self.cbAuth = cbAuth
        self.cbDecode = cbDecode
        self.errors = deque(maxlen=10)
        self.__socket = None
        self.__clients = {}
        self.__lock = Lock()
        self.__running = True

    def __del__(self):
        self.stop()

    def stop(self):
        self.__running = False
        if self.__socket is not None:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.address, self.port))
            sock.close()
            self.__socket.close()
            self.__socket = None
        self.join()

    def run(self):
        # c cdef bint bAcceptFailed
        try:
            self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.__socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.__socket.bind((self.address, self.port))
            # noinspection PyArgumentList
            self.__socket.listen()
            bAcceptFailed = True
            cbAccept = self.cbAccept
            socket_accept = self.__socket.accept
            while self.__running:
                # noinspection PyBroadException
                try:
                    client, (address, port) = socket_accept()
                    if self.__running is False:
                        break
                    bAcceptFailed = False
                except:
                    if bAcceptFailed:
                        time.sleep(0.01)
                    else:
                        bAcceptFailed = True
                    continue
                if cbAccept is not None and not cbAccept(client, address):
                    client.close()
                    continue
                thrClient = Thread(
                    target=self.receiver,
                    args=(client, address),
                    name="LoggingReceiver%s" % address,
                    daemon=True,
                )
                thrClient.start()
                with self.__lock:
                    self.__clients[client] = thrClient
        except Exception as exc:
            self.errors.append(exc)
        for client, thrClient in tuple(self.__clients.items()):
            client.close()
            thrClient.join()
        self.__clients.clear()

    def receiver(self, client, addr):
        # c cdef Py_ssize_t wPos, rPos, rcvCnt, bufLen, bufLastPos, rcvMax, cnt, cnt2, size
        try:
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except (OSError, NameError):
            pass
        client_recv = client.recv_into
        buf = bytearray(self.maxMsgSize)
        view = memoryview(buf)  # p
        # c cdef unsigned char[:] view = buf
        if self.cbAuth is None or self.cbAuth(client, addr, client_recv(self.maxMsgSize)):
            # noinspection PyBroadException
            try:
                prefix = socket.gethostbyaddr(addr)[0] + ": "
            except:
                prefix = addr + ": "
            with client:
                # noinspection PyProtectedMember
                logMessage = self.logger._logMessage
                msgpack_unpackb = msgpack.unpackb
                time_time = time.time
                cbDecode = self.cbDecode
                t0 = None
                try:
                    wPos = 0
                    rPos = 0
                    rcvCnt = 0
                    bufLen = len(buf)
                    bufLastPos = bufLen - 1
                    rcvMax = bufLen
                    while self.__running:
                        if wPos + rcvMax > bufLen:
                            cnt = client_recv(view[wPos:], bufLen - wPos)
                            wPos = (wPos + cnt) % bufLen
                            rcvMax -= cnt
                            if (rcvMax > 0) and (bufLen - wPos == cnt):
                                cnt2 = client_recv(view, rcvMax)
                                wPos = (wPos + cnt2) % bufLen
                                rcvMax -= cnt2
                                cnt += cnt2
                        else:
                            cnt = client_recv(view[wPos:], rcvMax)
                            wPos = (wPos + cnt) % bufLen
                            rcvMax -= cnt
                        if t0 is None:
                            t0 = time_time()
                        if not cnt:
                            break
                        rcvCnt += cnt
                        if rcvCnt < 3:
                            continue
                        while True:
                            if rPos < bufLastPos:
                                size = (view[rPos] << 8) + view[rPos + 1] + 2
                            else:
                                size = (view[rPos] << 8) + view[0] + 2
                            if size > bufLen:
                                raise Exception("Received too big (%d) message frame!" % size)
                            if size > rcvCnt:
                                break
                            if rPos == bufLastPos:
                                messages = msgpack_unpackb(
                                    view[1 : size - bufLen + rPos], use_list=False
                                )
                            elif rPos + size > bufLastPos:
                                messages = msgpack_unpackb(
                                    buf[rPos + 2 :] + buf[: size - bufLen + rPos],
                                    use_list=False,
                                )
                            else:
                                messages = msgpack_unpackb(
                                    view[rPos + 2 : rPos + size], use_list=False
                                )
                            if isinstance(messages, tuple):
                                for message in messages:
                                    if cbDecode is None:
                                        (
                                            logtime,
                                            domain,
                                            level,
                                            message,
                                            kwargs,
                                        ) = msgpack_unpackb(message, use_list=False)
                                        logMessage(
                                            None,
                                            (
                                                logtime,
                                                str(domain, "utf-8"),
                                                level,
                                                prefix
                                                + str(message, "utf-8")
                                                + " %.3f" % (time_time() - t0),
                                                kwargs,
                                            ),
                                            0,
                                        )
                                    else:
                                        logMessage(None, cbDecode(prefix, message), 0)
                            elif isinstance(messages, bytes):
                                prefix = str(messages, "utf-8") + ": "
                            rPos = (rPos + size) % bufLen
                            rcvCnt -= size
                            rcvMax += size
                            if rcvCnt < 3:
                                break
                except Exception as exc:
                    self.errors.append(exc)
        with self.__lock:
            del self.__clients[client]


class LoggingClient(Thread):  # pragma: no cover
    def __init__(
        self,
        address,
        port,
        clientName=None,
        maxMsgSize=4096,
        cbConnect=None,
        cbEncode=None,
    ):
        super(LoggingClient, self).__init__(
            name="LoggingClient_%s:%d" % (address, port), daemon=True
        )
        self.address = address
        self.port = port
        self.clientName = clientName
        self.maxMsgSize = maxMsgSize
        self.queue = deque()
        self.evtQueue = Event()
        self.evtSent = Event()
        self.cbConnect = cbConnect
        self.cbEncode = cbEncode
        self.errors = deque(maxlen=10)
        self.__running = True
        self.__socket = None

    def __del__(self):
        self.stop()

    def stop(self):
        self.__running = False
        if self.__socket is not None:
            self.__socket.close()
        self.evtQueue.set()
        self.join()

    def log(self, entry):
        self.queue.append(entry)
        self.evtQueue.set()

    def run(self):
        # c cdef Py_ssize_t msgLen, totLen, maxMsgSize
        self.__socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.__socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except (OSError, NameError):
            pass
        try:
            maxMsgSize = self.maxMsgSize
            self.__socket.connect((self.address, self.port))
            if self.cbConnect is not None:
                if not self.cbConnect(self.__socket):
                    self.evtSent.set()
                    raise Exception("Failed to connect to %s:%d!" % (self.address, self.port))
            queue_popleft = self.queue.popleft
            sock_sendall = self.__socket.sendall
            msgpack_packb = msgpack.packb
            struct_pack = struct.pack
            cbEncode = self.cbEncode
            if self.clientName:
                data = msgpack_packb(self.clientName, use_bin_type=True)
                sock_sendall(struct_pack(">H", len(data)) + data)
            while self.__running:
                self.evtSent.clear()
                messages = []
                totLen = 0
                gc.disable()
                try:
                    while True:
                        if cbEncode is None:
                            message = msgpack_packb(queue_popleft(), use_bin_type=True)
                        else:
                            message = msgpack_packb(cbEncode(queue_popleft()), use_bin_type=True)
                        msgLen = len(message) + 3
                        if totLen + msgLen >= maxMsgSize:
                            if msgLen >= maxMsgSize:
                                self.errors.append(
                                    "Error: Message with size %d too big! Ignoring it!" % msgLen
                                )
                                continue
                            data = msgpack_packb(messages, use_bin_type=True)
                            sock_sendall(struct_pack(">H", len(data)) + data)
                            messages = []
                            totLen = 0
                        messages.append(message)
                        totLen += msgLen
                except IndexError:
                    pass
                except Exception as exc:
                    self.errors.append(exc)
                if messages:
                    data = msgpack_packb(messages, use_bin_type=True)
                    sock_sendall(struct_pack(">H", len(data)) + data)
                gc.enable()
                self.evtSent.set()
                self.evtQueue.wait()
                self.evtQueue.clear()
        except Exception as exc:
            self.errors.append(exc)
        self.__socket.close()
