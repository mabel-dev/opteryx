# type:ignore
# -*- coding: utf-8 -*-
# Copyright 2019 Martin Bammer. All Rights Reserved.
# Licensed under MIT license.
# cython: language_level=3, boundscheck=False

"""Implements lightweight and fast logging."""

import atexit
import os
import sys
import time
import traceback
from collections import deque
from threading import Event
from threading import Lock
from threading import Thread
from threading import Timer

# c cdef time_time, time_strftime, time_localtime, path_join
time_time = time.time
time_strftime = time.strftime
time_localtime = time.localtime
path_join = os.path.join


def Shutdown(now=True):  # pragma: no cover
    for logger in tuple(domains.values()):
        logger.shutdown(now)


atexit.register(Shutdown)


domains = {}  # Dictionary holding all Logger instances for the configured domains

# Log-Levels
EXCEPTION = 60
CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0

LOG2SSYM = {
    EXCEPTION: "EXC",
    FATAL: "FAT",
    ERROR: "ERR",
    WARNING: "WRN",
    INFO: "INF",
    DEBUG: "DBG",
}

LOG2SYM = {
    EXCEPTION: "EXCEPT ",
    FATAL: "FATAL  ",
    ERROR: "ERROR  ",
    WARNING: "WARNING",
    INFO: "INFO   ",
    DEBUG: "DEBUG  ",
}

try:  # pragma: no cover
    # noinspection PyUnresolvedReferences,PyPep8Naming
    from colorama import Fore
    from colorama import Style
    from colorama import init as initColorama

    initColorama()

    class Colors:
        RESETALL = Style.RESET_ALL
        NORMAL = Style.NORMAL
        BRIGHT = Style.BRIGHT
        DARKWHITE = Fore.WHITE
        DARKRED = Fore.RED
        DARKMAGENTA = Fore.MAGENTA
        DARKYELLOW = Fore.YELLOW
        DARKCYAN = Fore.CYAN
        DARKGREEN = Fore.GREEN
        DARKORANGE = chr(27) + "[50m"
        WHITE = Style.BRIGHT + DARKWHITE
        RED = Style.BRIGHT + DARKRED
        MAGENTA = Style.BRIGHT + DARKMAGENTA
        GREEN = Style.BRIGHT + DARKGREEN
        YELLOW = Style.BRIGHT + DARKYELLOW
        CYAN = Style.BRIGHT + DARKCYAN
        ORANGE = Style.BRIGHT + DARKORANGE

except ImportError:  # pragma: no cover

    class Colors:
        RESETALL = ""
        NORMAL = ""
        BRIGHT = ""
        DARKWHITE = ""
        DARKRED = ""
        DARKMAGENTA = ""
        DARKYELLOW = ""
        DARKCYAN = ""
        DARKGREEN = ""
        DARKORANGE = ""
        WHITE = ""
        RED = ""
        MAGENTA = ""
        GREEN = ""
        YELLOW = ""
        CYAN = ""
        ORANGE = ""


LVL2COL = {
    FATAL: Colors.RED,
    ERROR: Colors.DARKRED,
    WARNING: Colors.YELLOW,
    INFO: Colors.GREEN,
    DEBUG: Colors.WHITE,
}


def Rotate():  # pragma: no cover
    signaled = set()
    for logger in domains.values():
        if logger.F is None or logger.common.evtRotate in signaled:
            continue
        signaled.add(logger.common.evtRotate)
        logger.rotate()


class LastMessage(object):  # pragma: no cover
    def __init__(self, key, cnt, entry):
        self.key = key
        self.cnt = cnt
        self.entry = entry


class CommonConfig(object):  # pragma: no cover
    def __init__(self, queue, evtQueue, evtRotate, maxSize, backupCnt):
        self.queue = queue
        self.evtQueue = evtQueue
        self.evtRotate = evtRotate
        self.maxSize = maxSize
        self.backupCnt = backupCnt


class Logger(object):  # pragma: no cover
    backlog = None
    dateFmt = "%y.%m.%d %H:%M:%S"
    cbMessageKey = None  # Custom log messages key calculation callback function
    cbFormatter = None  # Custom log messages formatter callback function
    cbWriter = None  # Custom log messages writer callback function
    colors = False  # Enable/Disable colored logging to console
    compress = None  # (CompressorInstance, CompressedFileExtension)
    useThreads = False  # Write log messages in main thread (False) or in background thread (True)
    encoding = None  # Encoding to use for log files
    sameMsgTimeout = 30.0  # Timeout for same log messages in a row
    sameMsgCountMax = 0  # Maximum counter value for same log messages in a row
    thrConsoleLogger = None  # Console logger thread instance.
    console = False  # Default console setting
    indent = None  # Message indent settings (offset, inc, max)
    consoleLock = None  # An optional lock for console logging
    stdout = None
    stderr = None

    def __init__(
        self,
        domain,
        level,
        pathName,
        maxSize,
        backupCnt,
        console,
        indent=None,
        server=None,
        connect=None,
    ):
        if (maxSize < 0) or (backupCnt < 0) or ((maxSize > 0) and (backupCnt == 0)):
            raise ValueError("Invalid maxSize or backupCnt")
        self.domain = domain
        self.level = level
        self.common = CommonConfig(deque(), Event(), Event(), maxSize, backupCnt)
        self._lastMsg = LastMessage(None, 1, None)
        self.pathName = pathName
        self.F = None
        self.lock = Lock()
        self.buf = []
        self.size = 0
        self.pos = 0
        self._console = console
        self._indent_offset = 0 if indent is None else indent[0]
        self._indent_inc = 0 if indent is None else indent[1]
        self._indent_max = 0 if indent is None else indent[2]
        self._thrTimer = None
        self._thrLogger = None
        self.stopped = False
        if pathName is not None:
            dirName = os.path.dirname(pathName)
            if os.path.isdir(dirName if dirName else "."):
                for logger in domains.values():
                    if pathName == logger.pathName:
                        common = logger.common
                        self.common = CommonConfig(
                            common.queue,
                            common.evtQueue,
                            common.evtRotate,
                            common.maxSize,
                            common.backupCnt,
                        )
                        break
                else:
                    self.F = open(pathName, "a", encoding=Logger.encoding)
                    self.pos = self.F.tell()
                    if Logger.useThreads:
                        self._thrLogger = Thread(
                            target=self.__logThread,
                            daemon=True,
                            name=f"LogThread_{domain}",
                        )
                        self._thrLogger.start()
        if Logger.useThreads and Logger.thrConsoleLogger is None:
            from .console import ConsoleLogger

            Logger.thrConsoleLogger = ConsoleLogger(Logger.consoleLock)
            Logger.thrConsoleLogger.start()
        if server is not None:
            from .network import LoggingServer

            self.server = LoggingServer(self, *server)
            self.server.start()
        if connect is not None:
            from .network import LoggingClient

            self.client = LoggingClient(*connect)
            self.client.start()

    def __del__(self):  # pragma: no cover
        self.stopNetwork()

    def stopNetwork(self):  # pragma: no cover
        if hasattr(self, "server"):
            self.server.stop()
            self.server.join()
            del self.server
        if hasattr(self, "client"):
            self.client.stop()
            self.client.join()
            del self.client

    def setLevel(self, level):  # pragma: no cover
        self.level = level

    def setConsole(self, console):  # pragma: no cover
        self._console = console

    @staticmethod
    def setBacklog(size):
        if size > 0:
            if Logger.backlog is None:
                Logger.backlog = deque(maxlen=size)
            else:
                Logger.backlog = deque(Logger.backlog, maxlen=size)
        else:
            Logger.backlog = None

    def __log(self, level, msg, args, kwargs, domain=None, log_time=None):
        if self.stopped:
            raise RuntimeError("Logger already stopped")
        if args:
            msg = msg % args
        if domain is None:
            domain = self.domain
        if log_time is None:
            log_time = time_time()
        if Logger.useThreads or self._thrLogger is not None:
            self.common.queue.append((log_time, domain, level, msg, kwargs))
            self.common.evtQueue.set()
        elif Logger.sameMsgCountMax > 0:
            self.__logEntry((log_time, domain, level, msg, kwargs))
        else:
            self._logMessage(None, (log_time, domain, level, msg, kwargs), 0)

    def logEntry(self, log_time, domain, level, msg, kwargs):
        self.__log(level, msg, None, kwargs, domain, log_time)

    def log(self, level, msg, *args, **kwargs):
        self.__log(level, msg, args, kwargs)

    def debug(self, msg, *args, **kwargs):
        if self.level <= DEBUG:
            self.__log(DEBUG, msg, args, kwargs)

    def info(self, msg, *args, **kwargs):
        if self.level <= INFO:
            self.__log(INFO, msg, args, kwargs)

    def warning(self, msg, *args, **kwargs):
        if self.level <= WARNING:
            self.__log(WARNING, msg, args, kwargs)

    def error(self, msg, *args, **kwargs):
        if self.level <= ERROR:
            self.__log(ERROR, msg, args, kwargs)

    def fatal(self, msg, *args, **kwargs):
        if self.level <= FATAL:
            self.__log(FATAL, msg, args, kwargs)

    def critical(self, msg, *args, **kwargs):
        if self.level <= FATAL:
            self.__log(FATAL, msg, args, kwargs)

    def exception(self, msg, *args, **kwargs):
        if self.level <= EXCEPTION:
            kwargs["exc_info"] = traceback.format_exc()
            self.__log(EXCEPTION, msg, args, kwargs)

    def stop(self, now=False):
        if self._thrTimer is not None:
            self._thrTimer.cancel()
            self._thrTimer.join()
            self._thrTimer = None
            self._logMessage(self._lastMsg.key, self._lastMsg.entry, self._lastMsg.cnt)
        self.stopNetwork()
        if self._thrLogger is None and self.F is not None:
            self.__writePending()
            self.F.flush()
            self.F.close()
            self.F = None
        if now:
            self.common.queue.clear()
        self.common.queue.append(None)
        self.common.evtQueue.set()
        self.stopped = True

    def join(self):
        if self._thrLogger is not None:
            self._thrLogger.join()
            self._thrLogger = None
        del domains[self.domain]
        if not domains and Logger.thrConsoleLogger is not None:
            Logger.thrConsoleLogger.append(None)
            Logger.thrConsoleLogger.join()
            Logger.thrConsoleLogger = None

    def shutdown(self, now=False):
        self.stop(now)
        self.join()

    def flush(self):
        if hasattr(self, "client"):
            self.client.evtSent.wait()
        if self._thrLogger is None and self.F is not None:
            self.__writePending()
            self.F.flush()

    def __rotate(self):
        self.__writePending()
        self.F.flush()
        self.F.close()
        self.pos = 0
        pathName = self.pathName
        dirName, logFileName = os.path.split(pathName)
        zExt = "" if Logger.compress is None else Logger.compress[1]
        fileNames = {
            fileName
            for fileName in os.listdir(dirName if dirName else ".")
            if fileName.startswith(logFileName)
        }
        # noinspection PyBroadException
        try:
            for cnt in range(self.common.backupCnt, 1, -1):
                srcFileName = f"{logFileName}.{cnt - 1}{zExt}"
                if srcFileName not in fileNames:
                    continue
                os.replace(
                    path_join(dirName, srcFileName),
                    path_join(dirName, f"{logFileName}.{cnt}{zExt}"),
                )
        except:
            pass
        dstFileName = f"{logFileName}.1{zExt}"
        if Logger.compress is None:
            os.replace(pathName, path_join(dirName, dstFileName))
        else:
            with open(path_join(dirName, dstFileName), "wb") as Z:
                Z.write(Logger.compress[0].compress(open(pathName, "rb").read()))
            os.remove(pathName)
        self.F = open(pathName, "a", encoding=Logger.encoding)

    def rotate(self, bWait=False):
        if self.F is not None:
            if Logger.useThreads:
                self.common.evtRotate.set()
                self.common.evtQueue.set()
                if bWait:
                    while self.common.evtQueue.is_set():
                        time.sleep(0.01)
            else:
                self.__rotate()

    def __writePending(self):
        if self.buf:
            if Logger.cbWriter is None:
                with self.lock:
                    data = "".join(self.buf)
                    del self.buf[:]
                    self.size = 0
                self.F.write(data)
            else:
                Logger.cbWriter(self)

    def _logMessage(self, key, entry, cnt):
        # entry = (logTime, domain, level, msg, kwargs)
        (
            logTime,
            domain,
            level,
            msg,
            kwargs,
        ) = entry  # logTime, domain, level, msg, kwargs
        if Logger.cbFormatter is None:
            sTime = time_strftime(Logger.dateFmt, time_localtime(logTime))
            if self._indent_inc > 0:
                depth = -self._indent_offset
                # noinspection PyProtectedMember
                frame = sys._getframe(3).f_back
                while frame:
                    frame = frame.f_back
                    depth += 1
                if depth > 0:
                    msg = " " * min(depth * self._indent_inc, self._indent_max) + msg
            if "exc_info" in kwargs:
                message = f"{sTime}: {domain}: {LOG2SYM[level]}: {msg}\n{kwargs['exc_info']}"
            else:
                message = f"{sTime}: {domain}: {LOG2SYM[level]}: {msg}"
        else:
            message = Logger.cbFormatter(self, entry)
        if key is not None:
            if cnt > 0:
                message = f"{cnt} times: {message}"
            self._lastMsg.key = key
            self._lastMsg.cnt = 1
            self._lastMsg.entry = entry
        if Logger.backlog is not None:
            Logger.backlog.append(entry)
        # noinspection PyBroadException
        try:
            if self.F is not None:
                size = len(message) + 1
                with self.lock:
                    self.buf.append(message + "\n")
                    self.size += size
                if self.common.maxSize == 0:
                    if (self.size >= 4096) or not Logger.useThreads:
                        self.__writePending()
                else:
                    self.pos += size
                    if self.pos >= self.common.maxSize:
                        if Logger.useThreads:
                            self.common.evtRotate.set()
                        else:
                            self.__rotate()
        except:
            errMsg = traceback.format_exc()
            if Logger.backlog is not None:
                Logger.backlog.append((logTime, domain, FATAL, errMsg, None))
            print(f"{Colors.RED}{errMsg}{Colors.RESETALL}", file=self.stderr)
        if self._console or kwargs.get("console", False):
            if Logger.colors:
                if "color" in kwargs:
                    color = kwargs["color"]
                else:
                    color = LVL2COL.get(level, FATAL)
                message = f"{color}{message}{Colors.RESETALL}"
            if Logger.thrConsoleLogger is None:
                if Logger.consoleLock is None:
                    print(message, file=self.stdout if level < ERROR else self.stderr)
                else:
                    with Logger.consoleLock:
                        print(message, file=self.stdout if level < ERROR else self.stderr)
            else:
                Logger.thrConsoleLogger.append((level, message))
        if hasattr(self, "client"):
            self.client.log(entry)

    def __logEntry(self, entry):
        # entry = (logTime, domain, level, msg, kwargs)
        if Logger.cbMessageKey is None:
            key = entry[3]
        else:
            key = Logger.cbMessageKey(self, entry)
        _lastMsg = self._lastMsg
        if (key == _lastMsg.key) and (_lastMsg.cnt < Logger.sameMsgCountMax):
            _lastMsg.cnt += 1
            _lastMsg.entry = entry
            if self._thrTimer is None:
                # noinspection PyTypeChecker
                self._thrTimer = Timer(
                    Logger.sameMsgTimeout, self._logMessage, args=(key, entry, 0)
                )
                self._thrTimer.start()
        elif self._thrTimer is None:
            self._logMessage(key, entry, 0)
        else:
            self._thrTimer.cancel()
            self._thrTimer.join()
            self._thrTimer = None
            self._logMessage(key, _lastMsg.entry, _lastMsg.cnt)
            self._logMessage(key, entry, 0)

    def __logThread(self):
        common = self.common
        common_evtQueue = common.evtQueue
        common_evtRotate = common.evtRotate
        common_queue_popleft = common.queue.popleft
        while True:
            try:
                entry = common_queue_popleft()
                if entry is None:
                    if self.F is not None:
                        if self.buf:
                            self.__writePending()
                        self.F.flush()
                        self.F.close()
                        self.F = None
                    break
            except IndexError:
                if self.F is not None and self.buf:
                    self.__writePending()
                common_evtQueue.wait()
                common_evtQueue.clear()
                if common_evtRotate.is_set():
                    self.__rotate()
                    common_evtRotate.clear()
                continue
            # noinspection PyBroadException
            try:
                if Logger.sameMsgCountMax > 0:
                    self.__logEntry(entry)
                else:
                    self._logMessage(None, entry, 0)
            except:
                errMsg = traceback.format_exc()
                if Logger.backlog is not None:
                    Logger.backlog.append((entry[0], entry[1], FATAL, errMsg, None))
                print(f"{Colors.RED}{errMsg}{Colors.RESETALL}", file=self.stderr)


def Remove(domain=None, now=False):  # pragma: no cover
    if domain is None:
        domain = "root"
    if domain in domains:
        domains[domain].stop(now)
        del domains[domain]
        return True
    return False


def GetLogger(
    domain=None,
    level=NOTSET,
    pathName=None,
    maxSize=0,
    backupCnt=0,
    console=None,
    indent=True,
    server=None,
    connect=None,
):  # pragma: no cover
    if not domains:
        print(f"Warning: {domain}: LogInit should be called first!")
    if console is None:
        console = Logger.console
    if indent is True:
        indent = Logger.indent
    if domain is None:
        domain = "root"
    if domain in domains:
        logger = domains[domain]
        if logger is None:
            del domains[domain]
        else:
            logger.stop()
            logger.join()
    logger = domains[domain] = Logger(
        domain, level, pathName, maxSize, backupCnt, console, indent, server, connect
    )
    return logger


def LogInit(
    domain=None,
    level=NOTSET,
    pathName=None,
    maxSize=0,
    backupCnt=0,
    console=False,
    colors=False,
    compress=None,
    useThreads=False,
    encoding=None,
    backlog=0,
    indent=None,
    server=None,
    connect=None,
    consoleLock=None,
    stdout=None,
    stderr=None,
):  # pragma: no cover
    if domain is None:
        domain = "root"
    Logger.colors = colors
    Logger.compress = compress
    Logger.useThreads = useThreads
    Logger.encoding = encoding
    Logger.console = console
    Logger.indent = indent
    Logger.setBacklog(backlog)
    Logger.consoleLock = consoleLock
    Logger.stdout = stdout if stdout is not None else sys.stdout
    Logger.stderr = stderr if stderr is not None else sys.stderr
    domains[domain] = None
    logger = GetLogger(
        domain, level, pathName, maxSize, backupCnt, console, indent, server, connect
    )
    if colors and (Colors.RESETALL == ""):
        logger.warning("Module colorama not installed! Colored log not available")
    return logger
