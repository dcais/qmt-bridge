"""Windows overlapped byte pipes. No filesystem, socket or database operations.

Endpoints are single-thread-owned. poll performs bounded nonblocking work;
close cancels and drains outstanding operations before releasing their buffers.
"""
import ctypes
import os
from collections import deque
from ctypes import wintypes as W

from .wire import Framer, MAX_FRAME_BYTES, encode_frame


class PipeDisconnected(OSError):
    pass


class QueueFull(BufferError):
    pass


class OVERLAPPED(ctypes.Structure):
    _fields_ = [("Internal", ctypes.c_size_t), ("InternalHigh", ctypes.c_size_t),
                ("Offset", W.DWORD), ("OffsetHigh", W.DWORD), ("hEvent", W.HANDLE)]


class SECURITY_ATTRIBUTES(ctypes.Structure):
    _fields_ = [("nLength", W.DWORD), ("lpSecurityDescriptor", W.LPVOID),
                ("bInheritHandle", W.BOOL)]


if os.name == "nt":
    _k = ctypes.WinDLL("kernel32", use_last_error=True)
    _a = ctypes.WinDLL("advapi32", use_last_error=True)

    def _bind(dll, name, result, args):
        fn = getattr(dll, name)
        fn.restype, fn.argtypes = result, args
        return fn

    _create = _bind(_k, "CreateNamedPipeW", W.HANDLE,
                    [W.LPCWSTR, W.DWORD, W.DWORD, W.DWORD, W.DWORD, W.DWORD,
                     W.DWORD, ctypes.POINTER(SECURITY_ATTRIBUTES)])
    _open = _bind(_k, "CreateFileW", W.HANDLE,
                  [W.LPCWSTR, W.DWORD, W.DWORD, W.LPVOID, W.DWORD, W.DWORD, W.HANDLE])
    _event = _bind(_k, "CreateEventW", W.HANDLE, [W.LPVOID, W.BOOL, W.BOOL, W.LPCWSTR])
    _close = _bind(_k, "CloseHandle", W.BOOL, [W.HANDLE])
    _reset = _bind(_k, "ResetEvent", W.BOOL, [W.HANDLE])
    _connect = _bind(_k, "ConnectNamedPipe", W.BOOL, [W.HANDLE, ctypes.POINTER(OVERLAPPED)])
    _read = _bind(_k, "ReadFile", W.BOOL,
                  [W.HANDLE, W.LPVOID, W.DWORD, ctypes.POINTER(W.DWORD), ctypes.POINTER(OVERLAPPED)])
    _write = _bind(_k, "WriteFile", W.BOOL,
                   [W.HANDLE, W.LPVOID, W.DWORD, ctypes.POINTER(W.DWORD), ctypes.POINTER(OVERLAPPED)])
    _result = _bind(_k, "GetOverlappedResult", W.BOOL,
                    [W.HANDLE, ctypes.POINTER(OVERLAPPED), ctypes.POINTER(W.DWORD), W.BOOL])
    _cancel = _bind(_k, "CancelIoEx", W.BOOL, [W.HANDLE, ctypes.POINTER(OVERLAPPED)])
    _process = _bind(_k, "GetCurrentProcess", W.HANDLE, [])
    _token = _bind(_a, "OpenProcessToken", W.BOOL, [W.HANDLE, W.DWORD, ctypes.POINTER(W.HANDLE)])
    _token_info = _bind(_a, "GetTokenInformation", W.BOOL,
                        [W.HANDLE, ctypes.c_int, W.LPVOID, W.DWORD, ctypes.POINTER(W.DWORD)])
    _sid_string = _bind(_a, "ConvertSidToStringSidW", W.BOOL,
                        [W.LPVOID, ctypes.POINTER(W.LPWSTR)])
    _descriptor = _bind(_a, "ConvertStringSecurityDescriptorToSecurityDescriptorW", W.BOOL,
                        [W.LPCWSTR, W.DWORD, ctypes.POINTER(W.LPVOID), ctypes.POINTER(W.DWORD)])
    _free = _bind(_k, "LocalFree", W.LPVOID, [W.LPVOID])


def _error():
    return ctypes.WinError(ctypes.get_last_error())


def _security():
    """Explicit protected DACL granting only the process user's SID access."""
    token = W.HANDLE()
    sid = W.LPWSTR()
    if not _token(_process(), 8, ctypes.byref(token)):
        raise _error()
    try:
        size = W.DWORD()
        _token_info(token, 1, None, 0, ctypes.byref(size))
        info = ctypes.create_string_buffer(size.value)
        if not _token_info(token, 1, info, size, ctypes.byref(size)):
            raise _error()
        sid_ptr = ctypes.cast(info, ctypes.POINTER(W.LPVOID))[0]
        if not _sid_string(sid_ptr, ctypes.byref(sid)):
            raise _error()
        descriptor = W.LPVOID()
        if not _descriptor("D:P(A;;GA;;;" + sid.value + ")", 1,
                           ctypes.byref(descriptor), None):
            raise _error()
        return SECURITY_ATTRIBUTES(ctypes.sizeof(SECURITY_ATTRIBUTES), descriptor, False)
    finally:
        if sid:
            _free(ctypes.cast(sid, W.LPVOID))
        _close(token)


class _Operation:
    def __init__(self):
        self.ov = OVERLAPPED()
        self.ov.hEvent = _event(None, True, False, None)
        if not self.ov.hEvent:
            raise _error()
        self.pending = False
        self.buffer = None


class PipeEndpoint:
    def __init__(self, name, server=False, max_frame_bytes=MAX_FRAME_BYTES,
                 max_queue_bytes=128 * 1024 * 1024):
        if os.name != "nt":
            raise OSError("named pipes require Windows")
        prefix = "\\\\.\\pipe\\"
        if name.startswith(prefix):
            name = name[len(prefix):]
        if not name or any(c in name for c in "\\/\x00"):
            raise ValueError("pipe name must be a local simple name")
        self.name = prefix + name
        self.server = server
        self.connected = False
        self.closed = False
        self.handle = None
        self.framer = Framer(max_frame_bytes)
        self.max_frame_bytes = max_frame_bytes
        self.max_queue_bytes = max_queue_bytes
        self.queue = deque()
        self.queued_bytes = 0
        self.offset = 0
        self.ops = []
        try:
            for _ in range(3):
                self.ops.append(_Operation())
            self.connect_op, self.read_op, self.write_op = self.ops
            if server:
                sa = _security()
                try:
                    # duplex | overlapped | first instance; byte mode | reject remote.
                    handle = _create(self.name, 3 | 0x40000000 | 0x00080000,
                                     8, 1, 65536, 65536, 0, ctypes.byref(sa))
                finally:
                    _free(sa.lpSecurityDescriptor)
                if handle == ctypes.c_void_p(-1).value:
                    raise _error()
                self.handle = handle
        except Exception:
            self.close()
            raise

    def send(self, message):
        if self.closed:
            raise PipeDisconnected("endpoint is closed")
        frame = encode_frame(message, self.max_frame_bytes)
        if self.queued_bytes + len(frame) > self.max_queue_bytes:
            raise QueueFull("outbound pipe queue is full")
        self.queue.append(frame)
        self.queued_bytes += len(frame)

    def _complete(self, op):
        count = W.DWORD()
        if not _result(self.handle, ctypes.byref(op.ov), ctypes.byref(count), False):
            error = ctypes.get_last_error()
            if error == 996:  # ERROR_IO_INCOMPLETE
                return None
            raise PipeDisconnected(error, "pipe operation failed")
        op.pending = False
        return count.value

    def _connection(self):
        if self.connected:
            return True
        if not self.server:
            handle = _open(self.name, 0xC0000000, 0, None, 3, 0x40000000, None)
            if handle == ctypes.c_void_p(-1).value:
                error = ctypes.get_last_error()
                if error in (2, 231):  # absent or busy: retry next poll
                    return False
                raise OSError(error, "cannot connect to local pipe")
            self.handle = handle
            self.connected = True
            return True
        op = self.connect_op
        if op.pending:
            if self._complete(op) is None:
                return False
        elif not _connect(self.handle, ctypes.byref(op.ov)):
            error = ctypes.get_last_error()
            if error == 997:
                op.pending = True
                return False
            if error != 535:  # ERROR_PIPE_CONNECTED: client won the race
                raise PipeDisconnected(error, "pipe connect failed")
        self.connected = True
        return True

    def _io(self, op, writing):
        if op.pending:
            count = self._complete(op)
            if count is None:
                return None
        else:
            if writing and not self.queue:
                return None
            _reset(op.ov.hEvent)
            if writing:
                data = self.queue[0][self.offset:self.offset + 65536]
                op.buffer = ctypes.create_string_buffer(data, len(data))
            else:
                op.buffer = ctypes.create_string_buffer(65536)
            count_out = W.DWORD()
            fn = _write if writing else _read
            if not fn(self.handle, op.buffer, len(op.buffer), ctypes.byref(count_out),
                      ctypes.byref(op.ov)):
                error = ctypes.get_last_error()
                if error == 997:
                    op.pending = True
                    return None
                raise PipeDisconnected(error, "pipe I/O failed")
            count = count_out.value
        if not count:
            raise PipeDisconnected("pipe peer disconnected")
        if writing:
            self.offset += count
            if self.offset == len(self.queue[0]):
                self.queued_bytes -= len(self.queue.popleft())
                self.offset = 0
            op.buffer = None
            return []
        data = op.buffer.raw[:count]
        op.buffer = None
        return self.framer.feed(data)

    def poll(self):
        if self.closed:
            raise PipeDisconnected("endpoint is closed")
        messages = []
        try:
            if not self._connection():
                return []
            for _ in range(16):
                read = self._io(self.read_op, False)
                if read is not None:
                    messages.extend(read)
                written = self._io(self.write_op, True)
                if read is None and written is None:
                    break
            return messages
        except Exception:
            self.close()
            if messages:
                # Deliver already received frames; next poll reports the closure.
                return messages
            raise

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.connected = False
        if self.handle is not None:
            for op in self.ops:
                if op.pending:
                    _cancel(self.handle, ctypes.byref(op.ov))
            for op in self.ops:
                if op.pending:
                    count = W.DWORD()
                    # Drain cancellation before releasing OVERLAPPED/buffer memory.
                    _result(self.handle, ctypes.byref(op.ov), ctypes.byref(count), True)
                    op.pending = False
            _close(self.handle)
            self.handle = None
        for op in self.ops:
            _close(op.ov.hEvent)
            op.buffer = None
        self.ops = []
        self.queue.clear()
        self.queued_bytes = 0
