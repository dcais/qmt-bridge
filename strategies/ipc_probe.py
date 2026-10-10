# -*- coding: gbk -*-
"""Run once from QMT init. No trading, network, disk files or dependencies.

LOCAL tests check primitives; CROSS tests require tools/ipc_peer.py running.
Copy into a separate QMT Python strategy and run once after peer says READY.
All text is ASCII (also valid GBK); compatible with Python 3.6.
"""


def test_shared_memory():
    import mmap
    import uuid

    name = "Local\\QMT_IPC_PROBE_" + uuid.uuid4().hex
    with mmap.mmap(-1, 64, tagname=name) as writer:
        with mmap.mmap(-1, 64, tagname=name) as reader:
            writer[:4] = b"PING"
            if reader[:4] != b"PING":
                raise RuntimeError("shared memory PING mismatch")
            reader[:4] = b"PONG"
            if writer[:4] != b"PONG":
                raise RuntimeError("shared memory PONG mismatch")


def pipe_api():
    import ctypes as c
    from ctypes import wintypes as w

    api = c.WinDLL("kernel32", use_last_error=True)
    # Explicit signatures are necessary for 64-bit handles.
    def bind(name, args, result):
        fn = getattr(api, name)
        fn.argtypes, fn.restype = args, result
        return fn

    create_pipe = bind("CreateNamedPipeW", [w.LPCWSTR, w.DWORD, w.DWORD,
                       w.DWORD, w.DWORD, w.DWORD, w.DWORD, c.c_void_p], w.HANDLE)
    open_pipe = bind("CreateFileW", [w.LPCWSTR, w.DWORD, w.DWORD,
                     c.c_void_p, w.DWORD, w.DWORD, w.HANDLE], w.HANDLE)
    write = bind("WriteFile", [w.HANDLE, c.c_void_p, w.DWORD,
                 c.POINTER(w.DWORD), c.c_void_p], w.BOOL)
    read = bind("ReadFile", [w.HANDLE, c.c_void_p, w.DWORD,
                c.POINTER(w.DWORD), c.c_void_p], w.BOOL)
    peek = bind("PeekNamedPipe", [w.HANDLE, c.c_void_p, w.DWORD,
                c.POINTER(w.DWORD), c.POINTER(w.DWORD),
                c.POINTER(w.DWORD)], w.BOOL)
    close = bind("CloseHandle", [w.HANDLE], w.BOOL)
    bind("ConnectNamedPipe", [w.HANDLE, c.c_void_p], w.BOOL)
    return api


def test_named_pipe():
    import ctypes as c
    from ctypes import wintypes as w
    import uuid

    api = pipe_api()
    create_pipe, open_pipe = api.CreateNamedPipeW, api.CreateFileW
    write, read, peek, close = (api.WriteFile, api.ReadFile,
                               api.PeekNamedPipe, api.CloseHandle)
    invalid = c.c_void_p(-1).value

    def checked(ok):
        if not ok:
            raise c.WinError(c.get_last_error())

    def transfer(source, target, payload):
        count, available = w.DWORD(), w.DWORD()
        buf = c.create_string_buffer(payload)
        checked(write(source, buf, len(payload), c.byref(count), None))
        if count.value != len(payload):
            raise RuntimeError("short pipe write")
        checked(peek(target, None, 0, None, c.byref(available), None))
        if available.value < len(payload):
            raise RuntimeError("pipe data not ready; refusing to block")
        out = c.create_string_buffer(len(payload))
        checked(read(target, out, len(payload), c.byref(count), None))
        if count.value != len(payload) or out.raw != payload:
            raise RuntimeError("pipe payload mismatch")

    name = "\\\\.\\pipe\\QMT_IPC_PROBE_" + uuid.uuid4().hex
    # DUPLEX | FIRST_PIPE_INSTANCE; BYTE | NOWAIT | REJECT_REMOTE_CLIENTS.
    server = create_pipe(name, 0x00080003, 0x00000009, 1, 64, 64, 0, None)
    if server == invalid:
        raise c.WinError(c.get_last_error())
    try:
        client = open_pipe(name, 0xC0000000, 0, None, 3, 0, None)
        if client == invalid:
            raise c.WinError(c.get_last_error())
        try:
            transfer(client, server, b"PING")
            transfer(server, client, b"PONG")
        finally:
            checked(close(client))
    finally:
        checked(close(server))


MAP_NAME = "Local\\QMT_IPC_LINK_V1"
PIPE_NAME = "\\\\.\\pipe\\QMT_IPC_LINK_V1"


def pipe_send(api, handle, payload):
    import ctypes as c
    from ctypes import wintypes as w
    count = w.DWORD()
    buf = c.create_string_buffer(payload)
    if not api.WriteFile(handle, buf, len(payload), c.byref(count), None):
        raise c.WinError(c.get_last_error())
    if count.value != len(payload):
        raise RuntimeError("short pipe write")


def pipe_receive(api, handle, size, waiting_for_client=False):
    import ctypes as c
    from ctypes import wintypes as w
    count, available = w.DWORD(), w.DWORD()
    if not api.PeekNamedPipe(handle, None, 0, None, c.byref(available), None):
        error = c.get_last_error()
        if waiting_for_client and error in (230, 232, 233, 536):
            # BAD_PIPE / NO_DATA / NOT_CONNECTED / LISTENING before open.
            return None
        raise c.WinError(error)
    if available.value < size:
        return None
    buf = c.create_string_buffer(size)
    if not api.ReadFile(handle, buf, size, c.byref(count), None):
        raise c.WinError(c.get_last_error())
    if count.value != size:
        raise RuntimeError("short pipe read")
    return buf.raw


def test_cross_memory():
    import mmap
    import time
    import uuid
    # Single peer/client only. Payload first, then a one-byte ready flag.
    with mmap.mmap(-1, 128, tagname=MAP_NAME) as shared:
        if shared[:4] != b"RDY!":
            raise RuntimeError("peer not READY; start tools/ipc_peer.py first")
        token = uuid.uuid4().hex.encode("ascii")
        shared[8:40] = token
        shared[4:5] = b"Q"
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if shared[5:6] == b"A":
                if shared[40:72] != token:
                    raise RuntimeError("shared memory response mismatch")
                shared[6:7] = b"K"
                return
            time.sleep(0.01)
        raise RuntimeError("shared memory peer response timed out (2s)")


def test_cross_pipe():
    import ctypes as c
    import time
    import uuid
    api = pipe_api()
    handle = api.CreateFileW(PIPE_NAME, 0xC0000000, 0, None, 3, 0, None)
    if handle == c.c_void_p(-1).value:
        raise c.WinError(c.get_last_error())
    try:
        token = uuid.uuid4().hex.encode("ascii")
        pipe_send(api, handle, b"PING" + token)
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            reply = pipe_receive(api, handle, 36)
            if reply is not None:
                if reply != b"PONG" + token:
                    raise RuntimeError("pipe response mismatch")
                pipe_send(api, handle, b"ACK!")
                return
            time.sleep(0.01)
        raise RuntimeError("pipe peer response timed out (2s)")
    finally:
        api.CloseHandle(handle)


def init(ContextInfo):
    print("[IPC] START - no trading; CROSS tests wait at most 2s each")
    for name, test in [("LOCAL_SHARED_MEMORY", test_shared_memory),
                       ("LOCAL_NAMED_PIPE", test_named_pipe),
                       ("CROSS_SHARED_MEMORY", test_cross_memory),
                       ("CROSS_NAMED_PIPE", test_cross_pipe)]:
        try:
            test()
        except Exception as exc:
            print("[IPC] FAIL {}: {}: {}".format(name, type(exc).__name__, exc))
        else:
            print("[IPC] PASS {}: PING/PONG verified".format(name))
    print("[IPC] DONE")


def handlebar(ContextInfo):
    pass


def stop(ContextInfo):
    pass


# No __main__ execution: QMT calls init itself.
