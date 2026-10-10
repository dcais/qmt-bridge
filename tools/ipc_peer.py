"""External IPC echo peer. Start first, then run ipc_probe.py in QMT.

Uses only Python standard library. Same Windows login/session as QMT.
One peer and one QMT probe per run; exits after both pass or after 120s.
"""
import ctypes
import mmap
from pathlib import Path
import runpy
import time


def main():
    probe = runpy.run_path(str(Path(__file__).resolve().parents[1]
                              / "strategies" / "ipc_probe.py"))
    api = probe["pipe_api"]()
    # FIRST_PIPE_INSTANCE prevents two peers from using the same endpoint.
    handle = api.CreateNamedPipeW(probe["PIPE_NAME"], 0x00080003,
                                 0x00000009, 1, 128, 128, 0, None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        if not api.ConnectNamedPipe(handle, None):
            error = ctypes.get_last_error()
            if error not in (535, 536):  # CONNECTED or LISTENING (NOWAIT).
                raise ctypes.WinError(error)
        with mmap.mmap(-1, 128, tagname=probe["MAP_NAME"]) as shared:
            shared[:] = b"\x00" * 128
            shared[:4] = b"RDY!"
            print("[PEER] READY - run QMT ipc_probe now (120s)", flush=True)
            memory_done = pipe_done = pipe_sent = False
            deadline = time.monotonic() + 120
            try:
                while time.monotonic() < deadline:
                    if not memory_done:
                        if shared[4:5] == b"Q" and shared[5:6] != b"A":
                            shared[40:72] = shared[8:40]
                            shared[5:6] = b"A"
                        if shared[6:7] == b"K":
                            memory_done = True
                            print("[PEER] PASS CROSS_SHARED_MEMORY", flush=True)
                    if not pipe_done:
                        msg = probe["pipe_receive"](api, handle, 4 if pipe_sent else 36,
                                                     waiting_for_client=not pipe_sent)
                        if msg is not None:
                            if pipe_sent:
                                if msg != b"ACK!":
                                    raise RuntimeError("invalid pipe acknowledgement")
                                pipe_done = True
                                print("[PEER] PASS CROSS_NAMED_PIPE", flush=True)
                            else:
                                if msg[:4] != b"PING":
                                    raise RuntimeError("invalid pipe request")
                                probe["pipe_send"](api, handle, b"PONG" + msg[4:])
                                pipe_sent = True
                    if memory_done and pipe_done:
                        print("[PEER] PASS ALL - both round trips confirmed", flush=True)
                        return 0
                    time.sleep(0.01)
                print("[PEER] TIMEOUT: shared_memory={}, named_pipe={}".format(
                    memory_done, pipe_done), flush=True)
                return 1
            finally:
                shared[:4] = b"STOP"
    finally:
        api.CloseHandle(handle)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("[PEER] stopped")
        raise SystemExit(130)
    except Exception as exc:
        print("[PEER] FAIL {}: {}".format(type(exc).__name__, exc))
        raise SystemExit(1)
