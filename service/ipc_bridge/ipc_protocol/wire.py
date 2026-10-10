"""Bounded UTF-8 JSON framing, shared by both sides of the local pipe."""
import json
import struct

MAX_FRAME_BYTES = 64 * 1024 * 1024


class ProtocolError(ValueError):
    pass


def encode_frame(message, max_frame_bytes=MAX_FRAME_BYTES):
    if not isinstance(message, dict):
        raise ProtocolError("message must be a JSON object")
    payload = json.dumps(message, ensure_ascii=False, allow_nan=False,
                         separators=(",", ":")).encode("utf-8")
    if not payload or len(payload) > max_frame_bytes:
        raise ProtocolError("frame exceeds configured size")
    return struct.pack("!I", len(payload)) + payload


class Framer:
    def __init__(self, max_frame_bytes=MAX_FRAME_BYTES):
        self.max_frame_bytes = max_frame_bytes
        self.buffer = bytearray()

    def feed(self, data):
        self.buffer.extend(data)
        messages = []
        while len(self.buffer) >= 4:
            size = struct.unpack_from("!I", self.buffer)[0]
            if not size or size > self.max_frame_bytes:
                raise ProtocolError("invalid frame length")
            if len(self.buffer) < size + 4:
                break
            payload = bytes(self.buffer[4:size + 4])
            del self.buffer[:size + 4]
            try:
                message = json.loads(payload.decode("utf-8"),
                                     parse_constant=self._invalid_constant)
            except (ValueError, UnicodeError) as exc:
                raise ProtocolError("invalid JSON frame: " + str(exc))
            if not isinstance(message, dict):
                raise ProtocolError("message must be a JSON object")
            messages.append(message)
        return messages

    @staticmethod
    def _invalid_constant(value):
        raise ValueError("non-finite JSON number: " + value)
