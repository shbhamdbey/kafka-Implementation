"""
protocol.py
Binary wire protocol and on-disk message format.

On-disk message layout (fixed header + variable payload):
    [4-byte Length] [8-byte Offset] [8-byte Timestamp] [Payload]

Wire commands (simple length-prefixed JSON for clarity in this
toy implementation; real Kafka uses a custom binary protocol).
"""
import struct
import json
import time
from typing import Tuple, Optional

# ------------------------------------------------------------------
# On-disk record format
# ------------------------------------------------------------------
# 4 bytes  : length of the whole record (header + payload)
# 8 bytes  : offset (logical position in the log)
# 8 bytes  : timestamp (milliseconds since epoch)
# N bytes  : payload

RECORD_HEADER_FMT = ">IQq"   # unsigned int, unsigned long long, signed long long
RECORD_HEADER_SIZE = struct.calcsize(RECORD_HEADER_FMT)  # 20 bytes


def encode_record(payload: bytes, offset: int) -> bytes:
    """Encode a single record into its on-disk binary representation."""
    timestamp = int(time.time() * 1000)
    length = RECORD_HEADER_SIZE + len(payload)
    header = struct.pack(RECORD_HEADER_FMT, length, offset, timestamp)
    return header + payload


def decode_record_header(data: bytes) -> Tuple[int, int, int]:
    """Decode the fixed header. Returns (length, offset, timestamp)."""
    length, offset, timestamp = struct.unpack(RECORD_HEADER_FMT, data)
    return length, offset, timestamp


def record_size_from_header(data: bytes) -> int:
    """Peek at the first 4 bytes to know the full record size."""
    if len(data) < 4:
        return -1
    return struct.unpack(">I", data[:4])[0]


# ------------------------------------------------------------------
# Wire protocol (length-prefixed JSON for simplicity)
# ------------------------------------------------------------------
# Real Kafka uses a custom binary request/response format.
# Here we use a simple length-prefixed JSON to keep the code readable.

def encode_wire_message(obj: dict) -> bytes:
    """Encode a dict into a length-prefixed JSON blob."""
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    return struct.pack(">I", len(body)) + body


def decode_wire_message(data: bytes) -> Tuple[Optional[dict], bytes]:
    """
    Try to extract one complete wire message from `data`.
    Returns (message_dict, remaining_bytes) or (None, data) if incomplete.
    """
    if len(data) < 4:
        return None, data
    body_len = struct.unpack(">I", data[:4])[0]
    if len(data) < 4 + body_len:
        return None, data
    body = data[4:4 + body_len]
    remaining = data[4 + body_len:]
    return json.loads(body.decode("utf-8")), remaining
