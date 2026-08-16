"""
consumer.py
A simple consumer client that pulls messages by offset.

Key design from the 2011 paper:
- The consumer tracks its own read position (offset).
- The broker is completely stateless about consumer progress.
- This eliminates complex broker-side locking and state tracking.
"""
import socket
import base64
import struct
from protocol import encode_wire_message, decode_wire_message, decode_record_header, RECORD_HEADER_SIZE


class Consumer:
    def __init__(self, host: str = "127.0.0.1", port: int = 9092):
        self.host = host
        self.port = port
        self.sock: socket.socket = None
        self._buffer = b""

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self._buffer = b""

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None

    def list_offsets(self, topic: str, partition: int = 0) -> dict:
        """Get earliest and latest offsets for a partition."""
        req = {
            "cmd": "LIST_OFFSETS",
            "topic": topic,
            "partition": partition,
        }
        self.sock.sendall(encode_wire_message(req))
        return self._recv_response()

    def fetch(self, topic: str, offset: int, partition: int = 0, max_bytes: int = 1_048_576) -> list:
        """
        Fetch a batch of messages starting at `offset`.
        Returns list of (offset, timestamp, payload_bytes).
        """
        req = {
            "cmd": "FETCH",
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "max_bytes": max_bytes,
        }
        self.sock.sendall(encode_wire_message(req))
        resp = self._recv_response()

        if resp.get("status") != "OK":
            raise RuntimeError(resp.get("message", "Unknown error"))

        raw = base64.b64decode(resp["payload"])
        messages = []
        pos = 0
        while pos < len(raw):
            if pos + RECORD_HEADER_SIZE > len(raw):
                break
            length, msg_offset, timestamp = decode_record_header(raw[pos:pos + RECORD_HEADER_SIZE])
            payload = raw[pos + RECORD_HEADER_SIZE:pos + length]
            messages.append((msg_offset, timestamp, payload))
            pos += length
        return messages

    def poll(self, topic: str, offset: int, partition: int = 0, max_bytes: int = 1_048_576) -> tuple:
        """
        Convenience: fetch messages and return (messages, next_offset).
        """
        msgs = self.fetch(topic, offset, partition, max_bytes)
        if msgs:
            next_offset = msgs[-1][0] + RECORD_HEADER_SIZE + len(msgs[-1][2])
        else:
            next_offset = offset
        return msgs, next_offset

    def _recv_response(self) -> dict:
        while True:
            msg, self._buffer = decode_wire_message(self._buffer)
            if msg is not None:
                return msg
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("Broker closed connection")
            self._buffer += chunk


if __name__ == "__main__":
    import sys
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9092
    topic = sys.argv[3] if len(sys.argv) > 3 else "demo-topic"

    c = Consumer(host, port)
    c.connect()
    try:
        info = c.list_offsets(topic)
        print(f"Partition offsets: earliest={info['earliest']}, latest={info['latest']}")

        offset = info["earliest"]
        while offset < info["latest"]:
            msgs, offset = c.poll(topic, offset)
            for msg_offset, ts, payload in msgs:
                print(f"  [{msg_offset}] {payload.decode()}")
    finally:
        c.close()
