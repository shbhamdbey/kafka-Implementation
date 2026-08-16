"""
producer.py
A simple producer client that connects to the broker and sends messages.
"""
import socket
import base64
from protocol import encode_wire_message, decode_wire_message


class Producer:
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

    def send(self, topic: str, payload: bytes, partition: int = 0) -> int:
        """
        Send a single message. Returns the offset assigned by the broker.
        """
        req = {
            "cmd": "PRODUCE",
            "topic": topic,
            "partition": partition,
            "payload": base64.b64encode(payload).decode(),
        }
        self.sock.sendall(encode_wire_message(req))
        return self._recv_response()["offset"]

    def send_batch(self, topic: str, payloads: list, partition: int = 0) -> list:
        """
        Send multiple messages (naive sequential batch).
        Returns list of offsets.
        """
        offsets = []
        for p in payloads:
            offsets.append(self.send(topic, p, partition))
        return offsets

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

    p = Producer(host, port)
    p.connect()
    try:
        for i in range(10):
            msg = f"Hello from producer, message #{i}".encode()
            offset = p.send("demo-topic", msg)
            print(f"  -> Sent offset={offset}")
    finally:
        p.close()
