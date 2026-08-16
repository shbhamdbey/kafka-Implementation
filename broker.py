"""
broker.py
A minimal TCP broker implementing the two core commands from the paper:

    PRODUCE(topic, partition, bytes) -> offset
    FETCH(topic, partition, offset, max_bytes) -> bytes

The broker is stateless regarding consumer progress — consumers
track their own offsets, exactly as described in the 2011 paper.
"""
import socket
import threading
import traceback
from typing import Dict
from protocol import encode_wire_message, decode_wire_message
from storage import Partition


class Broker:
    """
    Single-node broker.  
    Maintains a dict of (topic, partition) -> Partition.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 9092, data_dir: str = "data"):
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.partitions: Dict[tuple, Partition] = {}
        self.lock = threading.Lock()
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.running = False

    # ------------------------------------------------------------------
    # Partition management
    # ------------------------------------------------------------------
    def _get_partition(self, topic: str, partition: int) -> Partition:
        key = (topic, partition)
        with self.lock:
            if key not in self.partitions:
                self.partitions[key] = Partition(topic, partition, self.data_dir)
            return self.partitions[key]

    # ------------------------------------------------------------------
    # Command handlers
    # ------------------------------------------------------------------
    def handle_produce(self, req: dict) -> dict:
        """
        PRODUCE request:
            { "cmd": "PRODUCE", "topic": str, "partition": int, "payload": str(base64) }
        Response:
            { "status": "OK", "offset": int }
        """
        import base64
        topic = req["topic"]
        partition = req.get("partition", 0)
        payload = base64.b64decode(req["payload"])

        part = self._get_partition(topic, partition)
        offset = part.produce(payload)
        return {"status": "OK", "offset": offset}

    def handle_fetch(self, req: dict) -> dict:
        """
        FETCH request:
            { "cmd": "FETCH", "topic": str, "partition": int,
              "offset": int, "max_bytes": int }
        Response:
            { "status": "OK", "payload": str(base64), "next_offset": int }
        """
        import base64
        topic = req["topic"]
        partition = req.get("partition", 0)
        offset = req["offset"]
        max_bytes = req.get("max_bytes", 1_048_576)

        part = self._get_partition(topic, partition)
        data = part.fetch(offset, max_bytes)
        next_offset = offset + len(data) if data else offset

        return {
            "status": "OK",
            "payload": base64.b64encode(data).decode(),
            "next_offset": next_offset,
            "high_watermark": part.latest_offset(),
        }

    def handle_list_offsets(self, req: dict) -> dict:
        """
        LIST_OFFSETS request:
            { "cmd": "LIST_OFFSETS", "topic": str, "partition": int }
        Response:
            { "status": "OK", "earliest": int, "latest": int }
        """
        topic = req["topic"]
        partition = req.get("partition", 0)
        part = self._get_partition(topic, partition)
        return {
            "status": "OK",
            "earliest": part.earliest_offset(),
            "latest": part.latest_offset(),
        }

    # ------------------------------------------------------------------
    # Network loop
    # ------------------------------------------------------------------
    def _handle_client(self, conn: socket.socket, addr):
        """Handle one persistent TCP connection."""
        buffer = b""
        try:
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buffer += chunk
                while True:
                    msg, buffer = decode_wire_message(buffer)
                    if msg is None:
                        break
                    response = self._dispatch(msg)
                    conn.sendall(encode_wire_message(response))
        except ConnectionResetError:
            pass
        except Exception as e:
            print(f"[!] Client error from {addr}: {e}")
            traceback.print_exc()
        finally:
            conn.close()

    def _dispatch(self, req: dict) -> dict:
        cmd = req.get("cmd")
        try:
            if cmd == "PRODUCE":
                return self.handle_produce(req)
            elif cmd == "FETCH":
                return self.handle_fetch(req)
            elif cmd == "LIST_OFFSETS":
                return self.handle_list_offsets(req)
            else:
                return {"status": "ERROR", "message": f"Unknown cmd: {cmd}"}
        except Exception as e:
            traceback.print_exc()
            return {"status": "ERROR", "message": str(e)}

    def start(self):
        self.server.bind((self.host, self.port))
        self.server.listen(5)
        self.running = True
        print(f"[Broker] Listening on {self.host}:{self.port}")
        print(f"[Broker] Data directory: {self.data_dir}")
        try:
            while self.running:
                conn, addr = self.server.accept()
                t = threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True)
                t.start()
        except OSError:
            pass  # server closed

    def stop(self):
        self.running = False
        self.server.close()
        print("[Broker] Stopped.")


if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9092
    broker = Broker(port=port)
    try:
        broker.start()
    except KeyboardInterrupt:
        broker.stop()
