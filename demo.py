"""
demo.py
End-to-end demo: start a broker, produce messages, consume them.
All running in a single Python process for easy testing.
"""
import threading
import time
import shutil
import os

from broker import Broker
from producer import Producer
from consumer import Consumer


def run_demo():
    # Clean slate
    data_dir = "data"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

    # 1. Start broker in background thread
    broker = Broker(host="127.0.0.1", port=19092, data_dir=data_dir)
    broker_thread = threading.Thread(target=broker.start, daemon=True)
    broker_thread.start()
    time.sleep(0.3)  # let it bind

    topic = "events"
    partition = 0

    try:
        # 2. Produce some messages
        producer = Producer("127.0.0.1", 19092)
        producer.connect()
        print("=== PRODUCING ===")
        offsets = []
        for i in range(5):
            payload = f"Event number {i:03d} — timestamp={time.time():.6f}".encode()
            off = producer.send(topic, payload, partition)
            offsets.append(off)
            print(f"  Produced offset={off}  |  {payload.decode()}")
        producer.close()

        # 3. Consume from the beginning
        consumer = Consumer("127.0.0.1", 19092)
        consumer.connect()
        print("\n=== CONSUMING (from earliest) ===")
        info = consumer.list_offsets(topic, partition)
        print(f"  Earliest={info['earliest']}, Latest={info['latest']}")

        offset = info["earliest"]
        while offset < info["latest"]:
            msgs, offset = consumer.poll(topic, offset, partition)
            for msg_offset, ts, payload in msgs:
                print(f"  [{msg_offset:4d}] {payload.decode()}")

        # 4. Demonstrate consumer-driven offset rewind
        print("\n=== REWIND & RE-CONSUME (offset 0 again) ===")
        msgs, _ = consumer.poll(topic, 0, partition)
        for msg_offset, ts, payload in msgs:
            print(f"  [{msg_offset:4d}] {payload.decode()}")

        consumer.close()
        print("\n=== DEMO COMPLETE ===")

    finally:
        broker.stop()


if __name__ == "__main__":
    run_demo()
