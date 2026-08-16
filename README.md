# Kafka from Scratch — A Minimal Log-Centric Messaging System

> **Paper:** *"Kafka: a Distributed Messaging System for Log Processing"*  
> **Authors:** Jay Kreps, Neha Narkhede, Jun Rao (LinkedIn, 2011)  
> **Links:** [Wisconsin Mirror](http://pages.cs.wisc.edu/~akella/CS744/F17/838-CloudPapers/Kafka.pdf) | [GitHub Mirror](https://github.com/Ty-Chen/Reading-List/blob/master/Kafka%20a%20Distributed%20Messaging%20System%20for%20Log%20Processing.pdf)

---

## What I Built

I implemented a **minimal but fully working** distributed messaging broker in pure Python, built ground-up from the core design principles described in the 2011 Kafka paper. The system consists of:

- A **TCP broker** (`broker.py`) that handles `PRODUCE` and `FETCH` requests
- An **append-only log storage engine** (`storage.py`) with sparse indexing
- A **fixed-header binary record format** (`protocol.py`)
- A **producer client** (`producer.py`) and a **consumer client** (`consumer.py`) that tracks its own offsets
- An **end-to-end demo** (`demo.py`) that runs broker + producer + consumer in a single process

Everything runs with **zero external dependencies** — only the Python standard library.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PRODUCER (Writer)                           │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                              Append-Only Writes
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      LOG SEGMENT (Disk)                             │
│                                                                     │
│   [4B Length | 8B Offset | 8B Timestamp | Payload]                  │
│   [4B Length | 8B Offset | 8B Timestamp | Payload]  ← sequential      │
│   [4B Length | 8B Offset | 8B Timestamp | Payload]                  │
│                                                                     │
│   Sparse Index (.index):                                            │
│   Offset 0 → File Pos 0                                            │
│   Offset N → File Pos 4096   (every ~4KB)                          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                         Zero-Copy Read Path
                         (Seek Index → Sequential Read)
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         CONSUMER (Reader)                           │
│                                                                     │
│   • Tracks its OWN offset (broker is stateless)                    │
│   • Can rewind to any previous offset                              │
│   • No broker-side locking or session state                         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Core Design Principles from the Paper — What I Implemented

### 1. Append-Only Write Log
> *"Treat the data store as an immutable, sequential sequence of records. Avoid random disk writes completely."*

**My implementation:** `storage.py` — `LogSegment.append()`

- Every message is appended sequentially to a `.log` file using `open(path, "ab")` (append-binary mode).
- There are **no updates, no deletes, no random writes** — the log is strictly append-only.
- This matches the paper's insight that sequential I/O on HDDs is orders of magnitude faster than random I/O.

```python
# storage.py — LogSegment.append()
with open(self.log_path, "ab") as f:
    f.write(record)  # strictly sequential append
```

---

### 2. Fixed-Header Binary Record Format
> *"A simple, compact binary format for on-disk records."*

**My implementation:** `protocol.py`

I designed a 20-byte fixed header + variable payload layout:

```
+----------+----------+-----------+---------+
| 4 bytes  | 8 bytes  | 8 bytes   | N bytes |
| Length   | Offset   | Timestamp | Payload |
+----------+----------+-----------+---------+
```

```python
# protocol.py
RECORD_HEADER_FMT = ">IQq"   # unsigned int, unsigned long long, signed long long
RECORD_HEADER_SIZE = 20      # bytes

def encode_record(payload: bytes, offset: int) -> bytes:
    timestamp = int(time.time() * 1000)
    length = RECORD_HEADER_SIZE + len(payload)
    header = struct.pack(RECORD_HEADER_FMT, length, offset, timestamp)
    return header + payload
```

This is the exact structural idea from the paper — a self-describing record where the first 4 bytes tell you how much to read, enabling efficient sequential scanning.

---

### 3. Sparse Index (Offset → File Position)
> *"A sparse index maps offsets to file positions, allowing fast seeks without scanning the entire log."*

**My implementation:** `storage.py` — `LogSegment._offset_to_position()`

- I maintain a separate `.index` file alongside each `.log` file.
- An index entry is written every **~4KB** of log data (configurable via `INDEX_INTERVAL`).
- Each entry is 16 bytes: `[8-byte Offset | 8-byte File Position]`.
- For `FETCH` requests, I perform a **binary search** on the in-memory sparse index to find the closest file position ≤ the requested offset, then seek to that position and read sequentially.

```python
# storage.py — binary search in sparse index
lo, hi = 0, len(self.index) - 1
best_pos = 0
while lo <= hi:
    mid = (lo + hi) // 2
    idx_offset, idx_pos = self.index[mid]
    if idx_offset <= offset:
        best_pos = idx_pos
        lo = mid + 1
    else:
        hi = mid - 1
return best_pos
```

This gives **O(log n)** seek time on the index, followed by a **sequential** disk read — exactly the trade-off the paper describes.

---

### 4. Consumer-Driven Offsets
> *"The broker is completely stateless regarding consumer progress. Consumers track their own read position (offset), eliminating complex broker-side locking and state tracking."*

**My implementation:** `consumer.py` — `Consumer.poll()`

- The **broker does not know or track** where any consumer is reading.
- Each consumer maintains its own `offset` variable locally.
- When a consumer calls `FETCH(offset=42)`, the broker simply reads from byte position 42 in the log and returns the raw bytes.
- The consumer is free to **rewind** to any previous offset at any time — there is no "commit" or "ack" protocol.

```python
# consumer.py — the consumer tracks its own progress
offset = info["earliest"]
while offset < info["latest"]:
    msgs, offset = consumer.poll(topic, offset)  # consumer advances its own offset
```

This is one of the most important architectural decisions in the paper. By pushing offset tracking to the client, the broker avoids:
- Per-consumer state
- Locking and coordination overhead
- Session management and timeout logic

---

### 5. PRODUCE and FETCH as the Two Primary Commands
> *"The system supports two primary operations: append to a log (PRODUCE) and read from a log (FETCH)."*

**My implementation:** `broker.py`

I built a TCP server that handles exactly these two commands (plus `LIST_OFFSETS` for convenience):

| Command | Request | Response | Implementation |
|---------|---------|----------|----------------|
| **PRODUCE** | `topic, partition, payload` | `offset` | `Partition.produce()` → `LogSegment.append()` |
| **FETCH** | `topic, partition, offset, max_bytes` | `payload, next_offset, high_watermark` | `Partition.fetch()` → `LogSegment.read_from()` → binary search index → sequential read |
| **LIST_OFFSETS** | `topic, partition` | `earliest, latest` | Metadata query on the partition |

The wire protocol uses length-prefixed JSON for readability (the real Kafka uses a custom binary protocol, but the semantics are identical).

---

### 6. PageCache-Friendly Sequential I/O
> *"Outsource caching to the OS PageCache instead of maintaining in-memory object caches."*

**My implementation:** `storage.py`

- I do **not** maintain an in-memory message cache or object pool.
- Instead, I rely on the OS to cache the `.log` file in the **PageCache**.
- Reads are sequential (`f.read(max_bytes)` after a seek), which means the OS read-ahead prefetcher works optimally.
- The paper's insight is that the OS already has a sophisticated cache — don't fight it.

---

## What I Did NOT Implement (Honest Scope)

The following are either explicitly noted as future work in the 2011 paper or are production hardening features. I intentionally kept the scope minimal to focus on the core log-centric architecture:

| Feature | Paper Status | Why I Skipped It |
|---------|-------------|------------------|
| **Replication** | Future work (Section 6) | Single-node only; replication requires consensus (ZooKeeper / Raft) |
| **Multiple Segments per Partition** | Production detail | Single segment per partition for simplicity; rolling segments is straightforward to add |
| **Zero-Copy / `sendfile()`** | Linux-specific optimization | Python's socket layer doesn't expose `sendfile()` easily; the architecture supports it |
| **ZooKeeper / Consumer Groups** | Mentioned in paper | Consumer groups require coordination service; my consumer is standalone |
| **Compression** | Supported in real Kafka | Omitted to keep the binary format simple |
| **Custom Binary Wire Protocol** | Real Kafka uses one | I used length-prefixed JSON for readability; semantics are identical |

---

## Project Structure

```
kafka-from-scratch/
├── protocol.py      # Binary record format (20-byte header) + wire protocol
├── storage.py       # LogSegment (append-only .log + sparse .index)
├── broker.py        # TCP server: PRODUCE, FETCH, LIST_OFFSETS
├── producer.py      # Client producer
├── consumer.py      # Client consumer (pull-based, offset-tracking)
├── demo.py          # End-to-end single-process demo
└── README.md        # This file
```

---

## Quick Start

### Run the end-to-end demo
```bash
python demo.py
```

Output:
```
[Broker] Listening on 127.0.0.1:19092
=== PRODUCING ===
  Produced offset=0   |  Event number 000 — timestamp=...
  Produced offset=68  |  Event number 001 — timestamp=...
...
=== CONSUMING (from earliest) ===
  [   0] Event number 000 — timestamp=...
  [  68] Event number 001 — timestamp=...
...
=== REWIND & RE-CONSUME (offset 0 again) ===
  [   0] Event number 000 — timestamp=...
...
=== DEMO COMPLETE ===
```

### Run components manually
```bash
# Terminal 1 — Broker
python broker.py 9092

# Terminal 2 — Producer
python producer.py 127.0.0.1 9092

# Terminal 3 — Consumer
python consumer.py 127.0.0.1 9092 demo-topic
```

---

## On-Disk Format

Each partition creates two files under `data/`:

```
data/
├── <topic>-<partition>.log     # raw binary records (append-only)
└── <topic>-<partition>.index   # sparse offset→position map
```

### Record layout
```
 0          4          12         20         20+N
 | Length   | Offset   | Timestamp| Payload  |
 | 4 bytes  | 8 bytes  | 8 bytes  | N bytes  |
```

### Index entry layout
```
 0          8          16
 | Offset   | Position |
 | 8 bytes  | 8 bytes  |
```

---

## Why This Matters

The 2011 Kafka paper introduced a radical rethinking of messaging systems:

1. **Don't treat messages as transient events** — treat them as an immutable log.
2. **Don't buffer in the broker** — let the OS PageCache do the work.
3. **Don't track consumer state in the broker** — push it to the client.
4. **Don't do random I/O** — sequential append-only writes are orders of magnitude faster.

My implementation proves these principles can be expressed in ~500 lines of Python and actually work. The broker is stateless, the consumer can rewind at will, and the sparse index makes offset-based seeks efficient without any complex data structures.

---

## Quick Start

### 1. Run the end-to-end demo
```bash
cd kafka-from-scratch
python demo.py
```

This starts an in-memory broker, produces 5 messages, consumes them, and then rewinds to offset 0 to demonstrate the consumer-driven offset model.

### 2. Run broker + producer + consumer manually

**Terminal 1 — Start the broker:**
```bash
python broker.py 9092
```

**Terminal 2 — Produce messages:**
```bash
python producer.py 127.0.0.1 9092
```

**Terminal 3 — Consume messages:**
```bash
python consumer.py 127.0.0.1 9092 demo-topic
```

---

## On-Disk Format

Each partition creates two files under `data/`:

```
data/
└── <topic>-<partition>.log      # raw binary records
└── <topic>-<partition>.index    # sparse offset->position map
```

### Record layout (20-byte header + payload)

```
+----------+----------+-----------+---------+
| 4 bytes  | 8 bytes  | 8 bytes   | N bytes |
| Length   | Offset   | Timestamp | Payload |
+----------+----------+-----------+---------+
```

- **Length**: total record size (header + payload)
- **Offset**: logical byte position in the log (monotonically increasing)
- **Timestamp**: milliseconds since epoch
- **Payload**: arbitrary bytes

### Sparse Index layout

```
+----------+----------+
| 8 bytes  | 8 bytes  |
| Offset   | Position |
+----------+----------+
```

An index entry is written every ~4KB of log data.  
FETCH uses binary search on this index to locate the segment file position for a given offset, then reads sequentially from disk.

---

## Wire Protocol

For readability, this toy implementation uses **length-prefixed JSON** over TCP instead of Kafka's custom binary wire format.

### PRODUCE
```json
{"cmd":"PRODUCE","topic":"events","partition":0,"payload":"<base64>"}
```
Response:
```json
{"status":"OK","offset":42}
```

### FETCH
```json
{"cmd":"FETCH","topic":"events","partition":0,"offset":0,"max_bytes":1048576}
```
Response:
```json
{"status":"OK","payload":"<base64>","next_offset":123,"high_watermark":456}
```

---


## License

MIT — Educational use only.
