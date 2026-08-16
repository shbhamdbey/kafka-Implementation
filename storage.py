"""
storage.py
Append-only log segment with a sparse in-memory index.

Design (from the 2011 Kafka paper):
- Each partition is a logical log, physically a set of segment files.
- Messages are appended sequentially (no random writes).
- A sparse index maps Offset -> File Position for fast seeks.
- The broker is stateless regarding consumer progress.
"""
import os
import struct
import threading
from typing import List, Tuple, Optional, BinaryIO
from protocol import (
    encode_record,
    decode_record_header,
    record_size_from_header,
    RECORD_HEADER_SIZE,
)


class LogSegment:
    """
    A single append-only log segment file plus its sparse index.

    Files:
        <base_path>.log    — raw binary records
        <base_path>.index  — sparse offset->position map (binary)
    """

    INDEX_ENTRY_FMT = ">QQ"   # offset (8) -> file_position (8)
    INDEX_ENTRY_SIZE = struct.calcsize(INDEX_ENTRY_FMT)  # 16 bytes
    INDEX_INTERVAL = 4096      # add index entry every ~4KB of log data

    def __init__(self, base_path: str):
        self.base_path = base_path
        self.log_path = base_path + ".log"
        self.index_path = base_path + ".index"
        self.lock = threading.Lock()

        # In-memory sparse index: list of (offset, file_position)
        self.index: List[Tuple[int, int]] = []
        self.next_offset: int = 0
        self._file_size: int = 0

        self._ensure_files()
        self._rebuild_index()

    # ------------------------------------------------------------------
    # File lifecycle
    # ------------------------------------------------------------------
    def _ensure_files(self):
        """Create empty files if they don't exist."""
        for path in (self.log_path, self.index_path):
            if not os.path.exists(path):
                open(path, "wb").close()

    def _rebuild_index(self):
        """Load (or rebuild) the sparse index from disk."""
        self.index = []
        self.next_offset = 0
        self._file_size = os.path.getsize(self.log_path)

        if os.path.getsize(self.index_path) > 0:
            with open(self.index_path, "rb") as f:
                while True:
                    chunk = f.read(self.INDEX_ENTRY_SIZE)
                    if len(chunk) < self.INDEX_ENTRY_SIZE:
                        break
                    offset, pos = struct.unpack(self.INDEX_ENTRY_FMT, chunk)
                    self.index.append((offset, pos))
            if self.index:
                self.next_offset = self.index[-1][0]
                # Scan forward from last known position to find true next_offset
                self._scan_from(self.index[-1][1])
        else:
            # No index — scan the whole log file
            self._scan_from(0)

    def _scan_from(self, start_pos: int):
        """Scan the .log file from `start_pos` to rebuild next_offset."""
        with open(self.log_path, "rb") as f:
            f.seek(start_pos)
            while True:
                pos = f.tell()
                header = f.read(RECORD_HEADER_SIZE)
                if len(header) < RECORD_HEADER_SIZE:
                    break
                length, offset, _ = decode_record_header(header)
                payload_len = length - RECORD_HEADER_SIZE
                f.seek(payload_len, 1)  # skip payload
                self.next_offset = offset + length
                # Add sparse index entry if we crossed the interval
                if not self.index or (pos - self.index[-1][1]) >= self.INDEX_INTERVAL:
                    self.index.append((offset, pos))

    # ------------------------------------------------------------------
    # Append
    # ------------------------------------------------------------------
    def append(self, payload: bytes) -> int:
        """
        Append a payload to the log. Returns the offset of the new record.
        """
        with self.lock:
            offset = self.next_offset
            record = encode_record(payload, offset)
            pos = self._file_size

            with open(self.log_path, "ab") as f:
                f.write(record)

            # Update sparse index
            if not self.index or (pos - self.index[-1][1]) >= self.INDEX_INTERVAL:
                with open(self.index_path, "ab") as fidx:
                    fidx.write(struct.pack(self.INDEX_ENTRY_FMT, offset, pos))
                self.index.append((offset, pos))

            self._file_size += len(record)
            self.next_offset = offset + len(record)
            return offset

    # ------------------------------------------------------------------
    # Read / Fetch
    # ------------------------------------------------------------------
    def read_from(self, start_offset: int, max_bytes: int = 1_048_576) -> bytes:
        """
        Read raw bytes starting at `start_offset`, up to `max_bytes`.
        Returns a chunk of bytes that may contain one or more complete
        records (the caller is responsible for parsing boundaries).
        """
        with self.lock:
            if start_offset >= self.next_offset:
                return b""

            # Binary search in sparse index to find the file position
            file_pos = self._offset_to_position(start_offset)

            with open(self.log_path, "rb") as f:
                f.seek(file_pos)
                data = f.read(max_bytes)
            return data

    def _offset_to_position(self, offset: int) -> int:
        """Binary search the sparse index to find the closest <= position."""
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

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------
    def earliest_offset(self) -> int:
        if self.index:
            return self.index[0][0]
        return 0

    def latest_offset(self) -> int:
        return self.next_offset

    def __repr__(self):
        return (f"LogSegment({self.base_path!r}, "
                f"records={len(self.index)}, "
                f"next_offset={self.next_offset})")


class Partition:
    """
    A single partition backed by one LogSegment.
    (In production Kafka, partitions roll over into multiple segment files.)
    """

    def __init__(self, topic: str, partition_id: int, data_dir: str = "data"):
        self.topic = topic
        self.partition_id = partition_id
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

        base = os.path.join(data_dir, f"{topic}-{partition_id}")
        self.segment = LogSegment(base)

    def produce(self, payload: bytes) -> int:
        return self.segment.append(payload)

    def fetch(self, offset: int, max_bytes: int = 1_048_576) -> bytes:
        return self.segment.read_from(offset, max_bytes)

    def earliest_offset(self) -> int:
        return self.segment.earliest_offset()

    def latest_offset(self) -> int:
        return self.segment.latest_offset()
