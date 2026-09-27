#!/usr/bin/env python3
"""Write the committed seeds for the Kafka fuzz targets.

One well-formed input per shape a client sends, so a run starts from real
framing instead of spending its budget finding it. Re-run after changing the
set; the output is deterministic.

    python3 gen_seeds.py
"""

import gzip
import pathlib
import struct

HERE = pathlib.Path(__file__).resolve().parent


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82F63B78 if crc & 1 else 0)
    return crc ^ 0xFFFFFFFF


def varint(n: int) -> bytes:
    n = (n << 1) ^ (n >> 63)  # zigzag
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def record(offset_delta: int, value: bytes, key: bytes | None = None) -> bytes:
    body = b"\x00" + varint(0) + varint(offset_delta)
    body += varint(-1) if key is None else varint(len(key)) + key
    body += varint(len(value)) + value + varint(0)
    return varint(len(body)) + body


def batch(values: list[bytes], codec: int = 0, producer_id: int = -1) -> bytes:
    records = b"".join(record(i, v) for i, v in enumerate(values))
    if codec == 1:
        records = gzip.compress(records, mtime=0)
    after_crc = struct.pack(
        ">hiqqqhii",
        codec,  # attributes
        len(values) - 1,  # last offset delta
        1_700_000_000_000,
        1_700_000_000_000,
        producer_id,
        0 if producer_id >= 0 else -1,
        0 if producer_id >= 0 else -1,
        len(values),
    ) + records
    after_length = struct.pack(">ibI", -1, 2, crc32c(after_crc)) + after_crc
    return struct.pack(">qi", 0, len(after_length)) + after_length


def string(s: str | None) -> bytes:
    if s is None:
        return struct.pack(">h", -1)
    raw = s.encode()
    return struct.pack(">h", len(raw)) + raw


def header(api: int, version: int, correlation: int = 1) -> bytes:
    # Request header v1: every non-flexible version used below.
    return struct.pack(">hhi", api, version, correlation) + string("fuzz")


def produce(records: bytes) -> bytes:
    body = string(None) + struct.pack(">hi", -1, 30_000)
    body += struct.pack(">i", 1) + string("default.orders")
    body += struct.pack(">i", 1) + struct.pack(">i", 0)
    body += struct.pack(">i", len(records)) + records
    return header(0, 3) + body


def fetch() -> bytes:
    body = struct.pack(">iiiib", -1, 500, 1, 1 << 20, 0)
    body += struct.pack(">i", 1) + string("default.orders")
    body += struct.pack(">i", 1) + struct.pack(">iqi", 0, 0, 1 << 20)
    return header(1, 4) + body


def main() -> None:
    requests = {
        "api_versions": header(18, 2),
        "metadata_all": header(3, 1) + struct.pack(">i", -1),
        "metadata_one": header(3, 1) + struct.pack(">i", 1) + string("default.orders"),
        "produce": produce(batch([b"hello", b"world"])),
        "produce_gzip": produce(batch([b"a" * 64, b"b" * 64], codec=1)),
        "produce_idempotent": produce(batch([b"x"], producer_id=7)),
        "fetch": fetch(),
        "find_coordinator": header(10, 1) + string("group") + b"\x00",
        "sasl_handshake": header(17, 1) + string("PLAIN"),
        "init_producer_id": header(22, 0) + string(None) + struct.pack(">i", 60_000),
    }
    records = {
        "plain": batch([b"hello", b"world"]),
        "gzip": batch([b"a" * 64, b"b" * 64], codec=1),
        "idempotent": batch([b"x"], producer_id=7),
        "two_batches": batch([b"one"]) + batch([b"two"]),
    }
    for target, seeds in (("kafka_request", requests), ("kafka_records", records)):
        out = HERE / "seeds" / target
        out.mkdir(parents=True, exist_ok=True)
        for name, data in seeds.items():
            (out / name).write_bytes(data)


if __name__ == "__main__":
    main()
