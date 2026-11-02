#!/usr/bin/env python3
"""Writes seeds/mqtt_packet from the golden vectors.

Each seed is a selector byte (0 before CONNECT, 1 for 3.1.1, 2 for 5.0)
followed by well-formed packets. Run from this directory:

    python3 gen_seeds.py
"""
import pathlib

here = pathlib.Path(__file__).resolve().parent
out = here / "seeds" / "mqtt_packet"
out.mkdir(parents=True, exist_ok=True)
for old in out.glob("*"):
    old.unlink()

vectors = here.parent / "tests" / "vectors" / "rumqttc.txt"
count = 0
for line in vectors.read_text().splitlines():
    if line.startswith("#") or not line:
        continue
    version, name, hexed = line.split(" ")
    packet = bytes.fromhex(hexed)
    selector = 0 if packet[0] >> 4 == 1 else (1 if version == "v4" else 2)
    (out / f"{version}_{name}").write_bytes(bytes([selector]) + packet)
    count += 1

# Forms rumqttc does not write: AUTH, the optional short forms, an empty client
# identifier, and several packets in one read.
extra = {
    "v5_auth": (2, "f00e180c150005534352414d16000178"),
    "v5_puback_bare": (2, "40020007"),
    "v5_puback_reason_only": (2, "4003000710"),
    "v5_disconnect_empty": (2, "e000"),
    "v5_disconnect_reason_only": (2, "e00195"),
    "v5_pipelined": (2, "c000d000c000"),
    "v4_connect_empty_client_id": (0, "100c00044d5154540402003c0000"),
}
for name, (selector, hexed) in extra.items():
    (out / name).write_bytes(bytes([selector]) + bytes.fromhex(hexed))
    count += 1
print(f"wrote {count} seeds to {out}")
