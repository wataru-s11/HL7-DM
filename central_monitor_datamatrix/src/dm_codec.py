from __future__ import annotations

import json
import zlib
from typing import Any, Dict


def _canonical_json_bytes(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def add_crc32(payload_without_crc: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(payload_without_crc)
    crc = zlib.crc32(_canonical_json_bytes(data)) & 0xFFFFFFFF
    data["crc32"] = f"{crc:08x}"
    return data


def verify_crc32(payload: Dict[str, Any]) -> bool:
    given = str(payload.get("crc32", "")).lower()
    data = dict(payload)
    data.pop("crc32", None)
    expected = f"{(zlib.crc32(_canonical_json_bytes(data)) & 0xFFFFFFFF):08x}"
    return given == expected


def encode_payload(payload: Dict[str, Any]) -> bytes:
    return zlib.compress(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), level=9)


def decode_payload(blob: bytes) -> Dict[str, Any]:
    raw = zlib.decompress(blob)
    return json.loads(raw.decode("utf-8"))
