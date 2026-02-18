from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from dm_codec import add_crc32


def _sanitize_vital_entry(vital: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "value": vital.get("value"),
        "unit": vital.get("unit", ""),
        "flag": vital.get("flag", ""),
    }


def make_payload(monitor_cache_dict: Dict[str, Any], seq: int, schema_version: int = 1) -> Dict[str, Any]:
    beds_in = monitor_cache_dict.get("beds", {}) if isinstance(monitor_cache_dict, dict) else {}
    beds_out: Dict[str, Any] = {}

    for bed_name, bed_info in beds_in.items():
        vitals = bed_info.get("vitals", {}) if isinstance(bed_info, dict) else {}
        beds_out[bed_name] = {
            "vitals": {k: _sanitize_vital_entry(v) for k, v in vitals.items() if isinstance(v, dict)},
            "bed_ts": bed_info.get("ts"),
        }

    payload_wo_crc = {
        "v": schema_version,
        "ts": datetime.now(timezone.utc).isoformat(),
        "seq": seq,
        "beds": beds_out,
    }
    return add_crc32(payload_wo_crc)
