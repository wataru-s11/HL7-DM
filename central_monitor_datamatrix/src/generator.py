from __future__ import annotations

import argparse
import random
import time
from datetime import datetime

from hl7_sender import send_mllp_message


def build_message(bed: str, msg_id: int) -> str:
    hr = random.randint(55, 110)
    spo2 = random.randint(90, 100)
    rr = random.randint(10, 24)
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    return (
        f"MSH|^~\\&|GEN|ICU|MON|ICU|{now}||ORU^R01|MSG{msg_id:06d}|P|2.4\r"
        "PID|1||12345||DOE^JOHN||19800101|M\r"
        f"PV1|1|I|WARD^A^{bed}\r"
        "OBR|1|||VITALS\r"
        f"OBX|1|NM|HR^HeartRate||{hr}|bpm|||N\r"
        f"OBX|2|NM|SPO2^SpO2||{spo2}|%|||N\r"
        f"OBX|3|NM|RESP^RespRate||{rr}|rpm|||N\r"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=2575)
    ap.add_argument("--interval", type=float, default=1.0)
    args = ap.parse_args()

    msg_id = 1
    beds = ["BED01", "BED02"]
    while True:
        for bed in beds:
            send_mllp_message(args.host, args.port, build_message(bed, msg_id))
            msg_id += 1
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
