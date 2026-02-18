from __future__ import annotations

import socket
from typing import Final

SB: Final[bytes] = b"\x0b"
EB: Final[bytes] = b"\x1c"
CR: Final[bytes] = b"\x0d"


def send_mllp_message(host: str, port: int, message: str) -> bool:
    packet = SB + message.encode("utf-8") + EB + CR
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            sock.sendall(packet)
            try:
                sock.recv(4096)
            except socket.timeout:
                pass
        return True
    except ConnectionRefusedError:
        return False
    except OSError:
        return False


if __name__ == "__main__":
    SAMPLE = "MSH|^~\\&|GEN|ICU|MON|ICU|20260101120000||ORU^R01|MSG001|P|2.4\rPID|1||12345||DOE^JOHN||19800101|M\rPV1|1|I|WARD^A^BED01\rOBR|1|||VITALS\rOBX|1|NM|HR^HeartRate||72|bpm|||N\rOBX|2|NM|SPO2^SpO2||98|%|||N\rOBX|3|NM|RESP^RespRate||16|rpm|||N\r"
    if send_mllp_message("127.0.0.1", 2575, SAMPLE):
        print("sent")
    else:
        print("send failed")
