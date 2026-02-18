from __future__ import annotations

from io import BytesIO

from PIL import Image

try:
    from pylibdmtx.pylibdmtx import encode
except Exception as exc:  # pragma: no cover
    encode = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


def render_datamatrix(data: bytes, size: int = 280) -> Image.Image:
    if encode is None:
        raise RuntimeError(f"pylibdmtx unavailable: {_IMPORT_ERROR}")

    encoded = encode(data)
    image = Image.open(BytesIO(encoded.png)).convert("RGB")
    return image.resize((size, size), Image.NEAREST)
