from __future__ import annotations

from typing import Optional, Union

import numpy as np
from PIL import Image

try:
    from pylibdmtx.pylibdmtx import decode
except Exception as exc:  # pragma: no cover
    decode = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


ImageLike = Union[np.ndarray, Image.Image]


def decode_datamatrix_from_image(image: ImageLike) -> Optional[bytes]:
    if decode is None:
        raise RuntimeError(f"pylibdmtx unavailable: {_IMPORT_ERROR}")

    if isinstance(image, np.ndarray):
        pil_img = Image.fromarray(image)
    else:
        pil_img = image

    decoded = decode(pil_img)
    if not decoded:
        return None
    return decoded[0].data
