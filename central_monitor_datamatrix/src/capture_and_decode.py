from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

from PIL import Image

from dm_codec import decode_payload, verify_crc32
from dm_decoder import decode_datamatrix_from_image


def iter_input_images(path: Path, latest_n: int) -> Iterable[Path]:
    if path.is_file():
        yield path
        return

    exts = {".png", ".jpg", ".jpeg", ".bmp"}
    images: List[Path] = [p for p in path.iterdir() if p.suffix.lower() in exts]
    images.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    for p in reversed(images[:latest_n]):
        yield p


def crop_bottom_right(image: Image.Image, roi_size: int) -> Image.Image:
    w, h = image.size
    left = max(0, w - roi_size)
    top = max(0, h - roi_size)
    return image.crop((left, top, w, h))


def append_jsonl(records: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("input", help="screenshot image file or folder")
    ap.add_argument("--latest-n", type=int, default=10)
    ap.add_argument("--roi-size", type=int, default=420)
    ap.add_argument("--output-root", default="../dataset")
    args = ap.parse_args()

    input_path = Path(args.input)
    today_dir = datetime.now().strftime("%Y%m%d")
    out_path = Path(args.output_root) / today_dir / "dm_results.jsonl"

    results: list[dict] = []
    for img_path in iter_input_images(input_path, args.latest_n):
        try:
            img = Image.open(img_path).convert("RGB")
            roi = crop_bottom_right(img, args.roi_size)
            blob = decode_datamatrix_from_image(roi)
            if blob is None:
                print(f"decode failed: {img_path}")
                continue

            payload = decode_payload(blob)
            crc_ok = verify_crc32(payload)
            if not crc_ok:
                print(f"crc mismatch: {img_path}")
                continue

            record = {
                "source_image": str(img_path),
                "decoded_at": datetime.now().isoformat(),
                "crc_ok": crc_ok,
                "payload": payload,
            }
            results.append(record)
            print(f"ok: {img_path}")
        except Exception as exc:
            print(f"error [{img_path}]: {exc}")

    append_jsonl(results, out_path)
    print(f"saved: {out_path} ({len(results)} records)")


if __name__ == "__main__":
    main()
