from __future__ import annotations

import argparse
import json
import tkinter as tk
from pathlib import Path
from tkinter import ttk

from PIL import ImageTk

from dm_codec import encode_payload
from dm_payload import make_payload
from dm_render import render_datamatrix


class MonitorApp:
    def __init__(self, root: tk.Tk, cache_path: Path, refresh_ms: int = 1000) -> None:
        self.root = root
        self.cache_path = cache_path
        self.refresh_ms = refresh_ms
        self.seq = 0
        self.dm_photo = None

        self.root.title("Central Monitor + DataMatrix")
        self.root.geometry("1200x700")
        self.root.configure(bg="#101820")

        self.text = tk.Text(root, font=("Consolas", 13), bg="#0b2239", fg="#f2f6ff", height=24, width=90)
        self.text.place(x=20, y=20)

        self.dm_label = ttk.Label(root)
        self.dm_label.place(relx=1.0, rely=1.0, x=-20, y=-20, anchor="se")

        self.status_var = tk.StringVar(value="ready")
        ttk.Label(root, textvariable=self.status_var).place(x=20, y=660)

    def _load_cache(self) -> dict:
        if not self.cache_path.exists():
            return {"beds": {}}
        try:
            return json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception as exc:
            self.status_var.set(f"cache load error: {exc}")
            return {"beds": {}}

    def _render_text(self, cache: dict) -> None:
        self.text.delete("1.0", tk.END)
        for bed, info in cache.get("beds", {}).items():
            self.text.insert(tk.END, f"[{bed}] ts={info.get('ts')}\n")
            vitals = info.get("vitals", {})
            for key, val in vitals.items():
                self.text.insert(
                    tk.END,
                    f"  - {key}: {val.get('value')} {val.get('unit', '')} flag={val.get('flag', '')}\n",
                )
            self.text.insert(tk.END, "\n")

    def _update_dm(self, cache: dict) -> None:
        self.seq += 1
        payload = make_payload(cache, seq=self.seq, schema_version=1)
        blob = encode_payload(payload)
        dm_image = render_datamatrix(blob, size=320)
        self.dm_photo = ImageTk.PhotoImage(dm_image)
        self.dm_label.configure(image=self.dm_photo)
        self.status_var.set(f"seq={self.seq} crc={payload['crc32']} bytes={len(blob)}")

    def tick(self) -> None:
        cache = self._load_cache()
        self._render_text(cache)
        try:
            self._update_dm(cache)
        except Exception as exc:
            self.status_var.set(f"DataMatrix error: {exc}")
        self.root.after(self.refresh_ms, self.tick)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default="monitor_cache.json")
    ap.add_argument("--refresh-ms", type=int, default=1000)
    args = ap.parse_args()

    root = tk.Tk()
    app = MonitorApp(root, Path(args.cache), refresh_ms=args.refresh_ms)
    app.tick()
    root.mainloop()


if __name__ == "__main__":
    main()
