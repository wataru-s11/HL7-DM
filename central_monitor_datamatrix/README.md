# central_monitor_datamatrix

HL7受信結果（ベッド別vitals）から、PHIを除外した payload を DataMatrix として右下表示し、スクリーンショットから復号して JSONL 保存する最小実装です。

## ディレクトリ

```text
central_monitor_datamatrix/
  src/
    hl7_sender.py
    hl7_receiver.py
    hl7_parser.py
    generator.py
    monitor.py
    dm_payload.py
    dm_codec.py
    dm_render.py
    dm_decoder.py
    capture_and_decode.py
  dataset/
  requirements.txt
  README.md
```

## セットアップ

```bash
cd central_monitor_datamatrix
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 実行手順（最小動作確認）

1. **receiver起動（HL7受信→monitor_cache更新）**
   ```bash
   cd src
   python hl7_receiver.py --host 0.0.0.0 --port 2575 --cache monitor_cache.json
   ```
2. **generator起動（テストHL7送信）**
   ```bash
   cd src
   python generator.py --host 127.0.0.1 --port 2575 --interval 1
   ```
   `send failed` が出る場合は、receiver がまだ起動していない/ポートが違う可能性があります。
3. **monitor起動（GUI表示＋右下DataMatrix）**
   ```bash
   cd src
   python monitor.py --cache monitor_cache.json --refresh-ms 1000
   ```
4. **画面スクショpngを用意して decode**
   ```bash
   cd src
   python capture_and_decode.py /path/to/screenshot.png --output-root ../dataset
   ```
   フォルダを渡す場合は最新 `--latest-n` 枚を処理します。
5. **結果確認**
   `dataset/YYYYMMDD/dm_results.jsonl` に `crc_ok=true` のレコードが追記されます。

## DataMatrix payload仕様（最小）

```json
{
  "v": 1,
  "ts": "2026-01-01T12:00:00+00:00",
  "seq": 12,
  "beds": {
    "BED01": {
      "bed_ts": "...",
      "vitals": {
        "HR": {"value": 72, "unit": "bpm", "flag": "N"}
      }
    }
  },
  "crc32": "8hex"
}
```

- `v` は schema_version。
- `beds` には vitals だけを入れ、`patient` 情報は破棄（PHI除外）。
- CRC32 は `crc32` フィールドを除いた payload の canonical JSON から算出・検証。

## 依存関係メモ（DataMatrix）

- 第一候補は `pylibdmtx`（encode/decodeの両方を使用）。
- Windows で `pylibdmtx` が libdmtx の DLL 解決に失敗する場合は、DataMatrix を維持したまま以下を検討:
  - `libdmtx` 本体（DLL）をシステムへ導入して PATH を通す
  - もしくは `zxing-cpp` Python バインディング等、DataMatrix対応の別実装へ切替
- QRコードへの退避は行わない。

## 注意

- この最小実装は研究用サンプルです。医療現場で使う場合は監査ログ、再送制御、暗号化、署名等を追加してください。
