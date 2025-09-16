#!/usr/bin/env python3
"""
merge_audio_json.py
-------------------
Simplified tool to merge many per-utterance JSON files into a single main JSON database,
linking each JSON to its corresponding audio file by shared filename stem (e.g. ABC123.json <-> ABC123.wav).

Usage:
    python merge_audio_json.py --input data/jsons --output main_audio_database.json
    # Optionally control audio extensions and behavior on duplicates:
    python merge_audio_json.py --input data --output main_audio_database.json --audio-exts wav,mp3,flac --on-duplicate keep_first

Behaviors:
- Loads existing main file if present and updates it.
- For each *.json found recursively under --input:
    * Loads JSON.
    * Ensures an 'utt_id' (uses filename stem if missing).
    * Adds fields:
        - 'source_json' : relative path to the json file
        - 'audio_file'  : relative path to the first matching audio file (same stem)
        - 'added_at'    : ISO timestamp when merged (only when first added)
    * Writes a compact database:
        {
          "metadata": {...},
          "records": {
             "<utt_id>": {...record...},
             ...
          }
        }
- Duplicate handling (--on-duplicate):
    * keep_first   : keep the first record seen (default)
    * overwrite    : replace existing record data with the latest JSON read
    * keep_both    : keep the first, but record additional 'other_sources': [...] (paths)
"""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

SUPPORTED_AUDIO_EXTS = (".wav", ".mp3", ".flac", ".m4a", ".ogg", ".opus")

def find_audio_for_stem(stem: str, search_root: Path, audio_exts: tuple[str, ...]) -> str | None:
    """Find the first audio file under search_root with the given stem and one of the allowed extensions."""
    # Fast path: check sibling directories where JSON lives is not always possible, so do a recursive glob.
    # We search by each extension to avoid listing the entire tree for large datasets.
    for ext in audio_exts:
        # Use rglob with pattern like **/stem.ext
        for p in search_root.rglob(f"{stem}{ext}"):
            if p.is_file():
                return str(p.relative_to(search_root))
    return None

def load_existing_db(output_path: Path) -> dict:
    if output_path.exists():
        try:
            with output_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
                # minimal structure check
                if isinstance(data, dict) and "records" in data and "metadata" in data:
                    return data
        except Exception:
            pass
    # fresh structure
    return {
        "metadata": {
            "total_records": 0,
            "last_updated": None,
            "version": "1.0",
            "audio_linkage": "by_stem",
        },
        "records": {}
    }

def merge_jsons(input_dir: Path, output_path: Path, audio_exts: tuple[str, ...], on_duplicate: str) -> dict:
    db = load_existing_db(output_path)
    records = db["records"]
    root = input_dir.resolve()

    json_files = list(root.rglob("*.json"))
    if not json_files:
        print(f"No JSON files found under: {root}", file=sys.stderr)
        return db

    added, skipped, overwritten, noted = 0, 0, 0, 0
    for jf in sorted(json_files):
        try:
            with jf.open("r", encoding="utf-8") as f:
                rec = json.load(f)
        except Exception as e:
            print(f"[skip] {jf}: cannot load JSON ({e})", file=sys.stderr)
            skipped += 1
            continue

        stem = jf.stem
        utt_id = rec.get("utt_id") or stem
        audio_rel = find_audio_for_stem(stem, root, audio_exts)
        # annotate fields (do not clobber if exist)
        rec.setdefault("utt_id", utt_id)
        rec.setdefault("added_at", datetime.now().isoformat())
        rec["source_json"] = str(jf.relative_to(root))
        rec["audio_file"] = audio_rel  # may be None

        if utt_id in records:
            if on_duplicate == "overwrite":
                records[utt_id] = rec
                overwritten += 1
            elif on_duplicate == "keep_both":
                # keep original, but append other_sources
                orig = records[utt_id]
                others = orig.get("other_sources", [])
                if rec["source_json"] not in others:
                    others.append(rec["source_json"])
                    orig["other_sources"] = others
                    # if we found an audio file and original had none, fill it
                    if audio_rel and not orig.get("audio_file"):
                        orig["audio_file"] = audio_rel
                noted += 1
            else:
                # keep_first
                skipped += 1
        else:
            records[utt_id] = rec
            added += 1

    db["metadata"]["total_records"] = len(records)
    db["metadata"]["last_updated"] = datetime.now().isoformat()

    # write out
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

    print(f"\nDone. Saved to {output_path}")
    print(f"Added: {added} | Overwritten: {overwritten} | Skipped: {skipped} | Noted(other_sources): {noted}")
    return db

def comma_split_exts(s: str) -> tuple[str, ...]:
    raw = [x.strip().lower() for x in s.split(",") if x.strip()]
    # normalize to .ext
    norm = []
    for x in raw:
        if not x.startswith("."):
            x = "." + x
        norm.append(x)
    return tuple(norm)

def main():
    p = argparse.ArgumentParser(description="Merge many JSON files into a single main database, linking audio by shared filename stem.")
    p.add_argument("--input", "-i", type=Path, required=True, help="Directory containing JSON (and audio) files (searched recursively)")
    p.add_argument("--output", "-o", type=Path, default=Path("main_audio_database.json"), help="Path to write the merged database JSON")
    p.add_argument("--audio-exts", type=str, default="wav,mp3,flac,m4a,ogg,opus", help="Comma-separated list of audio extensions to look for")
    p.add_argument("--on-duplicate", choices=["keep_first", "overwrite", "keep_both"], default="keep_first",
                   help="How to handle duplicate utt_id when merging")
    args = p.parse_args()

    audio_exts = comma_split_exts(args.audio_exts)
    merge_jsons(args.input, args.output, audio_exts, args.on_duplicate)

if __name__ == "__main__":
    main()
