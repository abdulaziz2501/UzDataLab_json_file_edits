#!/usr/bin/env python3
import argparse
import csv
import json
import logging
import os
import re
import shutil
import sys
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple, Any, Set, DefaultDict
from collections import defaultdict

# ---------------------------
# Helpers
# ---------------------------

def normalize_text(text: str) -> str:
    """Normalize text for exact duplicate keys.
    - Unicode NFKC
    - lowercased
    - collapse whitespace
    - strip surrounding punctuation-like chars
    """
    if text is None:
        return ""
    t = unicodedata.normalize("NFKC", text)
    t = t.casefold()
    t = re.sub(r"\s+", " ", t).strip()
    # strip common trailing/leading punctuation and quotes
    t = t.strip(" \t\n\r\"'`.,;:!?-—–()[]{}")
    return t

def parse_datetime(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    # try common formats (ISO preferred)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        pass
    # try a few fallbacks if needed
    fmts = ["%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d"]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except Exception:
            continue
    return None

def similar(a: str, b: str) -> float:
    """Quick similarity for fuzzy near-duplicates."""
    return SequenceMatcher(None, a, b).ratio()

def is_jsonl_file(path: Path) -> bool:
    return path.suffix.lower() in {".jsonl", ".ndjson"}

def load_json_items(path: Path) -> Iterable[Dict[str, Any]]:
    """Yield JSON objects from a .json (single or list) or .jsonl file."""
    try:
        if is_jsonl_file(path):
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    yield json.loads(line)
        else:
            with path.open("r", encoding="utf-8") as f:
                obj = json.load(f)
            if isinstance(obj, list):
                for item in obj:
                    if isinstance(item, dict):
                        yield item
            elif isinstance(obj, dict):
                yield obj
            else:
                logging.warning(f"{path}: unsupported JSON root type: {type(obj)}")
    except Exception as e:
        logging.exception(f"Failed to read {path}: {e}")

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def find_audio_for_item(
    item: Dict[str, Any],
    json_path: Path,
    audio_exts: Tuple[str, ...],
    audio_roots: List[Path],
) -> Optional[Path]:
    """
    Try to locate the audio file for this JSON item.
    Priority:
      1) stem of the JSON file (same base name)
      2) utt_id field if present
    Searches across audio_roots with allowed extensions.
    """
    # 1) same stem as json
    stem = json_path.stem
    candidates = []
    for root in audio_roots:
        for ext in audio_exts:
            p = root / f"{stem}{ext}"
            if p.exists():
                candidates.append(p)

    # 2) utt_id as stem
    utt = item.get("utt_id")
    if utt:
        for root in audio_roots:
            for ext in audio_exts:
                p = root / f"{utt}{ext}"
                if p.exists():
                    candidates.append(p)

    if candidates:
        # If multiple, pick the first by stable ordering
        candidates.sort(key=lambda x: (str(x.parent), x.name))
        return candidates[0]
    return None

# ---------------------------
# Core data structures
# ---------------------------

@dataclass
class Record:
    text: str
    norm_text: str
    json_path: Path
    audio_path: Optional[Path]
    item: Dict[str, Any]
    duration_ms: Optional[int] = None
    created_at: Optional[datetime] = None

@dataclass
class KeptOrDropped:
    action: str  # "kept" or "dropped"
    reason: str
    text_preview: str
    json_path: str
    audio_path: str
    duration_ms: Optional[int]
    created_at: Optional[str]
    key: str

# ---------------------------
# Deduper
# ---------------------------

class Deduper:
    def __init__(
        self,
        on_duplicate: str = "keep_first",
        fuzzy_threshold: float = 0.0,  # 0 disables fuzzy
    ):
        assert on_duplicate in {"keep_first", "keep_longer", "keep_newer"}
        self.on_duplicate = on_duplicate
        self.fuzzy_threshold = fuzzy_threshold
        self.exact_map: Dict[str, Record] = {}  # norm_text -> Record
        # fuzzy buckets to reduce pairwise checks (prefix bucket)
        self.buckets: DefaultDict[str, List[Record]] = defaultdict(list)

        self.journal: List[KeptOrDropped] = []

    def decide(self, a: Record, b: Record) -> Tuple[Record, Record, str]:
        """Return (winner, loser, reason)."""
        if self.on_duplicate == "keep_first":
            reason = "duplicate (keep_first)"
            return (a, b, reason)
        elif self.on_duplicate == "keep_longer":
            da = a.duration_ms or -1
            db = b.duration_ms or -1
            if db > da:
                reason = "duplicate (keep_longer: newer has longer duration)"
                return (b, a, reason)
            else:
                reason = "duplicate (keep_longer: existing has longer or equal duration)"
                return (a, b, reason)
        else:  # keep_newer
            ta = a.created_at or datetime.min
            tb = b.created_at or datetime.min
            if tb > ta:
                reason = "duplicate (keep_newer: newer created_at)"
                return (b, a, reason)
            else:
                reason = "duplicate (keep_newer: existing is newer or equal)"
                return (a, b, reason)

    def maybe_insert(self, rec: Record):
        key = rec.norm_text

        # 1) Exact match first
        if key in self.exact_map:
            winner, loser, reason = self.decide(self.exact_map[key], rec)
            self.exact_map[key] = winner
            self.journal.append(
                KeptOrDropped(
                    action="dropped",
                    reason=reason,
                    text_preview=rec.text[:80],
                    json_path=str(rec.json_path),
                    audio_path=str(rec.audio_path) if rec.audio_path else "",
                    duration_ms=rec.duration_ms,
                    created_at=rec.created_at.isoformat() if rec.created_at else None,
                    key=key,
                )
            )
            return

        # 2) If fuzzy enabled, check near-duplicates inside a bucket
        if self.fuzzy_threshold > 0.0:
            bucket_key = key[:16]  # small prefix bucket
            for existing in self.buckets[bucket_key]:
                sim = similar(key, existing.norm_text)
                if sim >= self.fuzzy_threshold:
                    winner, loser, reason = self.decide(existing, rec)
                    if winner is not existing:
                        # replace in exact map too (must move key!)
                        if existing.norm_text in self.exact_map:
                            self.exact_map[existing.norm_text] = winner
                        else:
                            # not inserted yet; insert winner under its key
                            self.exact_map[winner.norm_text] = winner
                        # update bucket: replace
                        self.buckets[bucket_key].remove(existing)
                        self.buckets[bucket_key].append(winner)
                        self.journal.append(
                            KeptOrDropped(
                                action="dropped",
                                reason=f"near-duplicate ({sim:.2f}) | {reason}",
                                text_preview=rec.text[:80],
                                json_path=str(rec.json_path),
                                audio_path=str(rec.audio_path) if rec.audio_path else "",
                                duration_ms=rec.duration_ms,
                                created_at=rec.created_at.isoformat() if rec.created_at else None,
                                key=key,
                            )
                        )
                        return
                    else:
                        # drop new rec
                        self.journal.append(
                            KeptOrDropped(
                                action="dropped",
                                reason=f"near-duplicate ({sim:.2f}) | {reason}",
                                text_preview=rec.text[:80],
                                json_path=str(rec.json_path),
                                audio_path=str(rec.audio_path) if rec.audio_path else "",
                                duration_ms=rec.duration_ms,
                                created_at=rec.created_at.isoformat() if rec.created_at else None,
                                key=key,
                            )
                        )
                        return
            # no near-dup found; insert
            self.exact_map[key] = rec
            self.buckets[bucket_key].append(rec)
            self.journal.append(
                KeptOrDropped(
                    action="kept",
                    reason="unique (fuzzy enabled, no near-dup found)",
                    text_preview=rec.text[:80],
                    json_path=str(rec.json_path),
                    audio_path=str(rec.audio_path) if rec.audio_path else "",
                    duration_ms=rec.duration_ms,
                    created_at=rec.created_at.isoformat() if rec.created_at else None,
                    key=key,
                )
            )
            return

        # 3) Fuzzy disabled: insert as unique
        self.exact_map[key] = rec
        self.journal.append(
            KeptOrDropped(
                action="kept",
                reason="unique (exact)",
                text_preview=rec.text[:80],
                json_path=str(rec.json_path),
                audio_path=str(rec.audio_path) if rec.audio_path else "",
                duration_ms=rec.duration_ms,
                created_at=rec.created_at.isoformat() if rec.created_at else None,
                key=key,
            )
        )

# ---------------------------
# Main pipeline
# ---------------------------

def walk_files(roots: List[Path], exts: Tuple[str, ...]) -> Iterable[Path]:
    seen: Set[Path] = set()
    for root in roots:
        for p in root.rglob("*"):
            if p.is_file() and p.suffix.lower() in exts and p not in seen:
                seen.add(p)
                yield p

def main():
    parser = argparse.ArgumentParser(
        description="Deduplicate JSON transcripts by text and collect audios."
    )
    parser.add_argument(
        "--input", "-i", nargs="+", required=True,
        help="One or more folders containing JSON/JSONL files."
    )
    parser.add_argument(
        "--audio-input", nargs="+", default=[],
        help="Optional audio roots (if audio not in same folders)."
    )
    parser.add_argument(
        "--audio-exts", default=".wav,.mp3,.flac,.m4a,.ogg",
        help="Comma-separated list of audio extensions to consider."
    )
    parser.add_argument(
        "--output-audio", required=True,
        help="Folder where unique audio files will be copied."
    )
    parser.add_argument(
        "--output-json", required=True,
        help="Path to the combined JSONL file to write."
    )
    parser.add_argument(
        "--report", default="dedupe_report.csv",
        help="CSV report path (kept/dropped, reasons)."
    )
    parser.add_argument(
        "--log", default="dedupe.log",
        help="Detailed log file."
    )
    parser.add_argument(
        "--on-duplicate", choices=["keep_first", "keep_longer", "keep_newer"],
        default="keep_first",
        help="Policy for choosing among duplicates."
    )
    parser.add_argument(
        "--fuzzy", type=float, default=0.0,
        help="Fuzzy similarity threshold (0 disables; typical: 0.88–0.95)."
    )

    args = parser.parse_args()

    # Logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(args.log, encoding="utf-8"),
        ],
    )

    json_roots = [Path(p).resolve() for p in args.input]
    audio_roots = [Path(p).resolve() for p in (args.audio_input or [])]
    audio_exts = tuple(e if e.startswith(".") else f".{e}" for e in args.audio_exts.split(","))

    out_audio = Path(args.output_audio).resolve()
    out_json = Path(args.output_json).resolve()
    ensure_dir(out_audio)
    ensure_dir(out_json.parent)

    deduper = Deduper(on_duplicate=args.on_duplicate, fuzzy_threshold=args.fuzzy)

    total_json_files = 0
    total_json_items = 0

    # discover JSON files
    json_exts = (".json", ".jsonl", ".ndjson")
    json_files = list(walk_files(json_roots, json_exts))
    total_json_files = len(json_files)
    logging.info(f"Found {total_json_files} JSON files.")

    # iterate and insert
    for jpath in json_files:
        for item in load_json_items(jpath):
            text = item.get("text")
            if not text or not str(text).strip():
                logging.warning(f"Skipping item without text in {jpath}")
                continue

            norm = normalize_text(text)
            duration_ms = None
            if "duration_ms" in item:
                try:
                    duration_ms = int(item["duration_ms"])
                except Exception:
                    duration_ms = None

            created_at = parse_datetime(item.get("created_at") or item.get("date"))

            audio_path = find_audio_for_item(
                item=item,
                json_path=jpath,
                audio_exts=audio_exts,
                audio_roots=audio_roots if audio_roots else [jpath.parent],
            )

            rec = Record(
                text=text,
                norm_text=norm,
                json_path=jpath,
                audio_path=audio_path,
                item=item,
                duration_ms=duration_ms,
                created_at=created_at,
            )
            deduper.maybe_insert(rec)
            total_json_items += 1

    logging.info(f"Processed {total_json_items} items. Unique texts: {len(deduper.exact_map)}")

    # Write combined JSONL and copy audios
    kept = list(deduper.exact_map.values())
    kept.sort(key=lambda r: (r.created_at or datetime.min, r.json_path.name))

    copied = 0
    missing_audio = 0
    with out_json.open("w", encoding="utf-8") as jf:
        for r in kept:
            # write item
            jf.write(json.dumps(r.item, ensure_ascii=False) + "\n")

            # copy audio if available
            if r.audio_path and r.audio_path.exists():
                dst = out_audio / r.audio_path.name
                if not dst.exists():
                    shutil.copy2(r.audio_path, dst)
                copied += 1
            else:
                missing_audio += 1

    # Report
    with open(args.report, "w", newline="", encoding="utf-8") as cf:
        writer = csv.writer(cf)
        writer.writerow(
            ["action","reason","text_preview","json_path","audio_path","duration_ms","created_at","norm_key"]
        )
        for row in deduper.journal:
            writer.writerow([
                row.action,
                row.reason,
                row.text_preview,
                row.json_path,
                row.audio_path,
                row.duration_ms if row.duration_ms is not None else "",
                row.created_at if row.created_at else "",
                row.key
            ])

    logging.info(f"Done. Wrote {len(kept)} unique items to {out_json}")
    logging.info(f"Copied {copied} audio files to {out_audio}. Missing audio: {missing_audio}")
    logging.info(f"Report saved to {args.report}")

if __name__ == "__main__":
    main()
