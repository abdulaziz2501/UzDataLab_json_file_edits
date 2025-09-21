import streamlit as st
import json
import os
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
from difflib import SequenceMatcher
import re
import pandas as pd
import shutil
from pathlib import Path
import zipfile
import io

# ==========================
# Helper Functions
# ==========================

AUDIO_EXTS = {".wav", ".mp3", ".m4a", ".flac", ".ogg", ".opus", ".aac"}


def normalize_text(s: str, *, unique_word_signature: bool = False) -> str:
    """
    Strong normalization for deduping texts.
    🔎 Uzbek izoh: bu funksiya matnni taqqoslash uchun "tozalab" beradi.
    """
    if not s:
        return ""
    # Unicode normalize
    import unicodedata
    s = unicodedata.normalize("NFKC", s)
    # Lowercase
    s = s.lower()
    # Replace punctuation & symbols with space
    s = re.sub(r"[^\w\s''ʼ-]+", " ", s, flags=re.UNICODE)
    # Collapse spaces
    s = re.sub(r"\s+", " ", s).strip()

    if unique_word_signature:
        # Signature by unique words (ignores order & duplicates)
        words = sorted(set(s.split()))
        s = " ".join(words)

    return s


def file_stem(name: str) -> str:
    return Path(name).stem


def parse_created_at(s: str) -> Optional[str]:
    try:
        from dateutil import parser as dtparser
        return dtparser.isoparse(s).isoformat()
    except Exception:
        return None


class SmartAudioDataManager:
    def __init__(self, main_db_path: str = "main_audio_database.json",
                 similarity_threshold: float = 0.85,
                 unique_word_signature: bool = False):
        """
        main_db_path: asosiy ma'lumotlar bazasi
        similarity_threshold: matn o'xshashlik chegarasi (0.0-1.0)
        unique_word_signature: so'z tartibini e'tiborga olmaslik
        """
        self.main_db_path = main_db_path
        self.similarity_threshold = similarity_threshold
        self.unique_word_signature = unique_word_signature
        self.main_database = self.load_main_database()

    def clean_text(self, text: str) -> str:
        """Matnni taqqoslash uchun tozalash"""
        return normalize_text(text, unique_word_signature=self.unique_word_signature)

    def calculate_text_similarity(self, text1: str, text2: str) -> float:
        """Ikki matn orasidagi o'xshashlikni hisoblash"""
        clean_text1 = self.clean_text(text1)
        clean_text2 = self.clean_text(text2)

        if not clean_text1 or not clean_text2:
            return 0.0

        similarity = SequenceMatcher(None, clean_text1, clean_text2).ratio()
        return similarity

    def create_text_hash(self, text: str) -> str:
        """Matn uchun hash yaratish"""
        clean_text = self.clean_text(text)
        return hashlib.sha256(clean_text.encode('utf-8')).hexdigest()[:16]

    def find_similar_records(self, new_text: str) -> List[Tuple[str, Dict, float]]:
        """O'xshash matnlarni topish"""
        similar_records = []

        for record_id, record in self.main_database["records"].items():
            existing_text = record.get("text", "")
            similarity = self.calculate_text_similarity(new_text, existing_text)

            if similarity >= self.similarity_threshold:
                similar_records.append((record_id, record, similarity))

        similar_records.sort(key=lambda x: x[2], reverse=True)
        return similar_records

    def load_main_database(self) -> Dict[str, Any]:
        """Ma'lumotlar bazasini yuklash"""
        if os.path.exists(self.main_db_path):
            try:
                with open(self.main_db_path, 'r', encoding='utf-8') as file:
                    data = json.load(file)
                    if isinstance(data, list):
                        new_format = {
                            "metadata": {
                                "total_records": len(data),
                                "last_updated": datetime.now().isoformat(),
                                "version": "3.0",
                                "duplicate_policy": "smart_detection"
                            },
                            "records": {item.get("utt_id", f"record_{i}"): item
                                        for i, item in enumerate(data)},
                            "text_hashes": {},
                            "settings": {
                                "similarity_threshold": self.similarity_threshold,
                                "unique_word_signature": self.unique_word_signature
                            }
                        }
                        self.save_main_database(new_format)
                        return new_format
                    return data
            except (json.JSONDecodeError, FileNotFoundError):
                pass

        return {
            "metadata": {
                "total_records": 0,
                "last_updated": datetime.now().isoformat(),
                "version": "3.0",
                "duplicate_policy": "smart_detection"
            },
            "records": {},
            "text_hashes": {},
            "settings": {
                "similarity_threshold": self.similarity_threshold,
                "unique_word_signature": self.unique_word_signature
            }
        }

    def save_main_database(self, data: Dict[str, Any] = None):
        """Ma'lumotlar bazasini saqlash"""
        if data is None:
            data = self.main_database

        with open(self.main_db_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=2)

    def generate_unique_id(self, record: Dict[str, Any], filename: str) -> str:
        """ID yaratish"""
        if "utt_id" in record and record["utt_id"]:
            base_id = record["utt_id"]
        else:
            base_id = file_stem(filename)

        original_id = base_id
        counter = 1
        while base_id in self.main_database["records"]:
            base_id = f"{original_id}_{counter}"
            counter += 1

        return base_id

    def add_record_streamlit(self, new_record: dict, filename: str,
                             action_on_duplicate: str = "ask", folder_path: str = None) -> Dict[str, Any]:
        """Streamlit uchun record qo'shish"""
        try:
            new_text = new_record.get("text", "")

            if not new_text:
                return {"status": "error", "message": "⚠️ Matn topilmadi", "filename": filename}

            similar_records = self.find_similar_records(new_text)

            result = {
                "status": "unknown",
                "filename": filename,
                "folder_path": folder_path,
                "new_text": new_text,
                "similar_count": len(similar_records),
                "similar_records": similar_records[:3]
            }

            if similar_records:
                best_match = similar_records[0]
                similarity_percent = int(best_match[2] * 100)

                result["best_match"] = {
                    "id": best_match[0],
                    "text": best_match[1].get("text", ""),
                    "similarity": similarity_percent,
                    "speaker_id": best_match[1].get("speaker_id"),
                    "created_at": best_match[1].get("created_at")
                }

                if action_on_duplicate == "skip":
                    result["status"] = "skipped"
                    result["message"] = f"⏭️ Takroriy matn ({similarity_percent}% o'xshash), o'tkazib yuborildi"
                    return result

                elif action_on_duplicate == "update_existing":
                    existing_id = best_match[0]
                    existing_record = self.main_database["records"][existing_id]

                    if "duration_ms" in new_record:
                        existing_record["duration_ms"] = new_record["duration_ms"]
                    if "created_at" in new_record:
                        existing_record["last_recorded_at"] = new_record["created_at"]

                    existing_record["updated_at"] = datetime.now().isoformat()
                    existing_record["source_files"] = existing_record.get("source_files", []) + [filename]

                    result["status"] = "updated"
                    result["message"] = f"🔄 Mavjud record yangilandi: {existing_id}"
                    result["updated_id"] = existing_id
                    return result

            # Yangi record qo'shish
            unique_id = self.generate_unique_id(new_record, filename)

            # Normalize created_at if present
            if "created_at" in new_record:
                new_record["created_at"] = parse_created_at(new_record["created_at"]) or new_record["created_at"]

            if similar_records:
                new_record["is_potential_duplicate"] = True
                new_record["similar_to"] = [r[0] for r in similar_records[:3]]
                new_record["max_similarity"] = similar_records[0][2]
            else:
                new_record["is_potential_duplicate"] = False

            new_record["utt_id"] = unique_id
            new_record["source_file"] = filename
            new_record["source_folder"] = folder_path
            new_record["added_at"] = datetime.now().isoformat()
            new_record["text_hash"] = self.create_text_hash(new_text)

            self.main_database["records"][unique_id] = new_record
            self.main_database["metadata"]["total_records"] += 1
            self.main_database["metadata"]["last_updated"] = datetime.now().isoformat()

            text_hash = new_record["text_hash"]
            if text_hash not in self.main_database["text_hashes"]:
                self.main_database["text_hashes"][text_hash] = []
            self.main_database["text_hashes"][text_hash].append(unique_id)

            result["status"] = "added"
            result["message"] = f"✅ Yangi record qo'shildi: {unique_id}"
            result["new_id"] = unique_id

            return result

        except Exception as e:
            return {
                "status": "error",
                "message": f"❌ Xatolik: {str(e)}",
                "filename": filename,
                "folder_path": folder_path
            }

    def get_available_source_folders(self) -> List[str]:
        """Loyihadagi mavjud papkalarni topish"""
        available_folders = []
        current_dir = os.getcwd()

        try:
            for item in os.listdir(current_dir):
                item_path = os.path.join(current_dir, item)
                if os.path.isdir(item_path) and not item.startswith('.'):
                    has_json = False
                    has_audio = False

                    try:
                        for file in os.listdir(item_path):
                            if file.endswith('.json'):
                                has_json = True
                            if any(file.lower().endswith(ext) for ext in AUDIO_EXTS):
                                has_audio = True
                    except PermissionError:
                        continue

                    if has_json or has_audio:
                        available_folders.append(item)

        except Exception:
            pass

        return sorted(available_folders)

    def process_folder_files(self, folder_path: str, action_on_duplicate: str = "skip") -> Dict[str, Any]:
        """Butun papkadagi JSON fayllarni qayta ishlash"""
        try:
            if not os.path.exists(folder_path):
                return {"status": "error", "message": f"📁 Papka topilmadi: {folder_path}"}

            results = {
                "status": "success",
                "folder_path": folder_path,
                "total_files": 0,
                "processed_files": 0,
                "added": 0,
                "updated": 0,
                "skipped": 0,
                "errors": 0,
                "details": []
            }

            json_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.json')]
            results["total_files"] = len(json_files)

            if not json_files:
                return {"status": "warning", "message": f"📄 Papkada JSON fayllar topilmadi: {folder_path}"}

            for json_file in json_files:
                file_path = os.path.join(folder_path, json_file)

                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        file_content = json.load(f)

                    relative_folder = os.path.relpath(folder_path, os.getcwd())

                    result = self.add_record_streamlit(
                        file_content,
                        json_file,
                        action_on_duplicate,
                        folder_path=relative_folder
                    )

                    results["details"].append(result)
                    results[result["status"]] += 1
                    results["processed_files"] += 1

                except json.JSONDecodeError:
                    error_result = {
                        "status": "error",
                        "filename": json_file,
                        "folder_path": os.path.relpath(folder_path, os.getcwd()),
                        "message": "❌ JSON format xatosi"
                    }
                    results["details"].append(error_result)
                    results["errors"] += 1

                except Exception as e:
                    error_result = {
                        "status": "error",
                        "filename": json_file,
                        "folder_path": os.path.relpath(folder_path, os.getcwd()),
                        "message": f"❌ Xatolik: {str(e)}"
                    }
                    results["details"].append(error_result)
                    results["errors"] += 1

            return results

        except Exception as e:
            return {"status": "error", "message": f"❌ Papka qayta ishlashda xatolik: {str(e)}"}

    def find_unique_texts(self) -> Dict[str, List[str]]:
        """Faqat noyob (takrorlanmaydigan) matnlarni topish"""
        text_groups = {}

        for record_id, record in self.main_database["records"].items():
            text = record.get("text", "")
            if text:
                text_hash = self.create_text_hash(text)
                if text_hash not in text_groups:
                    text_groups[text_hash] = []
                text_groups[text_hash].append(record_id)

        unique_texts = {text_hash: ids for text_hash, ids in text_groups.items() if len(ids) == 1}
        return unique_texts

    def get_unique_texts_info(self) -> Dict[str, Any]:
        """Noyob matnlar haqida statistik ma'lumot"""
        unique_texts = self.find_unique_texts()

        info = {
            "total_unique_texts": len(unique_texts),
            "unique_records": [],
            "total_size_bytes": 0,
            "categories": {},
            "speakers": {},
            "languages": set()
        }

        for text_hash, record_ids in unique_texts.items():
            record_id = record_ids[0]
            record = self.main_database["records"].get(record_id)

            if record:
                record_json = json.dumps(record, ensure_ascii=False)
                record_size = len(record_json.encode('utf-8'))
                info["total_size_bytes"] += record_size

                category = record.get("category", "unknown")
                info["categories"][category] = info["categories"].get(category, 0) + 1

                speaker = record.get("speaker_id", "unknown")
                info["speakers"][speaker] = info["speakers"].get(speaker, 0) + 1

                lang = record.get("lang")
                if lang:
                    info["languages"].add(lang)

                info["unique_records"].append({
                    "record_id": record_id,
                    "text_preview": record.get("text", "")[:100] + "..." if len(
                        record.get("text", "")) > 100 else record.get("text", ""),
                    "speaker_id": record.get("speaker_id"),
                    "category": record.get("category"),
                    "source_file": record.get("source_file"),
                    "source_folder": record.get("source_folder"),
                    "size_bytes": record_size
                })

        info["languages"] = list(info["languages"])
        info["total_size_kb"] = round(info["total_size_bytes"] / 1024, 2)
        info["total_size_mb"] = round(info["total_size_bytes"] / (1024 * 1024), 2)

        return info

    def create_unique_dataset_bundle(self, destination_folder: str) -> Dict[str, Any]:
        """Noyob matnlar uchun to'liq ma'lumotlar to'plami yaratish (app.py dan ilhomlangan)"""
        try:
            os.makedirs(destination_folder, exist_ok=True)

            unique_texts = self.find_unique_texts()
            unique_records = []

            # Noyob recordlarni tayyorlash
            for text_hash, record_ids in unique_texts.items():
                record_id = record_ids[0]
                record = self.main_database["records"].get(record_id)

                if record:
                    unique_records.append({
                        "utt_id": record.get("utt_id"),
                        "text": record.get("text"),
                        "duration_ms": record.get("duration_ms"),
                        "speaker_id": record.get("speaker_id"),
                        "created_at": record.get("created_at"),
                        "sample_rate": record.get("sample_rate"),
                        "bit_depth": record.get("bit_depth"),
                        "lang": record.get("lang"),
                        "gender": record.get("gender"),
                        "device": record.get("device"),
                        "region": record.get("region"),
                        "sentiment": record.get("sentiment"),
                        "annotation": record.get("annotation"),
                        "category": record.get("category"),
                        "source_file": record.get("source_file"),
                        "source_folder": record.get("source_folder"),
                        "_text_hash": text_hash
                    })

            # JSONL fayl yaratish
            jsonl_path = os.path.join(destination_folder, "unique_dataset.jsonl")
            with open(jsonl_path, 'w', encoding='utf-8') as f:
                for record in unique_records:
                    clean_record = {k: v for k, v in record.items() if not k.startswith("_")}
                    f.write(json.dumps(clean_record, ensure_ascii=False) + "\n")

            # ZIP bundle yaratish
            zip_path = os.path.join(destination_folder, "unique_dataset_bundle.zip")
            with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                # JSON fayllar
                for record in unique_records:
                    meta = {k: v for k, v in record.items() if not k.startswith("_")}
                    zf.writestr(f"data/json/{record['utt_id']}.json",
                                json.dumps(meta, ensure_ascii=False, indent=2))

                # Manifest CSV
                import csv
                manifest = io.StringIO()
                cw = csv.writer(manifest)
                cw.writerow(["utt_id", "text_len", "source_folder", "source_file",
                             "speaker_id", "category", "lang", "created_at"])

                for record in unique_records:
                    cw.writerow([
                        record.get("utt_id", ""),
                        len(record.get("text", "")),
                        record.get("source_folder", ""),
                        record.get("source_file", ""),
                        record.get("speaker_id", ""),
                        record.get("category", ""),
                        record.get("lang", ""),
                        record.get("created_at", "")
                    ])

                zf.writestr("manifest.csv", manifest.getvalue())

                # Summary JSON
                summary = {
                    "created_at": datetime.now().isoformat(),
                    "total_unique_records": len(unique_records),
                    "similarity_threshold": self.similarity_threshold,
                    "unique_word_signature": self.unique_word_signature,
                    "total_size_mb": round(sum(len(json.dumps(r, ensure_ascii=False).encode())
                                               for r in unique_records) / (1024 * 1024), 2)
                }
                zf.writestr("dataset_summary.json", json.dumps(summary, ensure_ascii=False, indent=2))

            return {
                "status": "success",
                "unique_count": len(unique_records),
                "jsonl_path": jsonl_path,
                "zip_path": zip_path,
                "destination_folder": destination_folder
            }

        except Exception as e:
            return {"status": "error", "message": f"❌ Xatolik: {str(e)}"}

    def find_all_duplicates(self) -> Dict[str, List[str]]:
        """Barcha takroriy matnlarni topish"""
        text_groups = {}

        for record_id, record in self.main_database["records"].items():
            text = record.get("text", "")
            clean_text = self.clean_text(text)

            if clean_text:
                if clean_text not in text_groups:
                    text_groups[clean_text] = []
                text_groups[clean_text].append(record_id)

        duplicates = {text: ids for text, ids in text_groups.items() if len(ids) > 1}
        return duplicates

    def get_duplicate_statistics(self) -> Dict[str, Any]:
        """Takroriy matnlar statistikasi"""
        duplicates = self.find_all_duplicates()

        total_duplicate_groups = len(duplicates)
        total_duplicate_records = sum(len(ids) for ids in duplicates.values())

        duplicate_details = []
        total_duplicate_size = 0

        for text, ids in duplicates.items():
            group_size = 0
            group_duration = 0
            speakers = set()
            categories = set()

            for record_id in ids:
                record = self.main_database["records"][record_id]

                record_json = json.dumps(record, ensure_ascii=False)
                record_size = len(record_json.encode('utf-8'))
                group_size += record_size

                duration = record.get("duration_ms", 0)
                if duration:
                    group_duration += duration

                speaker_id = record.get("speaker_id")
                if speaker_id:
                    speakers.add(str(speaker_id))

                category = record.get("category")
                if category:
                    categories.add(category)

            total_duplicate_size += group_size

            duplicate_details.append({
                "text": text,
                "record_ids": ids,
                "count": len(ids),
                "size_bytes": group_size,
                "size_kb": round(group_size / 1024, 2),
                "size_mb": round(group_size / (1024 * 1024), 2),
                "duration_ms": group_duration,
                "duration_minutes": round(group_duration / 60000, 2),
                "speakers": list(speakers),
                "speaker_count": len(speakers),
                "categories": list(categories)
            })

        return {
            "total_records": len(self.main_database["records"]),
            "duplicate_groups": total_duplicate_groups,
            "duplicate_records": total_duplicate_records,
            "unique_records": len(self.main_database["records"]) - total_duplicate_records + total_duplicate_groups,
            "duplicate_size_bytes": total_duplicate_size,
            "duplicate_size_kb": round(total_duplicate_size / 1024, 2),
            "duplicate_size_mb": round(total_duplicate_size / (1024 * 1024), 2),
            "duplicate_details": duplicate_details
        }


def main():
    st.set_page_config(
        page_title="🧹 Audio+JSON Deduper",
        page_icon="🧹",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Custom CSS for better styling
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .success-box {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown('<h1 class="main-header">🧹 Audio+JSON Deduper — Noyob namunalarni yig\'ish</h1>',
                unsafe_allow_html=True)
    st.markdown("**Upload JSON+audio files → Smart duplicate detection → Download unique dataset**")

    # Sidebar sozlamalari
    with st.sidebar:
        st.header("⚙️ Sozlamalar / Settings")

        # Deduplication mode
        dedup_mode = st.radio(
            "🔍 Takrorlarni aniqlash usuli",
            ["Exact (normalized)", "Fuzzy (similarity)"],
            help="Exact: aniq normallashtirish. Fuzzy: o'xshashlik darajasi bo'yicha."
        )

        unique_word_sig = st.checkbox(
            "📝 So'z tartibini e'tiborga olmaslik",
            value=False,
            help="Agar yoqilsa, bir xil so'zlar to'plamiga ega matnlar takroriy hisoblanadi."
        )

        similarity_threshold = 0.85
        if dedup_mode == "Fuzzy (similarity)":
            similarity_threshold = st.slider(
                "🎯 O'xshashlik chegarasi",
                min_value=0.70, max_value=0.99, value=0.85, step=0.01,
                help="Yuqori qiymat = qattiqroq tekshiruv. 0.85 tavsiya etiladi."
            )

        db_file = st.text_input(
            "💾 Ma'lumotlar bazasi fayli",
            value="main_audio_database.json",
            help="JSON ma'lumotlar bazasi fayl nomi"
        )

        if st.button("🔄 Sozlamalarni Qo'llash", type="primary"):
            if 'manager' in st.session_state:
                del st.session_state.manager
            st.rerun()

        st.divider()
        st.markdown("**🎵 Audio moslashtirish**")
        st.markdown(
            "- `utt_id` dan audio fayl nomi izlanadi\n"
            "- JSON fayl nomidan audio qidiriladi\n"
            "- Papka yo'li avtomatik saqlanadi"
        )

    # Manager obyektini yaratish
    if 'manager' not in st.session_state:
        st.session_state.manager = SmartAudioDataManager(
            main_db_path=db_file,
            similarity_threshold=similarity_threshold,
            unique_word_signature=unique_word_sig
        )

    manager = st.session_state.manager

    # Tab'larni yaratish
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📁 Fayl Yuklash",
        "📊 Statistika",
        "🔍 Takrorlar",
        "✨ Noyob To'plam",
        "💾 Ma'lumotlar"
    ])

    with tab1:
        st.header("📁 JSON Fayllarni Yuklash va Qayta Ishlash")

        # Sub-tabs
        subtab1, subtab2 = st.tabs(["📤 Upload Fayllar", "📂 Papka Qayta Ishlash"])

        with subtab1:
            st.subheader("📤 Fayllarni Upload Qilish")

            uploaded_files = st.file_uploader(
                "JSON fayllarni tanlang",
                type=['json'],
                accept_multiple_files=True,
                help="Bir nechta JSON fayl tanlashingiz mumkin"
            )

            if uploaded_files:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("📄 Tanlangan fayllar", len(uploaded_files))
                with col2:
                    total_size = sum(len(f.getvalue()) for f in uploaded_files) / (1024 * 1024)
                    st.metric("📊 Umumiy hajm (MB)", f"{total_size:.2f}")
                with col3:
                    st.metric("🎯 O'xshashlik chegarasi", f"{similarity_threshold:.2f}")

                action_mode = st.selectbox(
                    "🎮 Takroriy fayllar uchun harakat",
                    ["skip", "update_existing"],
                    format_func=lambda x: {
                        "skip": "⏭️ O'tkazib yuborish",
                        "update_existing": "🔄 Mavjudini yangilash"
                    }[x]
                )

                if st.button("🚀 Fayllarni Qayta Ishlash", type="primary"):
                    with st.spinner("⏳ Fayllar tahlil qilinmoqda..."):
                        progress_bar = st.progress(0)
                        status_container = st.empty()

                        results = {"added": 0, "skipped": 0, "updated": 0, "errors": 0, "details": []}

                        for i, file in enumerate(uploaded_files):
                            try:
                                file.seek(0)
                                file_content = json.loads(file.read())

                                status_container.write(f"📝 Qayta ishlanmoqda: {file.name}")

                                result = manager.add_record_streamlit(
                                    file_content,
                                    file.name,
                                    action_mode,
                                    folder_path="uploaded_files"
                                )

                                results["details"].append(result)
                                results[result["status"]] += 1

                                progress_bar.progress((i + 1) / len(uploaded_files))

                            except json.JSONDecodeError:
                                results["errors"] += 1
                                results["details"].append({
                                    "status": "error",
                                    "filename": file.name,
                                    "message": "❌ JSON format xatosi"
                                })
                            except Exception as e:
                                results["errors"] += 1
                                results["details"].append({
                                    "status": "error",
                                    "filename": file.name,
                                    "message": f"❌ Xatolik: {str(e)}"
                                })

                        status_container.empty()
                        progress_bar.empty()

                        # Natijalar
                        st.success("✅ Upload qayta ishlash tugallandi!")

                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("✅ Qo'shildi", results["added"])
                        with col2:
                            st.metric("🔄 Yangilandi", results["updated"])
                        with col3:
                            st.metric("⏭️ O'tkazildi", results["skipped"])
                        with col4:
                            st.metric("❌ Xatolar", results["errors"])

                        manager.save_main_database()

                        # Batafsil natijalar
                        if results["details"]:
                            with st.expander("📋 Batafsil natijalar"):
                                for detail in results["details"]:
                                    status_icon = {
                                        "added": "✅",
                                        "updated": "🔄",
                                        "skipped": "⏭️",
                                        "error": "❌"
                                    }.get(detail["status"], "❓")

                                    st.write(f"{status_icon} **{detail['filename']}**: {detail['message']}")

        with subtab2:
            st.subheader("📂 Papka Bo'yicha Qayta Ishlash")

            available_folders = manager.get_available_source_folders()

            if available_folders:
                st.success(f"📁 {len(available_folders)} ta mos papka topildi")

                selected_folder = st.selectbox(
                    "📁 Papkani tanlang:",
                    available_folders,
                    help="JSON va audio fayllar mavjud papkalar"
                )

                if selected_folder:
                    # Papka statistikasi
                    folder_path = os.path.join(os.getcwd(), selected_folder)
                    json_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.json')]
                    audio_files = [f for f in os.listdir(folder_path)
                                   if any(f.lower().endswith(ext) for ext in AUDIO_EXTS)]

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("📄 JSON fayllar", len(json_files))
                    with col2:
                        st.metric("🎵 Audio fayllar", len(audio_files))
                    with col3:
                        folder_size = sum(os.path.getsize(os.path.join(folder_path, f))
                                          for f in os.listdir(folder_path)
                                          if os.path.isfile(os.path.join(folder_path, f)))
                        st.metric("📊 Papka hajmi (MB)", f"{folder_size / (1024 * 1024):.1f}")

                    folder_action = st.selectbox(
                        "🎮 Papka uchun harakat:",
                        ["skip", "update_existing"],
                        format_func=lambda x: {
                            "skip": "⏭️ Takroriy matnlarni o'tkazib yuborish",
                            "update_existing": "🔄 Takroriy matnlarni yangilash"
                        }[x]
                    )

                    if st.button("🚀 Butun Papkani Qayta Ishlash", type="primary"):
                        with st.spinner(f"📁 {selected_folder} papkasi qayta ishlanmoqda..."):
                            results = manager.process_folder_files(folder_path, folder_action)

                        if results["status"] == "success":
                            st.success(f"✅ {selected_folder} papkasi muvaffaqiyatli qayta ishlandi!")

                            col1, col2, col3, col4, col5 = st.columns(5)
                            with col1:
                                st.metric("📁 Jami fayllar", results["total_files"])
                            with col2:
                                st.metric("✅ Qo'shildi", results["added"])
                            with col3:
                                st.metric("🔄 Yangilandi", results["updated"])
                            with col4:
                                st.metric("⏭️ O'tkazildi", results["skipped"])
                            with col5:
                                st.metric("❌ Xatolar", results["errors"])

                            manager.save_main_database()

                            if results["details"]:
                                with st.expander(f"📋 {len(results['details'])} ta fayl tafsiloti"):
                                    details_df = pd.DataFrame([{
                                        "📄 Fayl": d["filename"],
                                        "📊 Holat": {
                                            "added": "✅ Qo'shildi",
                                            "updated": "🔄 Yangilandi",
                                            "skipped": "⏭️ O'tkazildi",
                                            "error": "❌ Xatolik"
                                        }.get(d["status"], "❓"),
                                        "💬 Xabar": d.get("message", "")
                                    } for d in results["details"]])
                                    st.dataframe(details_df, use_container_width=True)
                        else:
                            st.error(results["message"])
            else:
                st.warning("⚠️ JSON yoki audio fayllar mavjud papkalar topilmadi")

    with tab2:
        st.header("📊 Ma'lumotlar Statistikasi")

        # Umumiy statistika
        total_records = len(manager.main_database["records"])
        unique_info = manager.get_unique_texts_info()
        duplicate_stats = manager.get_duplicate_statistics()

        st.subheader("📈 Umumiy Ko'rsatkichlar")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("📄 Jami yozuvlar", total_records)
        with col2:
            st.metric("✨ Noyob matnlar", unique_info["total_unique_texts"])
        with col3:
            st.metric("🔄 Takroriy guruhlar", duplicate_stats["duplicate_groups"])
        with col4:
            if total_records > 0:
                unique_percentage = round((unique_info["total_unique_texts"] / total_records) * 100, 1)
                st.metric("🎯 Noyoblik foizi", f"{unique_percentage}%")

        # Kategoriya va speaker statistikasi
        if unique_info["categories"]:
            st.subheader("📋 Kategoriya Bo'yicha Taqsimot")
            category_df = pd.DataFrame([
                {"Kategoriya": k, "Soni": v, "Foiz": f"{(v / unique_info['total_unique_texts'] * 100):.1f}%"}
                for k, v in unique_info["categories"].items()
            ]).sort_values("Soni", ascending=False)
            st.dataframe(category_df, use_container_width=True)

        if unique_info["speakers"]:
            st.subheader("👥 Speaker Bo'yicha Taqsimot")
            speaker_df = pd.DataFrame([
                {"Speaker ID": k, "Soni": v, "Foiz": f"{(v / unique_info['total_unique_texts'] * 100):.1f}%"}
                for k, v in unique_info["speakers"].items()
            ]).sort_values("Soni", ascending=False)
            st.dataframe(speaker_df, use_container_width=True)

    with tab3:
        st.header("🔍 Takroriy Matnlar Tahlili")

        duplicate_stats = manager.get_duplicate_statistics()

        if duplicate_stats["duplicate_groups"] > 0:
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("🔄 Takroriy guruhlar", duplicate_stats["duplicate_groups"])
            with col2:
                st.metric("📄 Takroriy yozuvlar", duplicate_stats["duplicate_records"])
            with col3:
                if duplicate_stats["duplicate_size_mb"] >= 1:
                    st.metric("💾 Takroriy hajm", f"{duplicate_stats['duplicate_size_mb']:.2f} MB")
                else:
                    st.metric("💾 Takroriy hajm", f"{duplicate_stats['duplicate_size_kb']:.2f} KB")
            with col4:
                if duplicate_stats["total_records"] > 0:
                    dup_percentage = round(
                        (duplicate_stats["duplicate_records"] / duplicate_stats["total_records"]) * 100, 1)
                    st.metric("📊 Takroriy foiz", f"{dup_percentage}%")

            # Takroriy guruhlar jadvali
            if duplicate_stats["duplicate_details"]:
                st.subheader("📋 Takroriy Guruhlar")

                dup_table_data = []
                for i, detail in enumerate(duplicate_stats["duplicate_details"][:20], 1):
                    size_str = f"{detail['size_mb']:.2f} MB" if detail[
                                                                    'size_mb'] >= 1 else f"{detail['size_kb']:.2f} KB"
                    text_preview = detail["text"][:80] + "..." if len(detail["text"]) > 80 else detail["text"]

                    dup_table_data.append({
                        "№": i,
                        "🔄 Takrorlar": detail["count"],
                        "📝 Matn": text_preview,
                        "💾 Hajm": size_str,
                        "👥 Speakerlar": detail["speaker_count"],
                        "🏷️ Kategoriyalar": ", ".join(detail["categories"][:2])
                    })

                dup_df = pd.DataFrame(dup_table_data)
                st.dataframe(dup_df, use_container_width=True)
        else:
            st.success("🎉 Takroriy matnlar topilmadi! Barcha ma'lumotlar noyob.")

    with tab4:
        st.header("✨ Noyob Ma'lumotlar To'plami")

        unique_info = manager.get_unique_texts_info()

        if unique_info["total_unique_texts"] > 0:
            # Statistika
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("✨ Noyob matnlar", unique_info["total_unique_texts"])
            with col2:
                if unique_info["total_size_mb"] >= 1:
                    st.metric("💾 Hajm", f"{unique_info['total_size_mb']:.2f} MB")
                else:
                    st.metric("💾 Hajm", f"{unique_info['total_size_kb']:.2f} KB")
            with col3:
                st.metric("🏷️ Kategoriyalar", len(unique_info["categories"]))
            with col4:
                st.metric("👥 Speakerlar", len(unique_info["speakers"]))

            # Dataset yaratish
            st.subheader("📦 Noyob Dataset Yaratish")

            dest_folder = st.text_input(
                "📁 Natija papkasi nomi:",
                value="unique_dataset_output",
                help="Noyob dataset saqlanadigan papka nomi"
            )

            if st.button("🚀 Noyob Dataset Yaratish", type="primary"):
                with st.spinner("📦 Dataset yaratilmoqda..."):
                    result = manager.create_unique_dataset_bundle(dest_folder)

                if result["status"] == "success":
                    st.success(f"✅ Noyob dataset muvaffaqiyatli yaratildi!")

                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("✨ Noyob yozuvlar", result["unique_count"])
                    with col2:
                        st.metric("📁 Yaratilgan papka", dest_folder)

                    st.info(f"📂 **Yaratilgan fayllar:**")
                    st.write(f"- 📄 **JSONL**: `{result['jsonl_path']}`")
                    st.write(f"- 📦 **ZIP Bundle**: `{result['zip_path']}`")
                    st.write(f"- 📊 **Manifest & Summary** ZIP ichida")

                    # Download tugmalari
                    col1, col2 = st.columns(2)

                    with col1:
                        try:
                            with open(result['jsonl_path'], 'rb') as f:
                                st.download_button(
                                    "⬇️ JSONL Yuklab Olish",
                                    data=f.read(),
                                    file_name="unique_dataset.jsonl",
                                    mime="application/jsonl"
                                )
                        except Exception as e:
                            st.error(f"JSONL yuklab olishda xatolik: {e}")

                    with col2:
                        try:
                            with open(result['zip_path'], 'rb') as f:
                                st.download_button(
                                    "⬇️ ZIP Bundle Yuklab Olish",
                                    data=f.read(),
                                    file_name="unique_dataset_bundle.zip",
                                    mime="application/zip"
                                )
                        except Exception as e:
                            st.error(f"ZIP yuklab olishda xatolik: {e}")

                else:
                    st.error(result["message"])

            # Noyob yozuvlarni preview
            with st.expander("👁️ Noyob yozuvlar preview (dastlabki 20 ta)"):
                if unique_info["unique_records"]:
                    preview_data = []
                    for record in unique_info["unique_records"][:20]:
                        preview_data.append({
                            "🆔 ID": record["record_id"],
                            "📝 Matn": record["text_preview"],
                            "👤 Speaker": record["speaker_id"] or "N/A",
                            "🏷️ Kategoriya": record["category"] or "N/A",
                            "📁 Papka": record["source_folder"] or "N/A",
                            "💾 Hajm": f"{record['size_bytes'] / 1024:.1f} KB"
                        })

                    preview_df = pd.DataFrame(preview_data)
                    st.dataframe(preview_df, use_container_width=True)

        else:
            st.info("📄 Hozircha noyob matnlar mavjud emas. JSON fayllarni yuklang!")

    with tab5:
        st.header("💾 Ma'lumotlar Boshqaruvi")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("💾 Saqlash va Yuklab Olish")

            if st.button("💾 Ma'lumotlarni Saqlash", type="primary"):
                try:
                    manager.save_main_database()
                    st.success("✅ Ma'lumotlar muvaffaqiyatli saqlandi!")
                except Exception as e:
                    st.error(f"❌ Saqlashda xatolik: {str(e)}")

            if st.button("⬇️ Bazani Yuklab Olish"):
                try:
                    with open(manager.main_db_path, 'r', encoding='utf-8') as f:
                        file_content = f.read()
                        st.download_button(
                            label="📥 JSON Database Yuklab Olish",
                            data=file_content,
                            file_name=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
                except Exception as e:
                    st.error(f"❌ Faylni o'qishda xatolik: {str(e)}")

        with col2:
            st.subheader("🔧 Tizim Ma'lumotlari")

            db_stats = {
                "📊 Ma'lumotlar bazasi hajmi": f"{os.path.getsize(manager.main_db_path) / 1024:.2f} KB" if os.path.exists(
                    manager.main_db_path) else "Mavjud emas",
                "🆔 Ma'lumotlar bazasi versiyasi": manager.main_database.get("metadata", {}).get("version", "N/A"),
                "📅 So'nggi yangilanish": manager.main_database.get("metadata", {}).get("last_updated", "N/A")[:19],
                "🎯 O'xshashlik chegarasi": f"{similarity_threshold:.2f}",
                "📝 So'z tartib rejimi": "Faol" if unique_word_sig else "O'chirilgan"
            }

            for key, value in db_stats.items():
                st.write(f"**{key}:** {value}")


if __name__ == "__main__":
    main()