import streamlit as st
import json
import os
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Tuple
from difflib import SequenceMatcher
import re
import pandas as pd
import shutil
from pathlib import Path


class SmartAudioDataManager:
    def __init__(self, main_db_path: str = "main_audio_database.json",
                 similarity_threshold: float = 0.85):
        """
        main_db_path: asosiy ma'lumotlar bazasi
        similarity_threshold: matn o'xshashlik chegarasi (0.0-1.0)
        """
        self.main_db_path = main_db_path
        self.similarity_threshold = similarity_threshold
        self.main_database = self.load_main_database()

    def add_record_streamlit(self, new_record: dict, filename: str,
                             action_on_duplicate: str = "ask", folder_path: str = None) -> Dict[str, Any]:
        """Streamlit uchun record qo'shish (folder_path qo'shildi)"""
        try:
            new_text = new_record.get("text", "")

            if not new_text:
                return {"status": "error", "message": "Matn topilmadi"}

            similar_records = self.find_similar_records(new_text)

            result = {
                "status": "unknown",
                "filename": filename,
                "folder_path": folder_path,  # Papka yo'lini qo'shish
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
                    result["message"] = "Takroriy matn, o'tkazib yuborildi"
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
                    result["message"] = f"Mavjud record yangilandi: {existing_id}"
                    result["updated_id"] = existing_id
                    return result

            # Yangi record qo'shish
            unique_id = self.generate_unique_id(new_record, filename)

            if similar_records:
                new_record["is_potential_duplicate"] = True
                new_record["similar_to"] = [r[0] for r in similar_records[:3]]
                new_record["max_similarity"] = similar_records[0][2]
            else:
                new_record["is_potential_duplicate"] = False

            new_record["utt_id"] = unique_id
            new_record["source_file"] = filename
            new_record["source_folder"] = folder_path  # Papka yo'lini saqlash
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
            result["message"] = f"Yangi record qo'shildi: {unique_id}"
            result["new_id"] = unique_id

            return result

        except Exception as e:
            return {
                "status": "error",
                "message": f"Xatolik: {str(e)}",
                "filename": filename,
                "folder_path": folder_path
            }

    def process_folder_files(self, folder_path: str, action_on_duplicate: str = "skip") -> Dict[str, Any]:
        """
        Butun papkadagi JSON fayllarni qayta ishlash
        """
        try:
            if not os.path.exists(folder_path):
                return {"status": "error", "message": f"Papka topilmadi: {folder_path}"}

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

            # Papkadagi JSON fayllarni topish
            json_files = []
            for file in os.listdir(folder_path):
                if file.lower().endswith('.json'):
                    json_files.append(file)

            results["total_files"] = len(json_files)

            if not json_files:
                return {
                    "status": "warning",
                    "message": f"Papkada JSON fayllar topilmadi: {folder_path}"
                }

            # Har bir JSON faylni qayta ishlash
            for json_file in json_files:
                file_path = os.path.join(folder_path, json_file)

                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        file_content = json.load(f)

                    # Folder yo'lini relative qilish
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
                        "folder_path": relative_folder,
                        "message": "JSON format xatosi"
                    }
                    results["details"].append(error_result)
                    results["errors"] += 1

                except Exception as e:
                    error_result = {
                        "status": "error",
                        "filename": json_file,
                        "folder_path": relative_folder,
                        "message": f"Xatolik: {str(e)}"
                    }
                    results["details"].append(error_result)
                    results["errors"] += 1

            return results

        except Exception as e:
            return {"status": "error", "message": f"Papka qayta ishlashda xatolik: {str(e)}"}

    def get_records_by_folder(self, folder_path: str = None) -> Dict[str, List[Dict]]:
        """
        Papka bo'yicha recordlarni guruhlash
        """
        folder_groups = {}

        for record_id, record in self.main_database["records"].items():
            record_folder = record.get("source_folder", "unknown")

            if folder_path and record_folder != folder_path:
                continue

            if record_folder not in folder_groups:
                folder_groups[record_folder] = []

            folder_groups[record_folder].append({
                "record_id": record_id,
                "filename": record.get("source_file", ""),
                "text_preview": record.get("text", "")[:50] + "..." if len(record.get("text", "")) > 50 else record.get(
                    "text", ""),
                "speaker_id": record.get("speaker_id"),
                "category": record.get("category"),
                "added_at": record.get("added_at")
            })

        return folder_groups

    def find_and_collect_unique_audio_files_smart(self, destination_folder: str,
                                                  file_extension: str = ".wav") -> Dict[str, Any]:
        """
        Aqlli usul bilan noyob audio fayllarni yig'ish - har bir record uchun source_folder dan qidirish
        """
        try:
            # Destination papkani yaratish
            os.makedirs(destination_folder, exist_ok=True)

            results = {
                "status": "success",
                "total_unique_texts": 0,
                "audio_files_found": 0,
                "audio_files_copied": 0,
                "missing_audio_files": [],
                "copied_files": [],
                "errors": [],
                "folders_processed": set()
            }

            # Noyob matnlarni topish
            unique_texts = self.find_unique_texts()
            results["total_unique_texts"] = len(unique_texts)

            if not unique_texts:
                return {"status": "warning", "message": "Hech qanday noyob matn topilmadi"}

            # Har bir noyob matn uchun
            for text_hash, record_ids in unique_texts.items():
                main_record_id = record_ids[0]
                record = self.main_database["records"].get(main_record_id)

                if not record:
                    continue

                # Source folder va filename olish
                source_folder = record.get("source_folder", "")
                source_filename = record.get("source_file", "")

                if not source_folder or not source_filename:
                    # Eski usul bilan ID dan fayl nomi yaratish
                    source_filename = f"{main_record_id}.json"
                    # Available folderlardan qidirish
                    available_folders = self.get_available_source_folders()
                    found_in_folder = None

                    for folder in available_folders:
                        test_path = os.path.join(folder, source_filename.replace('.json', file_extension))
                        if os.path.exists(test_path):
                            found_in_folder = folder
                            break

                    if found_in_folder:
                        source_folder = found_in_folder
                    else:
                        source_folder = "unknown"

                results["folders_processed"].add(source_folder)

                # Audio fayl nomini yaratish
                audio_filename = source_filename.replace(".json", file_extension)

                # Source va destination yo'llar
                if source_folder == "unknown":
                    # Barcha available folderlardan qidirish
                    found = False
                    for folder in self.get_available_source_folders():
                        source_audio_path = os.path.join(folder, audio_filename)
                        if os.path.exists(source_audio_path):
                            destination_audio_path = os.path.join(destination_folder, audio_filename)

                            try:
                                shutil.copy2(source_audio_path, destination_audio_path)
                                results["audio_files_found"] += 1
                                results["audio_files_copied"] += 1
                                results["copied_files"].append({
                                    "audio_file": audio_filename,
                                    "record_id": main_record_id,
                                    "text_preview": record.get("text", "")[:50] + "..." if len(
                                        record.get("text", "")) > 50 else record.get("text", ""),
                                    "source_path": source_audio_path,
                                    "destination_path": destination_audio_path,
                                    "source_folder": folder
                                })
                                found = True
                                break
                            except Exception as e:
                                results["errors"].append({
                                    "audio_file": audio_filename,
                                    "error": f"Nusxalash xatosi: {str(e)}"
                                })

                    if not found:
                        results["missing_audio_files"].append({
                            "expected_audio_file": audio_filename,
                            "record_id": main_record_id,
                            "text_preview": record.get("text", "")[:50] + "..." if len(
                                record.get("text", "")) > 50 else record.get("text", ""),
                            "searched_folders": self.get_available_source_folders()
                        })

                else:
                    # Ma'lum source folder dan qidirish
                    source_audio_path = os.path.join(source_folder, audio_filename)
                    destination_audio_path = os.path.join(destination_folder, audio_filename)

                    if os.path.exists(source_audio_path):
                        results["audio_files_found"] += 1

                        try:
                            shutil.copy2(source_audio_path, destination_audio_path)
                            results["audio_files_copied"] += 1
                            results["copied_files"].append({
                                "audio_file": audio_filename,
                                "record_id": main_record_id,
                                "text_preview": record.get("text", "")[:50] + "..." if len(
                                    record.get("text", "")) > 50 else record.get("text", ""),
                                "source_path": source_audio_path,
                                "destination_path": destination_audio_path,
                                "source_folder": source_folder
                            })
                        except Exception as e:
                            results["errors"].append({
                                "audio_file": audio_filename,
                                "error": f"Nusxalash xatosi: {str(e)}"
                            })
                    else:
                        results["missing_audio_files"].append({
                            "expected_audio_file": audio_filename,
                            "record_id": main_record_id,
                            "text_preview": record.get("text", "")[:50] + "..." if len(
                                record.get("text", "")) > 50 else record.get("text", ""),
                            "expected_source_path": source_audio_path,
                            "source_folder": source_folder
                        })

            results["folders_processed"] = list(results["folders_processed"])
            return results

        except Exception as e:
            return {"status": "error", "message": f"Umumiy xatolik: {str(e)}"}


    def find_and_collect_unique_audio_files(self, source_folder: str, destination_folder: str,
                                            file_extension: str = ".wav") -> Dict[str, Any]:
        """
        Noyob matnli JSON fayllar uchun mos audio fayllarni topish va yangi papkaga nusxalash
        """
        try:
            if not os.path.exists(source_folder):
                return {"status": "error", "message": f"Manba papka topilmadi: {source_folder}"}

            # Destination papkani yaratish
            os.makedirs(destination_folder, exist_ok=True)

            results = {
                "status": "success",
                "total_unique_texts": 0,
                "audio_files_found": 0,
                "audio_files_copied": 0,
                "missing_audio_files": [],
                "copied_files": [],
                "errors": []
            }

            # Noyob matnlarni topish
            unique_texts = self.find_unique_texts()
            results["total_unique_texts"] = len(unique_texts)

            if not unique_texts:
                return {"status": "warning", "message": "Hech qanday noyob matn topilmadi"}

            # Har bir noyob matn uchun audio fayl qidirish
            for text_hash, record_ids in unique_texts.items():
                # Birinchi record_id ni olish (noyob matn uchun)
                main_record_id = record_ids[0]
                record = self.main_database["records"].get(main_record_id)

                if not record:
                    continue

                # JSON fayl nomidan audio fayl nomini hosil qilish
                source_file_name = record.get("source_file", "")
                if not source_file_name:
                    # utt_id dan audio fayl nomini yaratish
                    source_file_name = f"{main_record_id}.json"

                # Audio fayl nomini yaratish (.json ni .wav ga almashtirish)
                audio_file_name = source_file_name.replace(".json", file_extension)
                source_audio_path = os.path.join(source_folder, audio_file_name)
                destination_audio_path = os.path.join(destination_folder, audio_file_name)

                # Audio fayl mavjudligini tekshirish
                if os.path.exists(source_audio_path):
                    results["audio_files_found"] += 1

                    try:
                        # Audio faylni nusxalash
                        shutil.copy2(source_audio_path, destination_audio_path)
                        results["audio_files_copied"] += 1
                        results["copied_files"].append({
                            "audio_file": audio_file_name,
                            "record_id": main_record_id,
                            "text_preview": record.get("text", "")[:50] + "..." if len(
                                record.get("text", "")) > 50 else record.get("text", ""),
                            "source_path": source_audio_path,
                            "destination_path": destination_audio_path
                        })
                    except Exception as e:
                        results["errors"].append({
                            "audio_file": audio_file_name,
                            "error": f"Nusxalash xatosi: {str(e)}"
                        })
                else:
                    results["missing_audio_files"].append({
                        "expected_audio_file": audio_file_name,
                        "record_id": main_record_id,
                        "text_preview": record.get("text", "")[:50] + "..." if len(
                            record.get("text", "")) > 50 else record.get("text", "")
                    })

            return results

        except Exception as e:
            return {"status": "error", "message": f"Umumiy xatolik: {str(e)}"}

    def get_available_source_folders(self) -> List[str]:
        """
        Loyihadagi mavjud papkalarni topish
        """
        available_folders = []
        current_dir = os.getcwd()

        # Loyiha ildiz papkasidagi barcha papkalarni tekshirish
        try:
            for item in os.listdir(current_dir):
                item_path = os.path.join(current_dir, item)
                if os.path.isdir(item_path) and not item.startswith('.'):
                    # JSON va audio fayllar borligini tekshirish
                    has_json = False
                    has_audio = False

                    try:
                        for file in os.listdir(item_path):
                            if file.endswith('.json'):
                                has_json = True
                            if file.endswith(('.wav', '.mp3', '.m4a', '.flac')):
                                has_audio = True
                    except PermissionError:
                        continue

                    if has_json or has_audio:
                        available_folders.append(item)

        except Exception as e:
            st.error(f"Papkalarni o'qishda xatolik: {str(e)}")

        return sorted(available_folders)

    def auto_create_destination_folder(self, base_name: str = "noyob_audio_fayllar") -> str:
        """
        Noyob papka nomi yaratish (agar mavjud bo'lsa raqam qo'shish)
        """
        counter = 1
        original_name = base_name

        while os.path.exists(base_name):
            base_name = f"{original_name}_{counter}"
            counter += 1

        return base_name

    def scan_folder_contents(self, folder_path: str) -> Dict[str, Any]:
        """
        Papka tarkibini skanerlash va statistika berish
        """
        try:
            if not os.path.exists(folder_path):
                return {"status": "error", "message": "Papka topilmadi"}

            contents = {
                "json_files": [],
                "audio_files": [],
                "other_files": [],
                "total_files": 0,
                "json_count": 0,
                "audio_count": 0,
                "folder_size_bytes": 0
            }

            audio_extensions = ['.wav', '.mp3', '.m4a', '.flac', '.ogg']

            for file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file)

                if os.path.isfile(file_path):
                    file_size = os.path.getsize(file_path)
                    contents["folder_size_bytes"] += file_size
                    contents["total_files"] += 1

                    file_lower = file.lower()

                    if file_lower.endswith('.json'):
                        contents["json_files"].append({
                            "name": file,
                            "size_bytes": file_size,
                            "size_kb": round(file_size / 1024, 2)
                        })
                        contents["json_count"] += 1

                    elif any(file_lower.endswith(ext) for ext in audio_extensions):
                        contents["audio_files"].append({
                            "name": file,
                            "size_bytes": file_size,
                            "size_mb": round(file_size / (1024 * 1024), 2)
                        })
                        contents["audio_count"] += 1

                    else:
                        contents["other_files"].append(file)

            contents["folder_size_mb"] = round(contents["folder_size_bytes"] / (1024 * 1024), 2)
            contents["status"] = "success"

            return contents

        except Exception as e:
            return {"status": "error", "message": f"Xatolik: {str(e)}"}

    def find_unique_texts(self) -> Dict[str, List[str]]:
        """
        Faqat noyob (takrorlanmaydigan) matnlarni topish
        """
        text_groups = {}

        # Barcha matnlarni guruhlab chiqish
        for record_id, record in self.main_database["records"].items():
            text = record.get("text", "")
            if text:
                text_hash = self.create_text_hash(text)
                if text_hash not in text_groups:
                    text_groups[text_hash] = []
                text_groups[text_hash].append(record_id)

        # Faqat noyob (1 marta uchraydigan) matnlarni qaytarish
        unique_texts = {text_hash: ids for text_hash, ids in text_groups.items() if len(ids) == 1}
        return unique_texts

    def get_unique_texts_info(self) -> Dict[str, Any]:
        """
        Noyob matnlar haqida statistik ma'lumot
        """
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
            record_id = record_ids[0]  # Noyob matn uchun faqat 1 ta record
            record = self.main_database["records"].get(record_id)

            if record:
                # Hajmni hisoblash
                record_json = json.dumps(record, ensure_ascii=False)
                record_size = len(record_json.encode('utf-8'))
                info["total_size_bytes"] += record_size

                # Kategoriya
                category = record.get("category", "unknown")
                info["categories"][category] = info["categories"].get(category, 0) + 1

                # Speaker
                speaker = record.get("speaker_id", "unknown")
                info["speakers"][speaker] = info["speakers"].get(speaker, 0) + 1

                # Til
                lang = record.get("lang")
                if lang:
                    info["languages"].add(lang)

                # Record ma'lumotini qo'shish
                info["unique_records"].append({
                    "record_id": record_id,
                    "text_preview": record.get("text", "")[:100] + "..." if len(
                        record.get("text", "")) > 100 else record.get("text", ""),
                    "speaker_id": record.get("speaker_id"),
                    "category": record.get("category"),
                    "source_file": record.get("source_file"),
                    "size_bytes": record_size
                })

        info["languages"] = list(info["languages"])
        info["total_size_kb"] = round(info["total_size_bytes"] / 1024, 2)
        info["total_size_mb"] = round(info["total_size_bytes"] / (1024 * 1024), 2)

        return info

    def clean_text(self, text: str) -> str:
        """Matnni taqqoslash uchun tozalash"""
        if not text:
            return ""

        text = text.lower().strip()
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'[.,!?;:"""''„"«»]', '', text)

        return text

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
        return hashlib.md5(clean_text.encode('utf-8')).hexdigest()[:8]

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
                                "version": "2.0",
                                "duplicate_policy": "detect_and_mark"
                            },
                            "records": {item.get("utt_id", f"record_{i}"): item
                                        for i, item in enumerate(data)},
                            "text_hashes": {}
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
                "version": "2.0",
                "duplicate_policy": "detect_and_mark"
            },
            "records": {},
            "text_hashes": {}
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
            base_id = os.path.splitext(filename)[0]

        original_id = base_id
        counter = 1
        while base_id in self.main_database["records"]:
            base_id = f"{original_id}_{counter}"
            counter += 1

        return base_id

    def add_record_streamlit(self, new_record: dict, filename: str,
                             action_on_duplicate: str = "ask") -> Dict[str, Any]:
        """Streamlit uchun record qo'shish"""
        try:
            new_text = new_record.get("text", "")

            if not new_text:
                return {"status": "error", "message": "Matn topilmadi"}

            similar_records = self.find_similar_records(new_text)

            result = {
                "status": "unknown",
                "filename": filename,
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
                    result["message"] = "Takroriy matn, o'tkazib yuborildi"
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
                    result["message"] = f"Mavjud record yangilandi: {existing_id}"
                    result["updated_id"] = existing_id
                    return result

            # Yangi record qo'shish
            unique_id = self.generate_unique_id(new_record, filename)

            if similar_records:
                new_record["is_potential_duplicate"] = True
                new_record["similar_to"] = [r[0] for r in similar_records[:3]]
                new_record["max_similarity"] = similar_records[0][2]
            else:
                new_record["is_potential_duplicate"] = False

            new_record["utt_id"] = unique_id
            new_record["source_file"] = filename
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
            result["message"] = f"Yangi record qo'shildi: {unique_id}"
            result["new_id"] = unique_id

            return result

        except Exception as e:
            return {
                "status": "error",
                "message": f"Xatolik: {str(e)}",
                "filename": filename
            }

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

        # Har bir duplicate group uchun batafsil ma'lumot
        duplicate_details = []
        total_duplicate_size = 0

        for text, ids in duplicates.items():
            group_size = 0
            group_duration = 0
            speakers = set()
            categories = set()
            regions = set()
            first_created = None
            last_created = None

            for record_id in ids:
                record = self.main_database["records"][record_id]

                # Hajm hisoblash
                record_json = json.dumps(record, ensure_ascii=False)
                record_size = len(record_json.encode('utf-8'))
                group_size += record_size

                # Duration
                duration = record.get("duration_ms", 0)
                if duration:
                    group_duration += duration

                # Speakers
                speaker_id = record.get("speaker_id")
                if speaker_id:
                    speakers.add(str(speaker_id))

                # Categories
                category = record.get("category")
                if category:
                    categories.add(category)

                # Regions
                region = record.get("region")
                if region:
                    regions.add(region)

                # Time tracking
                created_at = record.get("created_at")
                if created_at:
                    if not first_created or created_at < first_created:
                        first_created = created_at
                    if not last_created or created_at > last_created:
                        last_created = created_at

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
                "categories": list(categories),
                "regions": list(regions),
                "first_created": first_created,
                "last_created": last_created,
                "days_span": self._calculate_days_span(first_created,
                                                       last_created) if first_created and last_created else 0
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

    def _calculate_days_span(self, first_date: str, last_date: str) -> int:
        """Ikki sana orasidagi kunlar sonini hisoblash"""
        try:
            from datetime import datetime
            first = datetime.fromisoformat(first_date.replace('Z', '+00:00'))
            last = datetime.fromisoformat(last_date.replace('Z', '+00:00'))
            return (last - first).days
        except:
            return 0
    def get_general_statistics(self) -> Dict[str, Any]:
        """Umumiy statistikalar"""
        records = self.main_database["records"]

        # Kategoriya bo'yicha
        categories = {}
        sentiments = {}
        speakers = {}
        devices = {}
        regions = {}
        languages = {}
        genders = {}

        total_duration = 0
        total_data_size = 0  # Bytes

        for record in records.values():
            # Data hajmini hisoblash (JSON record hajmi)
            record_json = json.dumps(record, ensure_ascii=False)
            record_size = len(record_json.encode('utf-8'))
            total_data_size += record_size

            # Kategoriya
            cat = record.get("category", "unknown")
            categories[cat] = categories.get(cat, 0) + 1

            # Sentiment
            sent = record.get("sentiment", "unknown")
            sentiments[sent] = sentiments.get(sent, 0) + 1

            # Speaker
            speaker = record.get("speaker_id", "unknown")
            speakers[speaker] = speakers.get(speaker, 0) + 1

            # Device
            device = record.get("device", "unknown")
            devices[device] = devices.get(device, 0) + 1

            # Region
            region = record.get("region", "unknown")
            regions[region] = regions.get(region, 0) + 1

            # Language
            lang = record.get("lang", "unknown")
            languages[lang] = languages.get(lang, 0) + 1

            # Gender
            gender = record.get("gender", "unknown")
            genders[gender] = genders.get(gender, 0) + 1

            # Duration
            duration = record.get("duration_ms", 0)
            if duration:
                total_duration += duration

        return {
            "total_records": len(records),
            "total_duration_ms": total_duration,
            "total_duration_minutes": round(total_duration / 60000, 2),
            "total_duration_hours": round(total_duration / 3600000, 2),
            "total_data_size_bytes": total_data_size,
            "total_data_size_kb": round(total_data_size / 1024, 2),
            "total_data_size_mb": round(total_data_size / (1024 * 1024), 2),
            "categories": categories,
            "sentiments": sentiments,
            "speakers": speakers,
            "devices": devices,
            "regions": regions,
            "languages": languages,
            "genders": genders,
            "last_updated": self.main_database["metadata"]["last_updated"]
        }

    def get_speaker_statistics(self) -> Dict[str, Dict[str, Any]]:
        """Har bir speaker bo'yicha statistika"""
        speaker_stats = {}

        for record in self.main_database["records"].values():
            speaker_id = record.get("speaker_id", "unknown")

            if speaker_id not in speaker_stats:
                speaker_stats[speaker_id] = {
                    "total_records": 0,
                    "total_duration_ms": 0,
                    "total_data_size_bytes": 0,  # Yangi maydon
                    "categories": {},
                    "sentiments": {},
                    "devices": {},
                    "regions": set(),
                    "languages": set(),
                    "genders": set(),
                    "first_record": None,
                    "last_record": None
                }

            stats = speaker_stats[speaker_id]
            stats["total_records"] += 1

            # Data hajmini hisoblash
            record_json = json.dumps(record, ensure_ascii=False)
            record_size = len(record_json.encode('utf-8'))
            stats["total_data_size_bytes"] += record_size

            # Duration
            duration = record.get("duration_ms", 0)
            if duration:
                stats["total_duration_ms"] += duration

            # Categories
            category = record.get("category", "unknown")
            stats["categories"][category] = stats["categories"].get(category, 0) + 1

            # Sentiments
            sentiment = record.get("sentiment", "unknown")
            stats["sentiments"][sentiment] = stats["sentiments"].get(sentiment, 0) + 1

            # Devices
            device = record.get("device", "unknown")
            stats["devices"][device] = stats["devices"].get(device, 0) + 1

            # Regions
            if record.get("region"):
                stats["regions"].add(record.get("region"))

            # Languages
            if record.get("lang"):
                stats["languages"].add(record.get("lang"))

            # Genders
            if record.get("gender"):
                stats["genders"].add(record.get("gender"))

            # Time tracking
            created_at = record.get("created_at")
            if created_at:
                if not stats["first_record"] or created_at < stats["first_record"]:
                    stats["first_record"] = created_at
                if not stats["last_record"] or created_at > stats["last_record"]:
                    stats["last_record"] = created_at

        # Convert sets to lists and add size calculations
        for speaker_id, stats in speaker_stats.items():
            stats["regions"] = list(stats["regions"])
            stats["languages"] = list(stats["languages"])
            stats["genders"] = list(stats["genders"])
            stats["duration_minutes"] = round(stats["total_duration_ms"] / 60000, 2)
            stats["duration_hours"] = round(stats["total_duration_ms"] / 3600000, 2)
            stats["data_size_kb"] = round(stats["total_data_size_bytes"] / 1024, 2)
            stats["data_size_mb"] = round(stats["total_data_size_bytes"] / (1024 * 1024), 2)

        return speaker_stats


def main():
    st.set_page_config(
        page_title="Audio Ma'lumotlar Boshqaruvchi",
        page_icon="🎵",
        layout="wide"
    )

    st.title("🎵 Audio Ma'lumotlar Boshqaruvchi")
    st.markdown("---")

    # Sidebar sozlamalari
    with st.sidebar:
        st.header("⚙️ Sozlamalar")
        similarity_threshold = st.slider(
            "O'xshashlik chegarasi",
            min_value=0.5,
            max_value=1.0,
            value=0.85,
            step=0.05,
            help="Matnlar o'xshashligini belgilash chegarasi"
        )

        db_file = st.text_input(
            "Ma'lumotlar bazasi fayli",
            value="main_audio_database.json",
            help="JSON ma'lumotlar bazasi fayl nomi"
        )

        # Reset session state when settings change
        if st.button("Sozlamalarni Qo'llash"):
            if 'manager' in st.session_state:
                del st.session_state.manager
            st.rerun()

    # Manager obyektini yaratish
    if 'manager' not in st.session_state or st.session_state.manager.similarity_threshold != similarity_threshold:
        st.session_state.manager = SmartAudioDataManager(
            main_db_path=db_file,
            similarity_threshold=similarity_threshold
        )

    manager = st.session_state.manager

    # File uploader uchun session state'lar
    if 'upload_key' not in st.session_state:
        st.session_state.upload_key = 0
    if 'files_processed' not in st.session_state:
        st.session_state.files_processed = False
    if 'processing_complete' not in st.session_state:
        st.session_state.processing_complete = False

    # Tab'larni yaratish
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📁 Fayl Qo'shish",
        "📊 Umumiy Statistika",
        "🔍 Takrorlar",
        "🎵 Noyob Audio Fayllar",
        "💾 Ma'lumotlar"
    ])

    with tab1:
        st.header("Yangi Fayl Qo'shish")

        # Tab ichida sub-tablar yaratish
        subtab1, subtab2 = st.tabs(["📄 Alohida Fayllar", "📁 Butun Papka"])

        with subtab1:
            st.subheader("Alohida JSON Fayllar")

            # Processing tugagandan keyin upload key'ni yangilash
            if st.session_state.processing_complete:
                st.session_state.upload_key += 1
                st.session_state.processing_complete = False

            uploaded_files = st.file_uploader(
                "Bir nechta JSON fayl yuklang",
                type=['json'],
                accept_multiple_files=True,
                key=f"multiple_files_{st.session_state.upload_key}"
            )

            if uploaded_files:
                st.write(f"Tanlangan: {len(uploaded_files)} ta fayl")

                batch_action = st.selectbox(
                    "Batch ish uchun harakat",
                    ["skip", "update_existing"],
                    format_func=lambda x: {
                        "skip": "Takroriy matnni o'tkazib yuborish",
                        "update_existing": "Takroriy matnni yangilash"
                    }[x],
                    key="batch_action"
                )

                if st.button("Barcha Fayllarni Qayta Ishlash", type="primary", key="process_batch"):
                    with st.spinner("Fayllar qayta ishlanmoqda..."):
                        progress_bar = st.progress(0)
                        status_container = st.empty()
                        results = {"added": 0, "skipped": 0, "updated": 0, "errors": 0, "details": []}

                        for i, file in enumerate(uploaded_files):
                            try:
                                file.seek(0)
                                file_content = json.loads(file.read())

                                status_container.write(
                                    f"Qayta ishlanmoqda: {file.name} ({i + 1}/{len(uploaded_files)})")

                                # Folder yo'lini "uploaded_files" qilib belgilash
                                result = manager.add_record_streamlit(
                                    file_content,
                                    file.name,
                                    batch_action,
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
                                    "message": "JSON format xatosi",
                                    "folder_path": "uploaded_files"
                                })
                            except Exception as e:
                                results["errors"] += 1
                                results["details"].append({
                                    "status": "error",
                                    "filename": file.name,
                                    "message": f"Xatolik: {str(e)}",
                                    "folder_path": "uploaded_files"
                                })

                        status_container.empty()
                        progress_bar.empty()

                        # Natijalarni ko'rsatish
                        st.subheader("Qayta Ishlash Natijalari")
                        col_a, col_b, col_c, col_d = st.columns(4)
                        with col_a:
                            st.metric("➕ Qo'shildi", results["added"])
                        with col_b:
                            st.metric("🔄 Yangilandi", results["updated"])
                        with col_c:
                            st.metric("⏭️ O'tkazildi", results["skipped"])
                        with col_d:
                            st.metric("❌ Xatolar", results["errors"])

                        # Ma'lumotlar bazasini saqlash
                        try:
                            manager.save_main_database()
                            st.success("✅ Batch qayta ishlash tugallandi va ma'lumotlar saqlandi!")
                        except Exception as e:
                            st.error(f"❌ Ma'lumotlarni saqlashda xatolik: {str(e)}")

                        st.session_state.processing_complete = True

                        # Tafsilotlarni ko'rsatish
                        if results["details"]:
                            with st.expander("Batafsil natijalar"):
                                for detail in results["details"]:
                                    status_icon = {
                                        "added": "➕",
                                        "updated": "🔄",
                                        "skipped": "⏭️",
                                        "error": "❌"
                                    }.get(detail["status"], "❓")

                                    st.write(f"{status_icon} **{detail['filename']}**: {detail['message']}")
            else:
                st.info("📁 Bir nechta JSON fayl tanlang")

        with subtab2:
            st.subheader("Butun Papkani Qayta Ishlash")

            # Mavjud papkalarni ko'rsatish
            available_folders = manager.get_available_source_folders()

            if available_folders:
                st.info(f"📁 {len(available_folders)} ta papka topildi")

                selected_folder = st.selectbox(
                    "Qayta ishlash uchun papkani tanlang:",
                    available_folders,
                    key="folder_selector"
                )

                # Tanlangan papka tarkibi
                if selected_folder:
                    folder_contents = manager.scan_folder_contents(selected_folder)

                    if folder_contents["status"] == "success":
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("📄 JSON Fayllar", folder_contents['json_count'])
                        with col2:
                            st.metric("🎵 Audio Fayllar", folder_contents['audio_count'])
                        with col3:
                            st.metric("📊 Papka Hajmi", f"{folder_contents['folder_size_mb']:.1f} MB")

                        # JSON fayllar ro'yxati
                        if folder_contents['json_files']:
                            with st.expander(f"📋 {len(folder_contents['json_files'])} ta JSON fayl"):
                                json_df_data = []
                                for json_file in folder_contents['json_files']:
                                    json_df_data.append({
                                        "Fayl nomi": json_file['name'],
                                        "Hajm (KB)": json_file['size_kb']
                                    })

                                json_df = pd.DataFrame(json_df_data)
                                st.dataframe(json_df, use_container_width=True)

                folder_action = st.selectbox(
                    "Papka uchun harakat:",
                    ["skip", "update_existing"],
                    format_func=lambda x: {
                        "skip": "Takroriy matnlarni o'tkazib yuborish",
                        "update_existing": "Takroriy matnlarni yangilash"
                    }[x],
                    key="folder_action"
                )

                if st.button("🚀 Butun Papkani Qayta Ishlash", type="primary", key="process_folder"):
                    if selected_folder:
                        with st.spinner(f"📁 {selected_folder} papkasidagi barcha JSON fayllar qayta ishlanmoqda..."):
                            results = manager.process_folder_files(
                                folder_path=selected_folder,
                                action_on_duplicate=folder_action
                            )

                        if results["status"] == "success":
                            st.success(f"✅ {selected_folder} papkasi muvaffaqiyatli qayta ishlandi!")

                            # Natijalar
                            col1, col2, col3, col4, col5 = st.columns(5)
                            with col1:
                                st.metric("📁 Jami Fayllar", results["total_files"])
                            with col2:
                                st.metric("➕ Qo'shildi", results["added"])
                            with col3:
                                st.metric("🔄 Yangilandi", results["updated"])
                            with col4:
                                st.metric("⏭️ O'tkazildi", results["skipped"])
                            with col5:
                                st.metric("❌ Xatolar", results["errors"])

                            # Ma'lumotlar bazasini saqlash
                            try:
                                manager.save_main_database()
                                st.success("💾 Ma'lumotlar bazasi saqlandi!")
                            except Exception as e:
                                st.error(f"❌ Saqlashda xatolik: {str(e)}")

                            # Batafsil natijalar
                            if results["details"]:
                                with st.expander(f"📋 Batafsil natijalar ({len(results['details'])} ta fayl)"):
                                    details_data = []
                                    for detail in results["details"]:
                                        status_text = {
                                            "added": "Qo'shildi",
                                            "updated": "Yangilandi",
                                            "skipped": "O'tkazildi",
                                            "error": "Xatolik"
                                        }.get(detail["status"], "Noma'lum")

                                        details_data.append({
                                            "Fayl": detail["filename"],
                                            "Holat": status_text,
                                            "Xabar": detail.get("message", ""),
                                        })

                                    details_df = pd.DataFrame(details_data)
                                    st.dataframe(details_df, use_container_width=True)

                        elif results["status"] == "warning":
                            st.warning(results["message"])
                        else:
                            st.error(results["message"])
                    else:
                        st.error("Iltimos, papkani tanlang!")

            else:
                st.warning("⚠️ Loyihada mos papkalar topilmadi!")
                st.info("💡 JSON yoki audio fayllar mavjud papkalar qidiriladi.")

    with tab2:
        st.header("📊 Umumiy Statistika")

        speaker_stats = manager.get_speaker_statistics()

        if speaker_stats:
            # Umumiy speaker ko'rsatkichlari
            st.subheader("Umumiy Ko'rsatkichlar")
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Jami Speakerlar", len(speaker_stats))
            with col2:
                total_speaker_records = sum(stats["total_records"] for stats in speaker_stats.values())
                st.metric("Jami Yozuvlar", total_speaker_records)
            with col3:
                total_speaker_data_size = sum(stats["total_data_size_bytes"] for stats in speaker_stats.values())
                if total_speaker_data_size >= 1024 * 1024:
                    st.metric("Jami Hajm", f"{total_speaker_data_size / (1024 * 1024):.2f} MB")
                else:
                    st.metric("Jami Hajm", f"{total_speaker_data_size / 1024:.2f} KB")
            with col4:
                avg_per_speaker = total_speaker_records / len(speaker_stats)
                st.metric("O'rtacha/Speaker", f"{avg_per_speaker:.1f}")

            # Speaker tanlash
            st.subheader("Speaker Tafsilotlari")
            speaker_ids = list(speaker_stats.keys())
            selected_speaker = st.selectbox("Speakerni tanlang:", speaker_ids)

            if selected_speaker and selected_speaker in speaker_stats:
                speaker_data = speaker_stats[selected_speaker]

                # Tanlangan speaker statistikasi
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Yozuvlar Soni", speaker_data["total_records"])
                with col2:
                    if speaker_data["data_size_mb"] >= 1:
                        st.metric("Ma'lumot Hajmi", f"{speaker_data['data_size_mb']:.2f} MB")
                    else:
                        st.metric("Ma'lumot Hajmi", f"{speaker_data['data_size_kb']:.2f} KB")
                with col3:
                    st.metric("Jami Vaqt (daqiqa)", f"{speaker_data['duration_minutes']:.1f}")
                with col4:
                    st.metric("So'nggi Yozuv", speaker_data["last_record"][:10] if speaker_data["last_record"] else "N/A")

                st.subheader("Speakerlar Bo'yicha Hajm Statistikasi")
                speaker_list = []
                for speaker_id, stats in speaker_stats.items():
                    # Hajm ko'rsatkichini formatlash
                    if stats["data_size_mb"] >= 1:
                        size_display = f"{stats['data_size_mb']:.2f} MB"
                    else:
                        size_display = f"{stats['data_size_kb']:.2f} KB"

                    speaker_list.append({
                        "Speaker ID": speaker_id,
                        "Yozuvlar Soni": stats["total_records"],
                        "Ma'lumot Hajmi": size_display,
                        "Vaqt (daq)": stats["duration_minutes"],
                        "So'nggi faollik": stats["last_record"][:10] if stats["last_record"] else "N/A"
                    })

                df_speakers = pd.DataFrame(speaker_list)
                # Hajm bo'yicha sortlash uchun bytes qiymatidan foydalanish
                df_speakers["_sort_bytes"] = [speaker_stats[row["Speaker ID"]]["total_data_size_bytes"] for _, row
                                              in df_speakers.iterrows()]
                df_speakers = df_speakers.sort_values("_sort_bytes", ascending=False)
                df_speakers = df_speakers.drop(columns=["_sort_bytes"])  # Sort ustunini o'chirish
                st.dataframe(df_speakers, use_container_width=True)

            else:
                st.info("Hozircha speaker ma'lumotlari yo'q!")
    with tab3:
        st.header("🔍 Takroriy Matnlar Tahlili")

        duplicate_stats = manager.get_duplicate_statistics()

        if duplicate_stats["duplicate_groups"] > 0:
            # Umumiy statistika
            st.subheader("📊 Takroriy Ma'lumotlar Statistikasi")
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Takroriy Guruhlar", duplicate_stats["duplicate_groups"])
            with col2:
                st.metric("Takroriy Yozuvlar", duplicate_stats["duplicate_records"])
            with col3:
                if duplicate_stats["duplicate_size_mb"] >= 1:
                    st.metric("Takroriy Hajm", f"{duplicate_stats['duplicate_size_mb']:.2f} MB")
                else:
                    st.metric("Takroriy Hajm", f"{duplicate_stats['duplicate_size_kb']:.2f} KB")
            with col4:
                duplicate_percentage = round(
                    (duplicate_stats["duplicate_records"] / duplicate_stats["total_records"]) * 100, 1)
                st.metric("Takroriy Foiz", f"{duplicate_percentage}%")

            # Jadval ko'rinishda takroriy guruhlar
            st.subheader("📋 Takroriy Guruhlar Jadvali")

            # DataFrame yaratish
            table_data = []
            for i, detail in enumerate(duplicate_stats["duplicate_details"], 1):
                # Hajm formatlash
                if detail["size_mb"] >= 1:
                    size_display = f"{detail['size_mb']:.2f} MB"
                else:
                    size_display = f"{detail['size_kb']:.2f} KB"

                # Matnni qisqartirish
                text_preview = detail["text"][:50] + "..." if len(detail["text"]) > 50 else detail["text"]

                table_data.append({
                    "№": i,
                    "Matn": text_preview,
                    "Takrorlar": detail["count"],
                    "Hajm": size_display,
                    "Vaqt (daq)": detail["duration_minutes"],
                    "Speakerlar": detail["speaker_count"],
                    "Kategoriya": ", ".join(detail["categories"][:2]) + (
                        "..." if len(detail["categories"]) > 2 else ""),
                    "Kun Oralig'i": detail["days_span"]
                })

            df_duplicates = pd.DataFrame(table_data)
            st.dataframe(df_duplicates, use_container_width=True)

            # Filter bo'yicha ko'rsatish
            st.subheader("🔍 Batafsil Ko'rish")

            # Takroriy guruhni tanlash
            group_options = [f"Guruh {i + 1}: {detail['text'][:30]}..."
                             for i, detail in enumerate(duplicate_stats["duplicate_details"])]

            if group_options:
                selected_group_index = st.selectbox(
                    "Batafsil ko'rish uchun guruhni tanlang:",
                    range(len(group_options)),
                    format_func=lambda x: group_options[x]
                )

                if selected_group_index is not None:
                    selected_detail = duplicate_stats["duplicate_details"][selected_group_index]

                    # Tanlangan guruh haqida batafsil ma'lumot
                    st.info(f"**To'liq matn:** {selected_detail['text']}")

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.write(f"**Takrorlar soni:** {selected_detail['count']}")
                        st.write(f"**Speakerlar:** {', '.join(selected_detail['speakers'])}")
                    with col2:
                        if selected_detail['size_mb'] >= 1:
                            st.write(f"**Hajm:** {selected_detail['size_mb']:.2f} MB")
                        else:
                            st.write(f"**Hajm:** {selected_detail['size_kb']:.2f} KB")
                        st.write(f"**Vaqt:** {selected_detail['duration_minutes']:.1f} daqiqa")
                    with col3:
                        st.write(f"**Kategoriyalar:** {', '.join(selected_detail['categories'])}")
                        st.write(f"**Hududlar:** {', '.join(selected_detail['regions'])}")

                    if selected_detail["first_created"] and selected_detail["last_created"]:
                        st.write(
                            f"**Vaqt oralig'i:** {selected_detail['first_created'][:10]} dan {selected_detail['last_created'][:10]} gacha ({selected_detail['days_span']} kun)")

                    # Har bir yozuv haqida ma'lumot
                    st.subheader("Guruh Yozuvlari")
                    records_data = []
                    for record_id in selected_detail["record_ids"]:
                        record = manager.main_database["records"][record_id]
                        records_data.append({
                            "ID": record_id,
                            "Speaker ID": record.get("speaker_id", "N/A"),
                            "Yaratilgan": record.get("created_at", "N/A")[:16] if record.get("created_at") else "N/A",
                            "Kategoriya": record.get("category", "N/A"),
                            "Hissiyot": record.get("sentiment", "N/A"),
                            "Qurilma": record.get("device", "N/A"),
                            "Hudud": record.get("region", "N/A")
                        })

                    df_records = pd.DataFrame(records_data)
                    st.dataframe(df_records, use_container_width=True)

            # Statistik tahlil
            st.subheader("📈 Takroriy Tahlil")

            col1, col2 = st.columns(2)

            with col1:
                # Eng ko'p takrorlangan matnlar
                st.write("**Eng Ko'p Takrorlangan:**")
                top_duplicates = sorted(duplicate_stats["duplicate_details"],
                                        key=lambda x: x["count"], reverse=True)[:5]

                for i, detail in enumerate(top_duplicates, 1):
                    st.write(f"{i}. **{detail['count']} marta:** {detail['text'][:40]}...")

            with col2:
                # Eng ko'p hajm egallagan
                st.write("**Eng Ko'p Hajm:**")
                top_size = sorted(duplicate_stats["duplicate_details"],
                                  key=lambda x: x["size_bytes"], reverse=True)[:5]

                for i, detail in enumerate(top_size, 1):
                    size_str = f"{detail['size_mb']:.2f} MB" if detail[
                                                                    'size_mb'] >= 1 else f"{detail['size_kb']:.2f} KB"
                    st.write(f"{i}. **{size_str}:** {detail['text'][:40]}...")

            # Takroriylarni speaker bo'yicha tahlil
            st.subheader("👥 Speaker Bo'yicha Takroriy Tahlil")
            speaker_duplicates = {}

            for detail in duplicate_stats["duplicate_details"]:
                for speaker in detail["speakers"]:
                    if speaker not in speaker_duplicates:
                        speaker_duplicates[speaker] = {
                            "groups": 0,
                            "total_duplicates": 0,
                            "total_size": 0
                        }

                    # Ushbu speakerning ushbu guruhdagi yozuvlar sonini hisoblash
                    speaker_count_in_group = sum(1 for rid in detail["record_ids"]
                                                 if str(
                        manager.main_database["records"][rid].get("speaker_id", "")) == speaker)

                    speaker_duplicates[speaker]["groups"] += 1
                    speaker_duplicates[speaker]["total_duplicates"] += speaker_count_in_group
                    speaker_duplicates[speaker]["total_size"] += detail["size_bytes"] * (
                                speaker_count_in_group / detail["count"])

            if speaker_duplicates:
                speaker_dup_data = []
                for speaker_id, stats in speaker_duplicates.items():
                    size_mb = stats["total_size"] / (1024 * 1024)
                    size_str = f"{size_mb:.2f} MB" if size_mb >= 1 else f"{stats['total_size'] / 1024:.2f} KB"

                    speaker_dup_data.append({
                        "Speaker ID": speaker_id,
                        "Takroriy Guruhlar": stats["groups"],
                        "Takroriy Yozuvlar": stats["total_duplicates"],
                        "Takroriy Hajm": size_str
                    })

                df_speaker_dup = pd.DataFrame(speaker_dup_data)
                df_speaker_dup = df_speaker_dup.sort_values("Takroriy Yozuvlar", ascending=False)
                st.dataframe(df_speaker_dup, use_container_width=True)

        else:
            st.success("🎉 Takroriy matnlar topilmadi! Barcha ma'lumotlar noyob.")

            # Hali ham ba'zi statistikalarni ko'rsatish
            st.subheader("📊 Ma'lumotlar Sifati")
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Jami Yozuvlar", duplicate_stats["total_records"])
            with col2:
                st.metric("Noyob Yozuvlar", duplicate_stats["unique_records"])
            with col3:
                st.metric("Ma'lumot Sifati", "100%")

    with tab4:
        st.header("🎵 Noyob Audio Fayllarni Yig'ish")

        # Noyob matnlar haqida ma'lumot
        unique_info = manager.get_unique_texts_info()

        st.subheader("📊 Noyob Matnlar Statistikasi")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Noyob Matnlar", unique_info["total_unique_texts"])
        with col2:
            if unique_info["total_size_mb"] >= 1:
                st.metric("Umumiy Hajm", f"{unique_info['total_size_mb']:.2f} MB")
            else:
                st.metric("Umumiy Hajm", f"{unique_info['total_size_kb']:.2f} KB")
        with col3:
            st.metric("Kategoriyalar", len(unique_info["categories"]))
        with col4:
            st.metric("Speakerlar", len(unique_info["speakers"]))

        if unique_info["total_unique_texts"] > 0:
            # Papka bo'yicha ma'lumotlar
            folder_groups = manager.get_records_by_folder()

            st.subheader("📁 Papka Bo'yicha Ma'lumotlar")
            folder_stats = []
            for folder, records in folder_groups.items():
                folder_stats.append({
                    "Papka": folder,
                    "Recordlar": len(records),
                    "Noyob": len([r for r in unique_info["unique_records"]
                                  if any(rec["record_id"] == r["record_id"] for rec in records)])
                })

            folder_df = pd.DataFrame(folder_stats)
            st.dataframe(folder_df, use_container_width=True)

            # Audio fayllarni yig'ish bo'limi
            st.subheader("🎵 Aqlli Audio Fayllar Yig'ish")
            st.info("💡 Har bir record uchun saqlangan papka yo'lidan audio fayl qidiriladi")

            # Avtomatik destination papka nomi
            auto_dest_name = manager.auto_create_destination_folder()

            destination_folder = st.text_input(
                "Maqsad papka nomi:",
                value=auto_dest_name,
                help="Noyob audio fayllar saqlanadigan yangi papka nomi"
            )

            file_extension = st.selectbox(
                "Audio fayl turi:",
                [".wav", ".mp3", ".m4a", ".flac"],
                index=0
            )

            # Jarayon tugmasi
            if st.button("🧠 Aqlli Usul Bilan Audio Fayllarni Yig'ish", type="primary"):
                if not destination_folder.strip():
                    st.error("Iltimos, maqsad papka nomini kiriting!")
                else:
                    full_destination_path = os.path.join(os.getcwd(), destination_folder.strip())

                    with st.spinner("🔍 Recordlar uchun saqlangan papka yo'llaridan audio fayllar qidirilmoqda..."):
                        results = manager.find_and_collect_unique_audio_files_smart(
                            destination_folder=full_destination_path,
                            file_extension=file_extension
                        )

                    if results["status"] == "success":
                        st.success(f"✅ Aqlli jarayon tugallandi! {destination_folder} papkasi yaratildi.")

                        # Natijalar
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Noyob Matnlar", results["total_unique_texts"])
                        with col2:
                            st.metric("Audio Topildi", results["audio_files_found"])
                        with col3:
                            st.metric("Nusxalandi", results["audio_files_copied"])
                        with col4:
                            missing_count = len(results["missing_audio_files"])
                            st.metric("Topilmadi", missing_count)

                        # Qayta ishlangan papkalar
                        st.info(f"📁 Qayta ishlangan papkalar: {', '.join(results['folders_processed'])}")

                        # Muvaffaqiyat darajasi
                        success_rate = (results["audio_files_copied"] / results["total_unique_texts"]) * 100 if results[
                                                                                                                    "total_unique_texts"] > 0 else 0
                        st.progress(success_rate / 100)
                        st.write(f"**Muvaffaqiyat darajasi:** {success_rate:.1f}%")

                        # Nusxalangan fayllar
                        if results["copied_files"]:
                            with st.expander(f"✅ Nusxalangan {len(results['copied_files'])} ta Audio Fayl"):
                                copied_data = []
                                for file_info in results["copied_files"]:
                                    copied_data.append({
                                        "Audio Fayl": file_info["audio_file"],
                                        "Record ID": file_info["record_id"],
                                        "Manba Papka": file_info["source_folder"],
                                        "Matn (qisqacha)": file_info["text_preview"]
                                    })

                                df_copied = pd.DataFrame(copied_data)
                                st.dataframe(df_copied, use_container_width=True)

                        # Topilmagan fayllar
                        if results["missing_audio_files"]:
                            with st.expander(f"❌ Topilmagan {len(results['missing_audio_files'])} ta Audio Fayl"):
                                missing_data = []
                                for missing_info in results["missing_audio_files"]:
                                    missing_data.append({
                                        "Kutilgan Audio Fayl": missing_info["expected_audio_file"],
                                        "Record ID": missing_info["record_id"],
                                        "Matn (qisqacha)": missing_info["text_preview"],
                                        "Qidirilgan Joylar": missing_info.get("searched_folders", ["N/A"])
                                    })

                                df_missing = pd.DataFrame(missing_data)
                                st.dataframe(df_missing, use_container_width=True)

                        # Xatolar
                        if results["errors"]:
                            with st.expander("⚠️ Xatolar"):
                                for error in results["errors"]:
                                    st.error(f"**{error['audio_file']}**: {error['error']}")

                        st.info(f"📂 Noyob audio fayllar: `{full_destination_path}`")

                    elif results["status"] == "warning":
                        st.warning(results["message"])
                    else:
                        st.error(results["message"])

            # ... qolgan kod bir xil ...

            else:
                st.warning("⚠️ Loyihada hech qanday mos papka topilmadi!")
                st.info("💡 JSON yoki audio fayllar mavjud bo'lgan papkalar qidirildi.")

                # Manual input imkoniyati
                with st.expander("📝 Qo'lda papka yo'lini kiritish"):
                    manual_source = st.text_input(
                        "Manba papka yo'li:",
                        placeholder="masalan: papka1 yoki /to'liq/yo'l/papka",
                        help="JSON va audio fayllar joylashgan papka yo'li"
                    )

                    manual_dest = st.text_input(
                        "Maqsad papka nomi:",
                        value="noyob_audio_fayllar_manual",
                        help="Yangi yaratilacak papka nomi"
                    )

                    if st.button("🚀 Qo'lda Ko'rsatilgan Papka Bilan Ishlash"):
                        if manual_source and manual_dest:
                            with st.spinner("Jarayon amalga oshirilmoqda..."):
                                results = manager.find_and_collect_unique_audio_files(
                                    source_folder=manual_source,
                                    destination_folder=manual_dest,
                                    file_extension=file_extension
                                )

                            if results["status"] == "success":
                                st.success("✅ Manual jarayon tugallandi!")
                                # Results display code here...
                            else:
                                st.error(results.get("message", "Noma'lum xatolik"))
                        else:
                            st.error("Iltimos, har ikkala papka yo'lini kiriting!")

            # Noyob matnlar jadvali
            st.subheader("📋 Noyob Matnlar Ro'yxati")

            if unique_info["unique_records"]:
                unique_data = []
                for record in unique_info["unique_records"]:
                    size_display = f"{record['size_bytes'] / 1024:.2f} KB" if record[
                                                                                  'size_bytes'] < 1024 * 1024 else f"{record['size_bytes'] / (1024 * 1024):.2f} MB"

                    unique_data.append({
                        "Record ID": record["record_id"],
                        "Matn (qisqacha)": record["text_preview"],
                        "Speaker": record["speaker_id"] or "N/A",
                        "Kategoriya": record["category"] or "N/A",
                        "Manba Fayl": record["source_file"] or "N/A",
                        "Hajm": size_display
                    })

                df_unique = pd.DataFrame(unique_data)
                st.dataframe(df_unique, use_container_width=True)

                # CSV sifatida yuklab olish imkoniyati
                csv_data = df_unique.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📄 Noyob matnlar ro'yxatini CSV sifatida yuklab olish",
                    data=csv_data,
                    file_name=f"noyob_matnlar_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

            # Kategoriya bo'yicha tahlil
            if unique_info["categories"]:
                st.subheader("📊 Kategoriya bo'yicha Tahlil")
                category_data = []
                for category, count in unique_info["categories"].items():
                    category_data.append({
                        "Kategoriya": category,
                        "Soni": count,
                        "Foiz": f"{(count / unique_info['total_unique_texts'] * 100):.1f}%"
                    })

                df_categories = pd.DataFrame(category_data)
                df_categories = df_categories.sort_values("Soni", ascending=False)
                st.dataframe(df_categories, use_container_width=True)

        else:
            st.info("🔍 Hozircha noyob matnlar topilmadi. JSON fayllarni yuklang va takroriy matnlarni olib tashlang.")

    with tab5:
        st.header("💾 Ma'lumotlar Boshqaruvi")

        # col1, col2 = st.columns(2)
        #
        # with col1:
        st.subheader("Saqlash")
        if st.button("Ma'lumotlarni Saqlash", type="primary"):
            try:
                manager.save_main_database()
                st.success("Ma'lumotlar muvaffaqiyatli saqlandi!")
            except Exception as e:
                st.error(f"Saqlashda xatolik: {str(e)}")

        # Ma'lumotlar bazasini yuklab olish
        if st.button("Bazani Yuklab Olish"):
            try:
                with open(manager.main_db_path, 'r', encoding='utf-8') as f:
                    file_content = f.read()
                    st.download_button(
                        label="JSON Faylni Yuklab Olish",
                        data=file_content,
                        file_name=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
            except Exception as e:
                st.error(f"Faylni o'qishda xatolik: {str(e)}")

        # with col2:
        st.subheader("Barcha Yozuvlarni Ko'rish")
        if st.button("Yozuvlarni Ko'rsatish"):
            if manager.main_database["records"]:
                # DataFrame yaratish
                records_data = []
                for record_id, record in manager.main_database["records"].items():
                    records_data.append({
                        "ID": record_id,
                        "Matn": record.get("text", "")[:100] + "..." if len(
                            record.get("text", "")) > 100 else record.get("text", ""),
                        "Speaker ID": record.get("speaker_id", "N/A"),
                        "Yaratilgan": record.get("created_at", "N/A")[:10] if record.get("created_at") else "N/A",
                        "Kategoriya": record.get("category", "N/A"),
                        "Takroriy": "Ha" if record.get("is_potential_duplicate", False) else "Yo'q"
                    })

                df = pd.DataFrame(records_data)
                st.dataframe(df, use_container_width=True)
            else:
                st.info("Hozircha yozuvlar yo'q!")


if __name__ == "__main__":
    main()
