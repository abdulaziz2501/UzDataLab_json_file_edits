# streamlit_audio_manager.py
import streamlit as st
import json
import os
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Tuple
from difflib import SequenceMatcher
import re
import pandas as pd
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
                        # Eski formatni yangi formatga o'tkazish
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

    def add_record_with_duplicate_check(self, file_path: str,
                                        action_on_duplicate: str = "ask") -> Dict[str, Any]:
        """
        Takroriy tekshiruv bilan record qo'shish
        action_on_duplicate: 'ask', 'skip', 'add_anyway', 'update_existing'
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                new_record = json.load(file)

            filename = os.path.basename(file_path)
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
                "filename": os.path.basename(file_path)
            }

    def batch_process_folder(self, folder_path: str,
                             action_on_duplicate: str = "ask") -> Dict[str, Any]:
        """Papkadagi fayllarni qayta ishlash"""
        results = {
            "added": 0,
            "skipped": 0,
            "updated": 0,
            "errors": 0,
            "details": [],
            "speaker_stats": {}
        }

        if not os.path.exists(folder_path):
            return {"error": "Papka topilmadi"}

        json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]

        if not json_files:
            return {"error": "JSON fayllar topilmadi"}

        for filename in json_files:
            file_path = os.path.join(folder_path, filename)
            result = self.add_record_with_duplicate_check(file_path, action_on_duplicate)
            results["details"].append(result)

            if result["status"] == "added":
                results["added"] += 1
                # Speaker statistikasini yangilash
                try:
                    with open(file_path, 'r', encoding='utf-8') as file:
                        record = json.load(file)
                        speaker_id = record.get("speaker_id", "unknown")
                        if speaker_id not in results["speaker_stats"]:
                            results["speaker_stats"][speaker_id] = 0
                        results["speaker_stats"][speaker_id] += 1
                except:
                    pass
            elif result["status"] == "skipped":
                results["skipped"] += 1
            elif result["status"] == "updated":
                results["updated"] += 1
            else:
                results["errors"] += 1

        return results

    def find_all_duplicates(self) -> Dict[str, List[str]]:
        """Barcha takroriy matnlarni topish - to'g'rilangan"""
        text_groups = {}

        for record_id, record in self.main_database["records"].items():
            text = record.get("text", "")
            text_hash = self.create_text_hash(text)  # Hash asosida guruhlaymiz

            if text_hash and text:
                if text_hash not in text_groups:
                    text_groups[text_hash] = {
                        "text": self.clean_text(text),
                        "original_text": text,
                        "records": []
                    }
                text_groups[text_hash]["records"].append(record_id)

        # Faqat 2 va undan ko'p yozuvga ega guruhlarni qaytarish
        duplicates = {}
        for text_hash, group_info in text_groups.items():
            if len(group_info["records"]) > 1:
                duplicates[group_info["original_text"]] = group_info["records"]

        return duplicates

    def get_speaker_statistics(self) -> Dict[str, int]:
        """Speaker ID bo'yicha statistika"""
        speaker_stats = {}

        for record in self.main_database["records"].values():
            speaker_id = record.get("speaker_id", "unknown")
            if speaker_id not in speaker_stats:
                speaker_stats[speaker_id] = 0
            speaker_stats[speaker_id] += 1

        return speaker_stats

    def get_duplicate_statistics(self) -> Dict[str, Any]:
        """Takroriy matnlar statistikasi"""
        duplicates = self.find_all_duplicates()
        speaker_stats = self.get_speaker_statistics()

        total_duplicate_groups = len(duplicates)
        total_duplicate_records = sum(len(ids) for ids in duplicates.values())

        return {
            "total_records": len(self.main_database["records"]),
            "duplicate_groups": total_duplicate_groups,
            "duplicate_records": total_duplicate_records,
            "unique_records": len(self.main_database["records"]) - total_duplicate_records + total_duplicate_groups,
            "speaker_statistics": speaker_stats,
            "duplicate_details": duplicates
        }


def get_folder_paths():
    """Tizimdan papka yo'llarini olish"""
    if os.name == 'nt':  # Windows
        common_paths = [
            str(Path.home() / "Desktop"),
            str(Path.home() / "Documents"),
            str(Path.home() / "Downloads"),
            "C:\\",
            "D:\\"
        ]
    else:  # Linux/Mac
        common_paths = [
            str(Path.home() / "Desktop"),
            str(Path.home() / "Documents"),
            str(Path.home() / "Downloads"),
            str(Path.home()),
            "/"
        ]

    # Mavjud papkalarni filtrlash
    return [path for path in common_paths if os.path.exists(path)]


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

    # Manager obyektini yaratish yoki yangilash
    if ('manager' not in st.session_state or
            st.session_state.get('similarity_threshold') != similarity_threshold or
            st.session_state.get('db_file') != db_file):
        st.session_state.manager = SmartAudioDataManager(
            main_db_path=db_file,
            similarity_threshold=similarity_threshold
        )
        st.session_state.similarity_threshold = similarity_threshold
        st.session_state.db_file = db_file

    manager = st.session_state.manager

    # Tab'larni yaratish
    tab1, tab2, tab3, tab4 = st.tabs([
        "📁 Papka Tanlash",
        "📊 Statistika",
        "🔍 Takrorlar",
        "💾 Ma'lumotlar"
    ])

    with tab1:
        st.header("📁 Papkadan Ma'lumot Qo'shish")

        # Papka yo'lini kiritish
        col1, col2 = st.columns([3, 1])

        with col1:
            folder_path = st.text_input(
                "Papka yo'lini kiriting:",
                placeholder="Masalan: C:\\Users\\Username\\Documents\\audio_data",
                help="JSON fayllar joylashgan papka yo'lini kiriting"
            )

        with col2:
            st.write("Yoki tanlang:")
            folder_paths = get_folder_paths()
            selected_path = st.selectbox(
                "Umumiy papkalar",
                [""] + folder_paths,
                format_func=lambda x: "Tanlang..." if x == "" else os.path.basename(x) or x
            )

            if selected_path:
                folder_path = selected_path

        # Papka tanlangach, uning ichidagi fayllarni ko'rsatish
        if folder_path and os.path.exists(folder_path):
            json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]

            if json_files:
                st.success(f"Topildi: **{len(json_files)}** ta JSON fayl")

                # Fayllar ro'yxatini ko'rsatish
                with st.expander(f"Fayllar ro'yxati ({len(json_files)} ta)"):
                    for i, filename in enumerate(json_files[:20], 1):  # Faqat birinchi 20 tasini ko'rsatish
                        st.write(f"{i}. {filename}")
                    if len(json_files) > 20:
                        st.write(f"... va yana {len(json_files) - 20} ta fayl")

                st.markdown("---")

                # Takroriy tekshiruv sozlamalari
                st.subheader("Takroriy Ma'lumotlar uchun Harakat")

                duplicate_action = st.radio(
                    "Takroriy matnlar topilganda nima qilish kerak?",
                    ["ask", "add_anyway", "skip", "update_existing"],
                    format_func=lambda x: {
                        "ask": "🤔 Har birini alohida so'rash",
                        "add_anyway": "✅ Barchasini qo'shish (har xil audio deb hisoblash)",
                        "skip": "⏭️ Takrorlarni o'tkazib yuborish",
                        "update_existing": "🔄 Mavjud yozuvlarni yangilash"
                    }[x]
                )

                st.markdown("---")

                # Qayta ishlashni boshlash tugmasi
                if st.button("🚀 Fayllarni Qayta Ishlashni Boshlash", type="primary", use_container_width=True):

                    # Progress bar va natijalar uchun joy
                    progress_container = st.container()
                    results_container = st.container()

                    with progress_container:
                        st.info("Fayllar qayta ishlanmoqda...")
                        progress_bar = st.progress(0)
                        status_text = st.empty()

                    # Takroriy so'rash rejimi uchun
                    if duplicate_action == "ask":
                        st.session_state.ask_mode = True
                        st.session_state.current_file_index = 0
                        st.session_state.pending_files = json_files
                        st.session_state.batch_results = {
                            "added": 0, "skipped": 0, "updated": 0, "errors": 0,
                            "details": [], "speaker_stats": {}
                        }

                    # Avtomatik rejim
                    else:
                        results = {"added": 0, "skipped": 0, "updated": 0, "errors": 0, "details": [],
                                   "speaker_stats": {}}

                        for i, filename in enumerate(json_files):
                            file_path = os.path.join(folder_path, filename)

                            # Progress yangilash
                            progress = (i + 1) / len(json_files)
                            progress_bar.progress(progress)
                            status_text.text(f"Qayta ishlanmoqda: {filename} ({i + 1}/{len(json_files)})")

                            result = manager.add_record_with_duplicate_check(file_path, duplicate_action)
                            results["details"].append(result)

                            if result["status"] == "added":
                                results["added"] += 1
                                # Speaker statistikasini yangilash
                                try:
                                    with open(file_path, 'r', encoding='utf-8') as file:
                                        record = json.load(file)
                                        speaker_id = record.get("speaker_id", "unknown")
                                        if speaker_id not in results["speaker_stats"]:
                                            results["speaker_stats"][speaker_id] = 0
                                        results["speaker_stats"][speaker_id] += 1
                                except:
                                    pass
                            elif result["status"] == "skipped":
                                results["skipped"] += 1
                            elif result["status"] == "updated":
                                results["updated"] += 1
                            else:
                                results["errors"] += 1

                        # Ma'lumotlarni saqlash
                        manager.save_main_database()

                        # Natijalarni ko'rsatish
                        with results_container:
                            st.success("✅ Qayta ishlash tugallandi!")

                            # Umumiy statistika
                            col1, col2, col3, col4 = st.columns(4)
                            with col1:
                                st.metric("Qo'shildi", results["added"], delta=results["added"])
                            with col2:
                                st.metric("Yangilandi", results["updated"])
                            with col3:
                                st.metric("O'tkazildi", results["skipped"])
                            with col4:
                                st.metric("Xatolar", results["errors"])

                            # Speaker statistikasi
                            if results["speaker_stats"]:
                                st.subheader("🎤 Speaker ID bo'yicha Statistika")
                                speaker_df = pd.DataFrame([
                                    {"Speaker ID": speaker_id, "Qo'shilgan Ma'lumotlar": count}
                                    for speaker_id, count in results["speaker_stats"].items()
                                ])
                                st.dataframe(speaker_df, use_container_width=True)

                            # Jami ma'lumotlar
                            total_records = len(manager.main_database["records"])
                            st.info(f"🗄️ Jami ma'lumotlar bazasida: **{total_records}** ta yozuv")

    # Takroriy so'rash rejimi uchun alohida interfeys
    if st.session_state.get("ask_mode", False):
        st.markdown("---")
        st.subheader("🤔 Takroriy Matn Topildi!")

        current_index = st.session_state.get("current_file_index", 0)
        pending_files = st.session_state.get("pending_files", [])

        if current_index < len(pending_files):
            current_file = pending_files[current_index]
            file_path = os.path.join(folder_path, current_file)

            # Joriy faylni tekshirish
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    record = json.load(file)
                    new_text = record.get("text", "")
                    similar_records = manager.find_similar_records(new_text)

                    if similar_records:
                        best_match = similar_records[0]
                        similarity_percent = int(best_match[2] * 100)

                        st.warning(f"Fayl: **{current_file}** ({current_index + 1}/{len(pending_files)})")
                        st.write(f"**Yangi matn:** {new_text}")
                        st.write(f"**Mavjud matn:** {best_match[1].get('text', '')}")
                        st.write(f"**O'xshashlik:** {similarity_percent}%")
                        st.write(f"**Mavjud ID:** {best_match[0]}")

                        col1, col2, col3, col4 = st.columns(4)

                        with col1:
                            if st.button("✅ Qo'shish"):
                                result = manager.add_record_with_duplicate_check(file_path, "add_anyway")
                                st.session_state.batch_results["added"] += 1
                                st.session_state.current_file_index += 1
                                manager.save_main_database()
                                st.rerun()

                        with col2:
                            if st.button("⏭️ O'tkazish"):
                                st.session_state.batch_results["skipped"] += 1
                                st.session_state.current_file_index += 1
                                st.rerun()

                        with col3:
                            if st.button("🔄 Yangilash"):
                                result = manager.add_record_with_duplicate_check(file_path, "update_existing")
                                st.session_state.batch_results["updated"] += 1
                                st.session_state.current_file_index += 1
                                manager.save_main_database()
                                st.rerun()

                        with col4:
                            if st.button("🛑 To'xtatish"):
                                st.session_state.ask_mode = False
                                st.rerun()
                    else:
                        # Takroriy emas, avtomatik qo'shish
                        result = manager.add_record_with_duplicate_check(file_path, "add_anyway")
                        st.session_state.batch_results["added"] += 1
                        st.session_state.current_file_index += 1
                        manager.save_main_database()
                        st.rerun()

            except Exception as e:
                st.error(f"Xatolik: {str(e)}")
                st.session_state.batch_results["errors"] += 1
                st.session_state.current_file_index += 1
                st.rerun()
        else:
            # Tugallandi
            st.success("✅ Barcha fayllar qayta ishlandi!")
            results = st.session_state.batch_results

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Qo'shildi", results["added"])
            with col2:
                st.metric("Yangilandi", results["updated"])
            with col3:
                st.metric("O'tkazildi", results["skipped"])
            with col4:
                st.metric("Xatolar", results["errors"])

            st.session_state.ask_mode = False

    with tab2:
        st.header("📊 Ma'lumotlar Statistikasi")

        stats = manager.get_duplicate_statistics()

        # Asosiy metrikalar
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Jami Yozuvlar", stats["total_records"])
        with col2:
            st.metric("Takroriy Guruhlar", stats["duplicate_groups"])
        with col3:
            st.metric("Takroriy Yozuvlar", stats["duplicate_records"])
        with col4:
            st.metric("Noyob Yozuvlar", stats["unique_records"])

        st.markdown("---")

        # Speaker statistikasi
        st.subheader("🎤 Speaker ID bo'yicha Statistika")
        speaker_stats = stats["speaker_statistics"]

        if speaker_stats:
            # Jadval ko'rinishida
            speaker_df = pd.DataFrame([
                {"Speaker ID": speaker_id, "Yozuvlar Soni": count}
                for speaker_id, count in sorted(speaker_stats.items(), key=lambda x: x[1], reverse=True)
            ])

            col1, col2 = st.columns([2, 1])
            with col1:
                st.dataframe(speaker_df, use_container_width=True)
            with col2:
                # Eng ko'p ma'lumot qo'shgan speakerlar
                st.write("**Top 5 Speaker:**")
                for i, (speaker_id, count) in enumerate(
                        sorted(speaker_stats.items(), key=lambda x: x[1], reverse=True)[:5], 1):
                    st.write(f"{i}. {speaker_id}: {count} ta")
        else:
            st.info("Hozircha speaker ma'lumotlari yo'q!")

        st.markdown("---")

        # Ma'lumotlar bazasi metadata
        st.subheader("📄 Ma'lumotlar Bazasi Haqida")
        metadata = manager.main_database.get("metadata", {})

        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Versiya:** {metadata.get('version', 'N/A')}")
            st.write(f"**So'nggi Yangilanish:** {metadata.get('last_updated', 'N/A')}")
        with col2:
            st.write(f"**Takroriy Siyosat:** {metadata.get('duplicate_policy', 'N/A')}")
            st.write(f"**O'xshashlik Chegarasi:** {similarity_threshold}")

    with tab3:
        st.header("🔍 Takroriy Matnlar")

        duplicates = manager.find_all_duplicates()

        if duplicates:
            st.write(f"Topilgan takroriy guruhlar: **{len(duplicates)}**")

            for i, (text, ids) in enumerate(duplicates.items(), 1):
                with st.expander(f"Guruh {i}: '{text}' ({len(ids)} marta)"):
                    for record_id in ids:
                        record = manager.main_database["records"][record_id]
                        st.write(f"**ID:** {record_id}")
                        st.write(f"**Yaratilgan:** {record.get('created_at', 'N/A')}")
                        st.write(f"**Manba Fayl:** {record.get('source_file', 'N/A')}")
                        st.write("---")
        else:
            st.info("Takroriy matnlar topilmadi!")

    with tab4:
        st.header("💾 Ma'lumotlar Boshqaruvi")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Saqlash")
            if st.button("Ma'lumotlarni Saqlash", type="primary"):
                try:
                    manager.save_main_database()
                    st.success("Ma'lumotlar muvaffaqiyatli saqlandi!")
                except Exception as e:
                    st.error(f"Saqlashda xatolik: {str(e)}")

            # Ma'lumotlar bazasini yuklab olish
            if st.button("Bazani Yuklab Olish"):
                with open(manager.main_db_path, 'r', encoding='utf-8') as f:
                    st.download_button(
                        label="JSON Faylni Yuklab Olish",
                        data=f.read(),
                        file_name=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )

        with col2:
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
                            "Yaratilgan": record.get("created_at", "N/A"),
                            "Spiker ID": record.get("speaker_id", "N/A"),
                            "Takroriy": "Ha" if record.get("is_potential_duplicate", False) else "Yo'q"
                        })

                    df = pd.DataFrame(records_data)
                    st.dataframe(df, use_container_width=True)
                else:
                    st.info("Hozircha yozuvlar yo'q!")


if __name__ == "__main__":
    main()