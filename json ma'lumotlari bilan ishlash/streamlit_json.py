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

        return {
            "total_records": len(self.main_database["records"]),
            "duplicate_groups": total_duplicate_groups,
            "duplicate_records": total_duplicate_records,
            "unique_records": len(self.main_database["records"]) - total_duplicate_records + total_duplicate_groups,
            "duplicate_details": duplicates
        }


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

    # Manager obyektini yaratish
    if 'manager' not in st.session_state:
        st.session_state.manager = SmartAudioDataManager(
            main_db_path=db_file,
            similarity_threshold=similarity_threshold
        )

    manager = st.session_state.manager

    # Tab'larni yaratish
    tab1, tab2, tab3, tab4 = st.tabs([
        "📁 Fayl Qo'shish",
        "📊 Statistika",
        "🔍 Takrorlar",
        "💾 Ma'lumotlar"
    ])

    with tab1:
        st.header("Yangi Fayl Qo'shish")

        col1, col2 = st.columns([1, 1])

        with col1:
            st.subheader("Bitta Fayl")
            uploaded_file = st.file_uploader(
                "JSON fayl yuklang",
                type=['json'],
                key="single_file"
            )

            if uploaded_file:
                try:
                    file_content = json.loads(uploaded_file.read())

                    with st.expander("Fayl tarkibi"):
                        st.json(file_content)

                    duplicate_action = st.selectbox(
                        "Takroriy fayllar uchun harakat",
                        ["add_anyway", "skip", "update_existing"],
                        format_func=lambda x: {
                            "add_anyway": "Qo'shish",
                            "skip": "O'tkazish",
                            "update_existing": "Yangilash"
                        }[x]
                    )

                    if st.button("Faylni Qo'shish", type="primary"):
                        result = manager.add_record_streamlit(
                            file_content,
                            uploaded_file.name,
                            duplicate_action
                        )

                        if result["status"] == "added":
                            st.success(result["message"])
                            manager.save_main_database()
                        elif result["status"] == "skipped":
                            st.warning(result["message"])
                        elif result["status"] == "updated":
                            st.info(result["message"])
                            manager.save_main_database()
                        else:
                            st.error(result["message"])

                        # O'xshash yozuvlarni ko'rsatish
                        if "similar_records" in result and result["similar_records"]:
                            st.subheader("O'xshash yozuvlar topildi:")
                            for i, (record_id, record, similarity) in enumerate(result["similar_records"]):
                                with st.expander(f"O'xshashlik: {int(similarity * 100)}% - {record_id}"):
                                    st.write(f"**Matn:** {record.get('text', '')}")
                                    st.write(f"**Yaratilgan:** {record.get('created_at', 'N/A')}")
                                    st.write(f"**Spiker ID:** {record.get('speaker_id', 'N/A')}")

                except json.JSONDecodeError:
                    st.error("JSON fayl formati noto'g'ri!")

        with col2:
            st.subheader("Bir nechta Fayl")
            uploaded_files = st.file_uploader(
                "Bir nechta JSON fayl yuklang",
                type=['json'],
                accept_multiple_files=True,
                key="multiple_files"
            )

            if uploaded_files:
                st.write(f"Tanlangan: {len(uploaded_files)} ta fayl")

                batch_action = st.selectbox(
                    "Batch ish uchun harakat",
                    ["add_anyway", "skip", "update_existing"],
                    format_func=lambda x: {
                        "add_anyway": "Barchasini qo'shish",
                        "skip": "Takrorlarni o'tkazish",
                        "update_existing": "Takrorlarni yangilash"
                    }[x],
                    key="batch_action"
                )

                if st.button("Barcha Fayllarni Qayta Ishlash", type="primary"):
                    progress_bar = st.progress(0)
                    results = {"added": 0, "skipped": 0, "updated": 0, "errors": 0, "details": []}

                    for i, file in enumerate(uploaded_files):
                        try:
                            file_content = json.loads(file.read())
                            result = manager.add_record_streamlit(
                                file_content,
                                file.name,
                                batch_action
                            )

                            results["details"].append(result)
                            results[result["status"]] += 1

                            progress_bar.progress((i + 1) / len(uploaded_files))

                        except json.JSONDecodeError:
                            results["errors"] += 1
                            results["details"].append({
                                "status": "error",
                                "filename": file.name,
                                "message": "JSON format xatosi"
                            })

                    # Natijalarni ko'rsatish
                    col_a, col_b, col_c, col_d = st.columns(4)
                    with col_a:
                        st.metric("Qo'shildi", results["added"])
                    with col_b:
                        st.metric("Yangilandi", results["updated"])
                    with col_c:
                        st.metric("O'tkazildi", results["skipped"])
                    with col_d:
                        st.metric("Xatolar", results["errors"])

                    manager.save_main_database()
                    st.success("Batch qayta ishlash tugallandi!")

    with tab2:
        st.header("📊 Ma'lumotlar Statistikasi")

        stats = manager.get_duplicate_statistics()

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Jami Yozuvlar", stats["total_records"])
        with col2:
            st.metric("Takroriy Guruhlar", stats["duplicate_groups"])
        with col3:
            st.metric("Takroriy Yozuvlar", stats["duplicate_records"])
        with col4:
            st.metric("Noyob Yozuvlar", stats["unique_records"])

        # Ma'lumotlar bazasi metadata
        st.subheader("Ma'lumotlar Bazasi Haqida")
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