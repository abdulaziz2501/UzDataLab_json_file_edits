import streamlit as st
import json
import os
import shutil
import pandas as pd
from pathlib import Path
import zipfile
import tempfile
from collections import defaultdict, Counter


def scan_folder_for_files(folder_path):
    """Papkadan JSON va audio fayllarni qidirish"""
    json_files = []
    audio_files = {}

    if not folder_path or not os.path.exists(folder_path):
        return json_files, audio_files

    folder = Path(folder_path)

    # JSON fayllarni qidirish
    for json_file in folder.glob("*.json"):
        json_files.append(str(json_file))

    # Audio fayllarni qidirish va indekslash
    audio_extensions = ['.wav', '.mp3', '.ogg', '.flac', '.m4a']
    for ext in audio_extensions:
        for audio_file in folder.glob(f"*{ext}"):
            # Fayl nomini (kengaytmasiz) kalit sifatida ishlatish
            file_stem = audio_file.stem
            audio_files[file_stem] = str(audio_file)

    return json_files, audio_files


def load_all_json_files(json_file_paths):
    """Barcha JSON fayllarni yuklash"""
    all_data = []
    failed_files = []

    for file_path in json_file_paths:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)
        except Exception as e:
            failed_files.append((file_path, str(e)))
            st.warning(f"Fayl o'qilmadi: {os.path.basename(file_path)} - {str(e)}")

    return all_data, failed_files


def find_unique_texts_detailed(json_data):
    """Noyob matnlarni topish va batafsil tahlil"""
    text_counts = Counter()
    text_to_items = defaultdict(list)

    # Matnlarni sanash va guruhlash
    for item in json_data:
        text = item.get('text', '').strip()
        if text:
            text_counts[text] += 1
            text_to_items[text].append(item)

    # Faqat bir marta uchraydigan matnlarni olish
    unique_items = []
    for text, count in text_counts.items():
        if count == 1:
            unique_items.extend(text_to_items[text])

    duplicate_texts = {text: count for text, count in text_counts.items() if count > 1}

    return unique_items, duplicate_texts, text_counts


def match_audio_files(unique_items, audio_files_dict):
    """Audio fayllarni JSON bilan moslashtirish"""
    matched_files = []
    unmatched_files = []

    for item in unique_items:
        # Turli usullar bilan audio faylni qidirish
        audio_found = False

        # 1. utt_id bo'yicha qidirish
        if 'utt_id' in item and item['utt_id'] in audio_files_dict:
            matched_files.append({
                'json_item': item,
                'audio_path': audio_files_dict[item['utt_id']],
                'match_method': 'utt_id'
            })
            audio_found = True

        # 2. Agar utt_id bo'yicha topilmasa, boshqa maydonlar bo'yicha qidirish
        if not audio_found:
            for key in ['id', 'file_id', 'filename']:
                if key in item and item[key] in audio_files_dict:
                    matched_files.append({
                        'json_item': item,
                        'audio_path': audio_files_dict[item[key]],
                        'match_method': key
                    })
                    audio_found = True
                    break

        if not audio_found:
            unmatched_files.append(item)

    return matched_files, unmatched_files


def create_output_package(unique_items, matched_files, output_folder):
    """Chiqish paketini yaratish"""
    os.makedirs(output_folder, exist_ok=True)

    # JSON faylni saqlash
    json_output_path = os.path.join(output_folder, "noyob_matnlar.json")
    with open(json_output_path, 'w', encoding='utf-8') as f:
        json.dump(unique_items, f, ensure_ascii=False, indent=2)

    # Audio fayllar papkasi
    audio_output_folder = os.path.join(output_folder, "audio_fayllar")
    os.makedirs(audio_output_folder, exist_ok=True)

    copied_count = 0
    copy_errors = []

    # Audio fayllarni nusxalash
    for match in matched_files:
        try:
            src_path = match['audio_path']
            filename = os.path.basename(src_path)
            dst_path = os.path.join(audio_output_folder, filename)

            shutil.copy2(src_path, dst_path)
            copied_count += 1

        except Exception as e:
            copy_errors.append(f"{filename}: {str(e)}")

    # Hisobot yaratish
    report = {
        'umumiy_json': len(unique_items),
        'moslashgan_audio': len(matched_files),
        'nusxalangan_audio': copied_count,
        'nusxalash_xatolari': copy_errors,
        'json_fayl': json_output_path,
        'audio_papka': audio_output_folder
    }

    return report


def create_statistics_dataframe(unique_items):
    """Statistika uchun DataFrame yaratish"""
    if not unique_items:
        return pd.DataFrame()

    stats_data = []
    for item in unique_items:
        stats_data.append({
            'ID': item.get('utt_id', 'N/A'),
            'Matn (qisqa)': item.get('text', '')[:50] + '...' if len(item.get('text', '')) > 50 else item.get('text',
                                                                                                              ''),
            'Davomiyligi (ms)': item.get('duration_ms', 'N/A'),
            'So\'zlovchi ID': item.get('speaker_id', 'N/A'),
            'Jins': item.get('gender', 'N/A'),
            'Hudud': item.get('region', 'N/A'),
            'Kategoriya': item.get('category', 'N/A'),
            'Kayfiyat': item.get('sentiment', 'N/A'),
            'Yaratilgan': item.get('created_at', 'N/A')[:10] if item.get('created_at') else 'N/A'
        })

    return pd.DataFrame(stats_data)


def main():
    st.set_page_config(
        page_title="Noyob Matn va Audio Yig'uvchi",
        page_icon="🎯",
        layout="wide"
    )

    st.title("🎯 Noyob Matn va Audio Yig'uvchi")
    st.markdown("Bu dastur faqat bir marta uchraydigan matnlarni topadi va tegishli audio fayllarni yig'adi")
    st.markdown("---")

    # Sidebar
    with st.sidebar:
        st.header("📂 Papka Tanlash")

        # Papka yo'lini kiritish
        folder_path = st.text_input(
            "Papka yo'lini kiriting:",
            placeholder=r"C:\Users\...\json_audio_files",
            help="JSON va audio fayllar joylashgan papka yo'lini kiriting"
        )

        # Papka mavjudligini tekshirish
        if folder_path:
            if os.path.exists(folder_path):
                st.success("✅ Papka topildi")

                # Fayllarni skanerlash
                json_files, audio_files = scan_folder_for_files(folder_path)

                st.info(f"📄 JSON fayllar: {len(json_files)}")
                st.info(f"🎵 Audio fayllar: {len(audio_files)}")

                # Jarayonni boshlash tugmasi
                if st.button("🚀 Jarayonni Boshlash", type="primary"):
                    st.session_state.start_processing = True
                    st.session_state.folder_path = folder_path
                    st.session_state.json_files = json_files
                    st.session_state.audio_files = audio_files
            else:
                st.error("❌ Papka topilmadi")

        st.markdown("---")
        st.markdown("**Misol yo'l:**")
        st.code("C:\\Dataset\\uzbek_audio")

    # Asosiy kontentlar
    if hasattr(st.session_state, 'start_processing') and st.session_state.start_processing:

        with st.spinner("Ma'lumotlar tahlil qilinmoqda..."):
            # JSON fayllarni yuklash
            all_json_data, failed_files = load_all_json_files(st.session_state.json_files)

            # Noyob matnlarni topish
            unique_items, duplicate_texts, text_counts = find_unique_texts_detailed(all_json_data)

            # Audio fayllar bilan moslashtirish
            matched_files, unmatched_files = match_audio_files(unique_items, st.session_state.audio_files)

        # Statistikalar
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("📊 Umumiy JSON yozuvlar", len(all_json_data))

        with col2:
            st.metric("🎯 Noyob matnlar", len(unique_items))

        with col3:
            st.metric("🔄 Takroriy matnlar", len(duplicate_texts))

        with col4:
            st.metric("🎵 Moslashgan audiolar", len(matched_files))

        st.markdown("---")

        # Tablar
        tab1, tab2, tab3, tab4 = st.tabs(
            ["📋 Noyob Ma'lumotlar", "📊 Statistika", "⚠️ Takroriy Matnlar", "⬇️ Yuklab Olish"])

        with tab1:
            if unique_items:
                df = create_statistics_dataframe(unique_items)
                st.dataframe(df, use_container_width=True, height=400)

                # CSV yuklab olish
                csv = df.to_csv(index=False, encoding='utf-8')
                st.download_button(
                    label="📁 CSV formatda yuklab olish",
                    data=csv.encode('utf-8'),
                    file_name="noyob_matnlar.csv",
                    mime="text/csv"
                )
            else:
                st.warning("Noyob matnlar topilmadi")

        with tab2:
            col_stat1, col_stat2 = st.columns(2)

            with col_stat1:
                st.subheader("📈 Kategoriya bo'yicha")
                if unique_items:
                    categories = [item.get('category', 'Noma\'lum') for item in unique_items]
                    category_counts = Counter(categories)
                    st.bar_chart(category_counts)

            with col_stat2:
                st.subheader("👥 Jins bo'yicha")
                if unique_items:
                    genders = [item.get('gender', 'Noma\'lum') for item in unique_items]
                    gender_counts = Counter(genders)
                    st.bar_chart(gender_counts)

        with tab3:
            if duplicate_texts:
                st.subheader(f"🔄 Takroriy matnlar ({len(duplicate_texts)} ta)")

                duplicate_df = pd.DataFrame([
                    {'Matn': text[:100] + '...' if len(text) > 100 else text,
                     'Takrorlanish soni': count}
                    for text, count in duplicate_texts.items()
                ])

                st.dataframe(duplicate_df, use_container_width=True)
            else:
                st.success("✅ Takroriy matnlar topilmadi!")

        with tab4:
            st.subheader("📦 Paket yaratish va yuklab olish")

            # Chiqish papkasi
            output_folder = st.text_input(
                "Chiqish papkasi:",
                value=os.path.join(st.session_state.folder_path, "noyob_natija"),
                help="Noyob fayllar saqlanadigan papka"
            )

            col_btn1, col_btn2 = st.columns(2)

            with col_btn1:
                if st.button("📁 Fayllarni papkaga saqlash", type="primary"):
                    if output_folder:
                        with st.spinner("Fayllar nusxalanmoqda..."):
                            report = create_output_package(unique_items, matched_files, output_folder)

                            st.success(f"✅ Jarayon tugadi!")

                            # Hisobot
                            st.json({
                                "Umumiy JSON": report['umumiy_json'],
                                "Moslashgan audio": report['moslashgan_audio'],
                                "Nusxalangan audio": report['nusxalangan_audio'],
                                "JSON fayl yo'li": report['json_fayl'],
                                "Audio papka yo'li": report['audio_papka']
                            })

                            if report['nusxalash_xatolari']:
                                st.warning("⚠️ Nusxalash xatolari:")
                                for error in report['nusxalash_xatolari']:
                                    st.text(f"• {error}")

            with col_btn2:
                if st.button("📦 ZIP faylni yaratish"):
                    with st.spinner("ZIP fayl yaratilmoqda..."):
                        # Vaqtinchalik papka
                        temp_dir = tempfile.mkdtemp()

                        # Fayllarni vaqtinchalik papkaga nusxalash
                        report = create_output_package(unique_items, matched_files, temp_dir)

                        # ZIP yaratish
                        zip_path = os.path.join(temp_dir, "noyob_dataset.zip")
                        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            # Barcha fayllarni ZIP ga qo'shish
                            for root, dirs, files in os.walk(temp_dir):
                                for file in files:
                                    if file != "noyob_dataset.zip":
                                        file_path = os.path.join(root, file)
                                        arcname = os.path.relpath(file_path, temp_dir)
                                        zipf.write(file_path, arcname)

                        # ZIP ni yuklab olish
                        with open(zip_path, 'rb') as f:
                            st.download_button(
                                label="⬇️ ZIP faylni yuklab olish",
                                data=f.read(),
                                file_name="noyob_dataset.zip",
                                mime="application/zip"
                            )

                        st.success(f"ZIP yaratildi: {report['nusxalangan_audio']} audio fayl")

    else:
        # Boshlash ko'rsatmalari
        st.info("👈 Chapdan papka yo'lini kiriting va jarayonni boshlang")

        st.markdown("""
        ### 📖 Qo'llanma:

        1. **Papka tanlash**: JSON va audio fayllar joylashgan papka yo'lini kiriting
        2. **Avtomatik skanerlash**: Dastur papkadagi barcha JSON va audio fayllarni topadi
        3. **Noyob matnlarni aniqlash**: Faqat bir marta uchraydigan matnlarni ajratadi
        4. **Audio moslashtirish**: Har bir noyob JSON uchun tegishli audio faylni qidiradi
        5. **Natijani saqlash**: Papka yoki ZIP formatida yuklab oling

        ### ⚙️ Talablar:
        - JSON fayllar `text` maydoniga ega bo'lishi kerak
        - Audio fayl nomlari JSON dagi `utt_id` ga mos kelishi kerak
        - Barcha fayllar bitta papkada bo'lishi kerak
        """)


if __name__ == "__main__":
    main()