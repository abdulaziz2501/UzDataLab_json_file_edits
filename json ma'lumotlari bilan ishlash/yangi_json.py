# import streamlit as st
# import json
# import os
# import shutil
# import pandas as pd
# from pathlib import Path
# import zipfile
# import tempfile
# from collections import defaultdict, Counter
#
#
# def get_available_drives_and_folders():
#     """Mavjud disklarni va umumiy papkalarni olish"""
#     drives_and_folders = []
#
#     # Windows disklari
#     if os.name == 'nt':  # Windows
#         for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
#             drive = f"{letter}:\\"
#             if os.path.exists(drive):
#                 drives_and_folders.append(drive)
#
#     # Umumiy papkalar
#     common_folders = [
#         os.path.expanduser("~"),  # Home directory
#         os.path.expanduser("~/Desktop"),
#         os.path.expanduser("~/Documents"),
#         os.path.expanduser("~/Downloads"),
#     ]
#
#     for folder in common_folders:
#         if os.path.exists(folder):
#             drives_and_folders.append(folder)
#
#     return drives_and_folders
#
#
# def browse_folder(base_path):
#     """Papka ko'rish funksiyasi"""
#     if not os.path.exists(base_path):
#         return []
#
#     folders = []
#     try:
#         for item in os.listdir(base_path):
#             item_path = os.path.join(base_path, item)
#             if os.path.isdir(item_path):
#                 folders.append(item_path)
#     except PermissionError:
#         pass
#
#     return sorted(folders)
#
#
# def scan_folder_for_files(folder_path):
#     """Papkadan JSON va audio fayllarni qidirish"""
#     json_files = []
#     audio_files = {}
#
#     if not folder_path or not os.path.exists(folder_path):
#         return json_files, audio_files
#
#     folder = Path(folder_path)
#
#     # JSON fayllarni qidirish
#     for json_file in folder.glob("*.json"):
#         json_files.append(str(json_file))
#
#     # Audio fayllarni qidirish va indekslash
#     audio_extensions = ['.wav', '.mp3', '.ogg', '.flac', '.m4a']
#     for ext in audio_extensions:
#         for audio_file in folder.glob(f"*{ext}"):
#             file_stem = audio_file.stem
#             audio_files[file_stem] = str(audio_file)
#
#     return json_files, audio_files
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

    for item in json_data:
        text = item.get('text', '').strip()
        if text:
            text_counts[text] += 1
            text_to_items[text].append(item)

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
        audio_found = False

        if 'utt_id' in item and item['utt_id'] in audio_files_dict:
            matched_files.append({
                'json_item': item,
                'audio_path': audio_files_dict[item['utt_id']],
                'match_method': 'utt_id'
            })
            audio_found = True

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

    json_output_path = os.path.join(output_folder, "noyob_matnlar.json")
    with open(json_output_path, 'w', encoding='utf-8') as f:
        json.dump(unique_items, f, ensure_ascii=False, indent=2)

    audio_output_folder = os.path.join(output_folder, "audio_fayllar")
    os.makedirs(audio_output_folder, exist_ok=True)

    copied_count = 0
    copy_errors = []

    for match in matched_files:
        try:
            src_path = match['audio_path']
            filename = os.path.basename(src_path)
            dst_path = os.path.join(audio_output_folder, filename)
            shutil.copy2(src_path, dst_path)
            copied_count += 1
        except Exception as e:
            copy_errors.append(f"{filename}: {str(e)}")

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

#
# def main():
#     st.set_page_config(
#         page_title="Noyob Matn va Audio Yig'uvchi",
#         page_icon="🎯",
#         layout="wide"
#     )
#
#     st.title("🎯 Noyob Matn va Audio Yig'uvchi")
#     st.markdown("Bu dastur faqat bir marta uchraydigan matnlarni topadi va tegishli audio fayllarni yig'adi")
#     st.markdown("---")
#
#     # **YANGI MARKAZIY PAPKA TANLASH PANELI**
#     st.subheader("📂 Ishlash Papkasini Tanlang")
#
#     col1, col2, col3 = st.columns([2, 2, 1])
#
#     with col1:
#         # Asosiy yo'nalishlar
#         base_options = get_available_drives_and_folders()
#         selected_base = st.selectbox(
#             "Asosiy papka yoki disk:",
#             options=base_options,
#             format_func=lambda x: f"📁 {os.path.basename(x) or x}",
#             help="Asosiy papka yoki diskni tanlang"
#         )
#
#     with col2:
#         # Tanlangan asosiy papka ichidagi papkalar
#         if selected_base:
#             subfolders = browse_folder(selected_base)
#             if subfolders:
#                 selected_folder = st.selectbox(
#                     "Ichki papka:",
#                     options=['--- Asosiy papkani tanlash ---'] + subfolders,
#                     format_func=lambda x: f"📁 {os.path.basename(x)}" if x != '--- Asosiy papkani tanlash ---' else x,
#                     help="Ichki papkani tanlang yoki asosiy papkani ishlating"
#                 )
#
#                 final_folder = selected_base if selected_folder == '--- Asosiy papkani tanlash ---' else selected_folder
#             else:
#                 final_folder = selected_base
#                 st.info("Bu papkada ichki papkalar yo'q")
#
#     with col3:
#         # Tanlangan papka haqida ma'lumot
#         if 'final_folder' in locals():
#             if os.path.exists(final_folder):
#                 json_files, audio_files = scan_folder_for_files(final_folder)
#                 st.metric("JSON", len(json_files))
#                 st.metric("Audio", len(audio_files))
#
#     # Tanlangan papka yo'lini ko'rsatish
#     if 'final_folder' in locals():
#         st.code(f"Tanlangan papka: {final_folder}")
#
#         # Jarayonni boshlash tugmasi
#         if json_files or audio_files:
#             if st.button("🚀 Tahlilni Boshlash", type="primary", use_container_width=True):
#                 st.session_state.start_processing = True
#                 st.session_state.folder_path = final_folder
#                 st.session_state.json_files = json_files
#                 st.session_state.audio_files = audio_files
#         else:
#             st.warning("⚠️ Bu papkada JSON yoki audio fayllar topilmadi")
#
#     st.markdown("---")
#
#     # **TAHLIL NATIJALARI**
#     if hasattr(st.session_state, 'start_processing') and st.session_state.start_processing:
#
#         with st.spinner("Ma'lumotlar tahlil qilinmoqda..."):
#             all_json_data, failed_files = load_all_json_files(st.session_state.json_files)
#             unique_items, duplicate_texts, text_counts = find_unique_texts_detailed(all_json_data)
#             matched_files, unmatched_files = match_audio_files(unique_items, st.session_state.audio_files)
#
#         # Statistikalar
#         col1, col2, col3, col4 = st.columns(4)
#         with col1:
#             st.metric("📊 Umumiy JSON yozuvlar", len(all_json_data))
#         with col2:
#             st.metric("🎯 Noyob matnlar", len(unique_items))
#         with col3:
#             st.metric("🔄 Takroriy matnlar", len(duplicate_texts))
#         with col4:
#             st.metric("🎵 Moslashgan audiolar", len(matched_files))
#
#         st.markdown("---")
#
#         # Tablar (oldingi kodda bo'lgandek)
#         tab1, tab2, tab3, tab4 = st.tabs(
#             ["📋 Noyob Ma'lumotlar", "📊 Statistika", "⚠️ Takroriy Matnlar", "⬇️ Yuklab Olish"]
#         )
#
#         with tab1:
#             if unique_items:
#                 df = create_statistics_dataframe(unique_items)
#                 st.dataframe(df, use_container_width=True, height=400)
#                 csv = df.to_csv(index=False, encoding='utf-8')
#                 st.download_button(
#                     label="📄 CSV formatda yuklab olish",
#                     data=csv.encode('utf-8'),
#                     file_name="noyob_matnlar.csv",
#                     mime="text/csv"
#                 )
#             else:
#                 st.warning("Noyob matnlar topilmadi")
#
#         with tab2:
#             col_stat1, col_stat2 = st.columns(2)
#             with col_stat1:
#                 st.subheader("📈 Kategoriya bo'yicha")
#                 if unique_items:
#                     categories = [item.get('category', 'Noma\'lum') for item in unique_items]
#                     category_counts = Counter(categories)
#                     st.bar_chart(category_counts)
#             with col_stat2:
#                 st.subheader("👥 Jins bo'yicha")
#                 if unique_items:
#                     genders = [item.get('gender', 'Noma\'lum') for item in unique_items]
#                     gender_counts = Counter(genders)
#                     st.bar_chart(gender_counts)
#
#         with tab3:
#             if duplicate_texts:
#                 st.subheader(f"🔄 Takroriy matnlar ({len(duplicate_texts)} ta)")
#                 duplicate_df = pd.DataFrame([
#                     {'Matn': text[:100] + '...' if len(text) > 100 else text,
#                      'Takrorlanish soni': count}
#                     for text, count in duplicate_texts.items()
#                 ])
#                 st.dataframe(duplicate_df, use_container_width=True)
#             else:
#                 st.success("✅ Takroriy matnlar topilmadi!")
#
#         with tab4:
#             st.subheader("📦 Paket yaratish va yuklab olish")
#             output_folder = st.text_input(
#                 "Chiqish papkasi:",
#                 value=os.path.join(st.session_state.folder_path, "noyob_natija"),
#                 help="Noyob fayllar saqlanadigan papka"
#             )
#
#             col_btn1, col_btn2 = st.columns(2)
#             with col_btn1:
#                 if st.button("📁 Fayllarni papkaga saqlash", type="primary"):
#                     if output_folder:
#                         with st.spinner("Fayllar nusxalanmoqda..."):
#                             report = create_output_package(unique_items, matched_files, output_folder)
#                             st.success(f"✅ Jarayon tugadi!")
#                             st.json({
#                                 "Umumiy JSON": report['umumiy_json'],
#                                 "Moslashgan audio": report['moslashgan_audio'],
#                                 "Nusxalangan audio": report['nusxalangan_audio'],
#                                 "JSON fayl yo'li": report['json_fayl'],
#                                 "Audio papka yo'li": report['audio_papka']
#                             })
#
#             with col_btn2:
#                 if st.button("📦 ZIP faylni yaratish"):
#                     with st.spinner("ZIP fayl yaratilmoqda..."):
#                         temp_dir = tempfile.mkdtemp()
#                         report = create_output_package(unique_items, matched_files, temp_dir)
#                         zip_path = os.path.join(temp_dir, "noyob_dataset.zip")
#
#                         with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
#                             for root, dirs, files in os.walk(temp_dir):
#                                 for file in files:
#                                     if file != "noyob_dataset.zip":
#                                         file_path = os.path.join(root, file)
#                                         arcname = os.path.relpath(file_path, temp_dir)
#                                         zipf.write(file_path, arcname)
#
#                         with open(zip_path, 'rb') as f:
#                             st.download_button(
#                                 label="⬇️ ZIP faylni yuklab olish",
#                                 data=f.read(),
#                                 file_name="noyob_dataset.zip",
#                                 mime="application/zip"
#                             )
#                         st.success(f"ZIP yaratildi: {report['nusxalangan_audio']} audio fayl")
#
#     else:
#         # Boshlash ko'rsatmalari
#         st.info("👆 Yuqorida papkani tanlang va tahlilni boshlang")
#         st.markdown("""
#         ### 📖 Qo'llanma:
#         1. **Papka tanlash**: Yuqorida disk yoki papka tanlang
#         2. **Ichki papka**: Kerak bo'lsa ichki papkani ham tanlang
#         3. **Avtomatik skanerlash**: JSON va audio fayllar avtomatik topiladi
#         4. **Tahlil boshlash**: "🚀 Tahlilni Boshlash" tugmasini bosing
#         5. **Natijani yuklab olish**: Papka yoki ZIP formatida saqlang
#
#         ### ⚙️ Talablar:
#         - JSON fayllar `text` maydoniga ega bo'lishi kerak
#         - Audio fayl nomlari JSON dagi `utt_id` ga mos kelishi kerak
#         """)
#
#
# if __name__ == "__main__":
#     main()

def main():
    st.set_page_config(
        page_title="Noyob Matn va Audio Yig'uvchi",
        page_icon="🎯",
        layout="wide"
    )

    st.title("🎯 Noyob Matn va Audio Yig'uvchi")
    st.markdown("Bu dastur faqat bir marta uchraydigan matnlarni topadi va tegishli audio fayllarni yig'adi")
    st.markdown("---")

    # **YANGILANGAN PAPKA TANLASH PANELI**
    st.subheader("📂 Asosiy Papkani Tanlang")

    # Papka yo'lini kiritish uchun text input
    col1, col2 = st.columns([3, 1])

    with col1:
        folder_path = st.text_input(
            "Papka yo'lini kiriting:",
            value=st.session_state.get('last_folder_path', os.getcwd()),
            help="To'liq papka yo'lini kiriting (masalan: C:\\Users\\Username\\Desktop\\MyFolder)",
            placeholder="Papka yo'lini bu yerga yozing..."
        )

    with col2:
        st.markdown("<br>", unsafe_allow_html=True)  # Bo'sh joy qo'shish

        # Agar folder path mavjud bo'lsa, uni tekshirish tugmasi
        if st.button("🔍 Tekshirish", type="secondary"):
            if folder_path and os.path.exists(folder_path) and os.path.isdir(folder_path):
                st.session_state.last_folder_path = folder_path
                st.session_state.folder_validated = True
                st.rerun()
            else:
                st.error("⚠️ Bunday papka topilmadi yoki noto'g'ri yo'l kiritilgan!")

    # Tez tanlash uchun umumiy papkalar
    st.markdown("**Tez tanlash:**")
    quick_options = [
        ("🏠 Home", os.path.expanduser("~")),
        ("🖥️ Desktop", os.path.expanduser("~/Desktop")),
        ("📄 Documents", os.path.expanduser("~/Documents")),
        ("📥 Downloads", os.path.expanduser("~/Downloads")),
        ("💼 Joriy papka", os.getcwd())
    ]

    cols = st.columns(len(quick_options))
    for i, (label, path) in enumerate(quick_options):
        with cols[i]:
            if st.button(label, key=f"quick_{i}"):
                if os.path.exists(path):
                    st.session_state.last_folder_path = path
                    folder_path = path
                    st.session_state.folder_validated = True
                    st.rerun()

    # Papka ma'lumotlarini ko'rsatish
    if folder_path and os.path.exists(folder_path) and os.path.isdir(folder_path):
        st.success(f"✅ Tanlangan papka: `{folder_path}`")

        # Papka tarkibini skanerlash
        json_files, audio_files = scan_folder_for_files(folder_path)

        # Statistika ko'rsatish
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📄 JSON fayllar", len(json_files))
        with col2:
            st.metric("🎵 Audio fayllar", len(audio_files))
        with col3:
            try:
                total_files = len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
                st.metric("📁 Jami fayllar", total_files)
            except:
                st.metric("📁 Jami fayllar", "N/A")

        # Jarayonni boshlash tugmasi
        if json_files:
            if st.button("🚀 Tahlilni Boshlash", type="primary", use_container_width=True):
                st.session_state.start_processing = True
                st.session_state.folder_path = folder_path
                st.session_state.json_files = json_files
                st.session_state.audio_files = audio_files
                st.rerun()
        else:
            st.warning("⚠️ Bu papkada JSON fayllar topilmadi!")

        # Fayllar ro'yxatini ko'rsatish (opsional)
        if st.checkbox("📋 Topilgan fayllarni ko'rsatish"):
            if json_files:
                st.write("**JSON fayllar:**")
                for i, file_path in enumerate(json_files, 1):
                    st.write(f"{i}. {os.path.basename(file_path)}")

            if audio_files:
                st.write("**Audio fayllar:**")
                for i, (name, file_path) in enumerate(audio_files.items(), 1):
                    st.write(f"{i}. {os.path.basename(file_path)}")

    elif folder_path:
        st.error("❌ Kiritilgan papka mavjud emas yoki noto'g'ri!")

    st.markdown("---")

    # **TAHLIL NATIJALARI** - qolgan kod o'zgarishsiz
    if hasattr(st.session_state, 'start_processing') and st.session_state.start_processing:

        with st.spinner("Ma'lumotlar tahlil qilinmoqda..."):
            all_json_data, failed_files = load_all_json_files(st.session_state.json_files)
            unique_items, duplicate_texts, text_counts = find_unique_texts_detailed(all_json_data)
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

        # Tablar (oldingi kodda bo'lgandek)
        tab1, tab2, tab3, tab4 = st.tabs(
            ["📋 Noyob Ma'lumotlar", "📊 Statistika", "⚠️ Takroriy Matnlar", "⬇️ Yuklab Olish"]
        )

        with tab1:
            if unique_items:
                df = create_statistics_dataframe(unique_items)
                st.dataframe(df, use_container_width=True, height=400)
                csv = df.to_csv(index=False, encoding='utf-8')
                st.download_button(
                    label="📄 CSV formatda yuklab olish",
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
                            st.json({
                                "Umumiy JSON": report['umumiy_json'],
                                "Moslashgan audio": report['moslashgan_audio'],
                                "Nusxalangan audio": report['nusxalangan_audio'],
                                "JSON fayl yo'li": report['json_fayl'],
                                "Audio papka yo'li": report['audio_papka']
                            })

            with col_btn2:
                if st.button("📦 ZIP faylni yaratish"):
                    with st.spinner("ZIP fayl yaratilmoqda..."):
                        temp_dir = tempfile.mkdtemp()
                        report = create_output_package(unique_items, matched_files, temp_dir)
                        zip_path = os.path.join(temp_dir, "noyob_dataset.zip")

                        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            for root, dirs, files in os.walk(temp_dir):
                                for file in files:
                                    if file != "noyob_dataset.zip":
                                        file_path = os.path.join(root, file)
                                        arcname = os.path.relpath(file_path, temp_dir)
                                        zipf.write(file_path, arcname)

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
        st.info("👆 Yuqorida papka yo'lini kiriting va tahlilni boshlang")
        st.markdown("""
        ### 📖 Qo'llanma:
        1. **Papka yo'lini kiriting**: Yuqorida to'liq papka yo'lini yozing
        2. **Tez tanlash**: Umumiy papkalar tugmalaridan foydalaning
        3. **Tekshirish**: Papka mavjudligini tekshiring
        4. **Avtomatik skanerlash**: JSON va audio fayllar avtomatik topiladi
        5. **Tahlil boshlash**: "🚀 Tahlilni Boshlash" tugmasini bosing
        6. **Natijani yuklab olish**: Papka yoki ZIP formatida saqlang

        ### ⚙️ Talablar:
        - JSON fayllar `text` maydoniga ega bo'lishi kerak
        - Audio fayl nomlari JSON dagi `utt_id` ga mos kelishi kerak

        ### 💡 Misol yo'llar:
        - Windows: `C:\\Users\\Username\\Desktop\\MyData`
        - Linux/Mac: `/home/username/Desktop/MyData`
        """)


if __name__ == "__main__":
    main()
