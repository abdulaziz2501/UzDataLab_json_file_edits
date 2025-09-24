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


def find_unique_and_duplicate_texts(json_data, similarity_threshold=0.95):
    """Matnlarni unique va duplicate guruhlariga bo'lish"""
    from difflib import SequenceMatcher

    text_groups = defaultdict(list)
    processed_texts = []

    # Har bir elementni tekshirish
    for item in json_data:
        text = item.get('text', '').strip()
        if not text:
            continue

        # O'xshashlik tekshiruvi
        found_group = False
        for existing_text in processed_texts:
            similarity = SequenceMatcher(None, text.lower(), existing_text.lower()).ratio()
            if similarity >= similarity_threshold:
                text_groups[existing_text].append(item)
                found_group = True
                break

        if not found_group:
            processed_texts.append(text)
            text_groups[text].append(item)

    # Unique va duplicate ajratish
    unique_items = []
    duplicate_groups = {}

    for text, items in text_groups.items():
        if len(items) == 1:
            unique_items.extend(items)
        else:
            # Takroriy guruhdan birinchisini olish (yoki eng yaxshisini tanlash)
            best_item = items[0]  # Yoki boshqa mezon bo'yicha eng yaxshisini tanlash
            unique_items.append(best_item)
            duplicate_groups[text] = {
                'count': len(items),
                'selected_item': best_item,
                'all_items': items
            }

    return unique_items, duplicate_groups


def match_audio_files(unique_items, all_audio_files):
    """Audio fayllarni JSON bilan moslashtirish"""
    matched_files = []
    unmatched_files = []

    for item in unique_items:
        audio_found = False

        # Har xil maydonlar bo'yicha qidirish
        search_fields = ['utt_id', 'id', 'file_id', 'filename']

        for field in search_fields:
            if field in item and item[field] in all_audio_files:
                matched_files.append({
                    'json_item': item,
                    'audio_path': all_audio_files[item[field]],
                    'match_method': field
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

            # Agar fayl allaqachon mavjud bo'lsa, noyob nom berish
            counter = 1
            base_name, ext = os.path.splitext(filename)
            while os.path.exists(dst_path):
                new_filename = f"{base_name}_{counter}{ext}"
                dst_path = os.path.join(audio_output_folder, new_filename)
                counter += 1

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


def main():
    st.set_page_config(
        page_title="Noyob Matn va Audio Yig'uvchi",
        page_icon="🎯",
        layout="wide"
    )

    st.title("🎯 Noyob Matn va Audio Yig'uvchi")
    st.markdown("Bu dastur bir nechta papkalardan noyob matnlarni topadi va tegishli audio fayllarni yig'adi")
    st.markdown("---")

    # Session state initialization
    if 'selected_folders' not in st.session_state:
        st.session_state.selected_folders = []
    if 'all_json_data' not in st.session_state:
        st.session_state.all_json_data = []
    if 'all_audio_files' not in st.session_state:
        st.session_state.all_audio_files = {}

    # **PAPKA TANLASH PANELI**
    st.subheader("📂 Papkalarni Tanlang")

    # Yangi papka qo'shish
    col1, col2 = st.columns([3, 1])

    with col1:
        folder_path = st.text_input(
            "Papka yo'lini kiriting:",
            value=st.session_state.get('last_folder_path', os.getcwd()),
            help="To'liq papka yo'lini kiriting",
            placeholder="Papka yo'lini bu yerga yozing..."
        )

    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("➕ Papka Qo'shish", type="primary"):
            if folder_path and os.path.exists(folder_path) and os.path.isdir(folder_path):
                if folder_path not in st.session_state.selected_folders:
                    st.session_state.selected_folders.append(folder_path)
                    st.session_state.last_folder_path = folder_path
                    st.success(f"✅ Papka qo'shildi: {os.path.basename(folder_path)}")
                    st.rerun()
                else:
                    st.warning("⚠️ Bu papka allaqachon qo'shilgan!")
            else:
                st.error("⚠️ Bunday papka topilmadi!")

    # Tez tanlash tugmalari
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
                    st.rerun()

    # Tanlangan papkalar ro'yxati
    if st.session_state.selected_folders:
        st.subheader("📋 Tanlangan Papkalar:")

        for i, selected_folder in enumerate(st.session_state.selected_folders):
            col1, col2, col3 = st.columns([4, 1, 1])

            with col1:
                json_files, audio_files = scan_folder_for_files(selected_folder)
                st.write(f"**{i + 1}.** `{selected_folder}` - JSON: {len(json_files)}, Audio: {len(audio_files)}")

            with col2:
                if st.button("👁️", key=f"view_{i}", help="Ko'rish"):
                    st.info(f"JSON: {len(json_files)}, Audio: {len(audio_files)}")

            with col3:
                if st.button("🗑️", key=f"remove_{i}", help="O'chirish"):
                    st.session_state.selected_folders.pop(i)
                    st.rerun()

        # Barcha papkalarni tahlil qilish
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🚀 Barcha Papkalarni Tahlil Qilish", type="primary", use_container_width=True):
                st.session_state.start_processing = True
                st.rerun()

        with col2:
            if st.button("🗑️ Hammasini Tozalash", type="secondary", use_container_width=True):
                st.session_state.selected_folders = []
                st.session_state.all_json_data = []
                st.session_state.all_audio_files = {}
                if 'start_processing' in st.session_state:
                    del st.session_state.start_processing
                st.rerun()

    st.markdown("---")

    # **TAHLIL NATIJALARI**
    if hasattr(st.session_state,
               'start_processing') and st.session_state.start_processing and st.session_state.selected_folders:

        with st.spinner("Barcha papkalar tahlil qilinmoqda..."):
            # Barcha papkalardan ma'lumotlarni yig'ish
            all_json_files = []
            all_audio_files = {}

            for folder in st.session_state.selected_folders:
                json_files, audio_files = scan_folder_for_files(folder)
                all_json_files.extend(json_files)
                all_audio_files.update(audio_files)  # Audio fayllarni birlashtirish

            # JSON ma'lumotlarni yuklash
            all_json_data, failed_files = load_all_json_files(all_json_files)

            # O'xshashlik darajasini sozlash
            similarity_threshold = st.slider(
                "📏 Matn o'xshashlik darajasi (%)",
                min_value=70, max_value=100, value=95, step=5,
                help="Qanchalik o'xshash matnlarni bir xil deb hisoblash kerak"
            ) / 100.0

            # Unique va duplicate matnlarni topish
            unique_items, duplicate_groups = find_unique_and_duplicate_texts(all_json_data, similarity_threshold)
            matched_files, unmatched_files = match_audio_files(unique_items, all_audio_files)

        # Statistikalar
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("📂 Papkalar", len(st.session_state.selected_folders))
        with col2:
            st.metric("📊 Jami JSON", len(all_json_data))
        with col3:
            st.metric("🎯 Noyob matnlar", len(unique_items))
        with col4:
            st.metric("🔄 Takroriy guruhlar", len(duplicate_groups))
        with col5:
            st.metric("🎵 Moslashgan audio", len(matched_files))

        st.markdown("---")

        # Tablar
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
            if duplicate_groups:
                st.subheader(f"🔄 Takroriy matn guruhlari ({len(duplicate_groups)} ta)")
                for text, info in list(duplicate_groups.items())[:10]:  # Faqat birinchi 10 ta
                    with st.expander(f"Takrorlanish soni: {info['count']} - {text[:50]}..."):
                        st.write("**Tanlangan variant:**")
                        st.json(info['selected_item'])
                        st.write(f"**Jami {info['count']} ta variant mavjud**")
            else:
                st.success("✅ Takroriy matnlar topilmadi!")

        with tab4:
            st.subheader("📦 Final Dataset yaratish")

            # Output folder tanlash
            default_output = os.path.join(os.getcwd(), "final_unique_dataset")
            output_folder = st.text_input(
                "Chiqish papkasi:",
                value=default_output,
                help="Yakuniy noyob dataset saqlanadigan papka"
            )

            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                if st.button("📁 Final Dataset Yaratish", type="primary"):
                    if output_folder:
                        with st.spinner("Final dataset yaratilmoqda..."):
                            report = create_output_package(unique_items, matched_files, output_folder)
                            st.success(f"✅ Final dataset yaratildi!")
                            st.json({
                                "Jami papkalar": len(st.session_state.selected_folders),
                                "Jami JSON yozuvlar": len(all_json_data),
                                "Final noyob JSON": report['umumiy_json'],
                                "Moslashgan audio": report['moslashgan_audio'],
                                "Nusxalangan audio": report['nusxalangan_audio'],
                                "Dataset manzili": report['json_fayl'],
                                "Audio papka": report['audio_papka']
                            })

            with col_btn2:
                if st.button("📦 ZIP Dataset"):
                    with st.spinner("ZIP dataset yaratilmoqda..."):
                        temp_dir = tempfile.mkdtemp()
                        report = create_output_package(unique_items, matched_files, temp_dir)
                        zip_path = os.path.join(temp_dir, "final_unique_dataset.zip")

                        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            for root, dirs, files in os.walk(temp_dir):
                                for file in files:
                                    if file != "final_unique_dataset.zip":
                                        file_path = os.path.join(root, file)
                                        arcname = os.path.relpath(file_path, temp_dir)
                                        zipf.write(file_path, arcname)

                        with open(zip_path, 'rb') as f:
                            st.download_button(
                                label="⬇️ Final Dataset ZIP yuklab olish",
                                data=f.read(),
                                file_name="final_unique_dataset.zip",
                                mime="application/zip"
                            )
                        st.success(f"✅ ZIP yaratildi: {report['nusxalangan_audio']} audio fayl")

    else:
        # Boshlash ko'rsatmalari
        st.info("👆 Papkalarni qo'shing va tahlilni boshlang")
        st.markdown("""
        ### 📖 Qo'llanma:
        1. **Papka qo'shish**: Har bir papka yo'lini kiriting va "➕ Papka Qo'shish" bosing
        2. **Bir nechta papka**: Kerakli barcha papkalarni qo'shing  
        3. **Tanlangan papkalar**: Qo'shilgan papkalarni ko'rib chiqing
        4. **Tahlil boshlash**: "🚀 Barcha Papkalarni Tahlil Qilish" bosing
        5. **O'xshashlik sozlash**: Matn o'xshashlik darajasini belgilang
        6. **Final dataset**: Birlashtirilgan noyob ma'lumotlarni yuklab oling

        ### ⚙️ Xususiyatlar:
        - **Bir nechta papka**: Bir vaqtning o'zida ko'plab papkalarni tahlil qilish
        - **Takroriy boshqaruv**: O'xshash matnlardan eng yaxshisini tanlash  
        - **Global uniqueness**: Barcha papkalar bo'yicha noyoblik tekshiruvi
        - **Audio moslashtirish**: JSON va audio fayllarni avtomatik moslashtirish
        """)


if __name__ == "__main__":
    main()