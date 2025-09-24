import streamlit as st
import json
import os
import shutil
import pandas as pd
from pathlib import Path
import zipfile
import tempfile
from collections import defaultdict, Counter
from difflib import SequenceMatcher
import hashlib
from datetime import datetime


def clean_text(text):
    """Matnni tozalash va normalizatsiya qilish"""
    import re
    if not text:
        return ""

    # Kichik harflarga o'tkazish
    text = text.lower().strip()

    # Ortiqcha bo'sh joylarni olib tashlash
    text = re.sub(r'\s+', ' ', text)

    # Maxsus belgilarni olib tashlash (ixtiyoriy)
    text = re.sub(r'[^\w\s]', '', text)

    return text


def calculate_comprehensive_similarity(item1, item2, weights=None):
    """Ko'p mezonli o'xshashlik hisoblagich"""
    if weights is None:
        weights = {
            'text': 0.6,  # Matn o'xshashligi (eng muhim)
            'duration': 0.15,  # Davomiylik o'xshashligi
            'audio_name': 0.1,  # Audio fayl nomi
            'metadata': 0.15  # Boshqa metadata
        }

    similarities = {}

    # 1. MATN O'XSHASHLIGI
    text1 = clean_text(item1.get('text', ''))
    text2 = clean_text(item2.get('text', ''))

    if text1 and text2:
        # Qisqa matnlar uchun to'liq mos kelish talab qilish
        if len(text1) < 20 or len(text2) < 20:
            similarities['text'] = 1.0 if text1 == text2 else 0.0
        else:
            # Uzun matnlar uchun o'xshashlik darajasi
            similarities['text'] = SequenceMatcher(None, text1, text2).ratio()
    else:
        similarities['text'] = 0.0

    # 2. DAVOMIYLIK O'XSHASHLIGI
    dur1 = item1.get('duration_ms')
    dur2 = item2.get('duration_ms')

    if dur1 is not None and dur2 is not None:
        try:
            dur1, dur2 = float(dur1), float(dur2)
            if dur1 > 0 and dur2 > 0:
                # Davomiylik farqi 5% dan kam bo'lsa o'xshash deb hisoblaymiz
                duration_diff = abs(dur1 - dur2) / max(dur1, dur2)
                similarities['duration'] = max(0, 1.0 - duration_diff * 20)  # 5% farq = 0 o'xshashlik
            else:
                similarities['duration'] = 0.0
        except (ValueError, TypeError):
            similarities['duration'] = 0.0
    else:
        similarities['duration'] = 0.5  # Ma'lumot yo'q bo'lsa neytral

    # 3. AUDIO FAYL NOMI O'XSHASHLIGI
    audio_id1 = item1.get('utt_id', item1.get('id', item1.get('file_id', '')))
    audio_id2 = item2.get('utt_id', item2.get('id', item2.get('file_id', '')))

    if audio_id1 and audio_id2:
        similarities['audio_name'] = 1.0 if audio_id1 == audio_id2 else 0.0
    else:
        similarities['audio_name'] = 0.5  # Ma'lumot yo'q

    # 4. METADATA O'XSHASHLIGI
    metadata_score = 0
    metadata_count = 0

    # Spiker ID tekshiruvi
    speaker1 = item1.get('speaker_id')
    speaker2 = item2.get('speaker_id')
    if speaker1 is not None and speaker2 is not None:
        metadata_score += 1.0 if speaker1 == speaker2 else 0.0
        metadata_count += 1

    # Kategoriya tekshiruvi
    cat1 = item1.get('category')
    cat2 = item2.get('category')
    if cat1 is not None and cat2 is not None:
        metadata_score += 1.0 if cat1 == cat2 else 0.0
        metadata_count += 1

    # Jins tekshiruvi
    gender1 = item1.get('gender')
    gender2 = item2.get('gender')
    if gender1 is not None and gender2 is not None:
        metadata_score += 1.0 if gender1 == gender2 else 0.0
        metadata_count += 1

    # Sana tekshiruvi (kun darajasida)
    date1 = item1.get('created_at', '')[:10] if item1.get('created_at') else ''
    date2 = item2.get('created_at', '')[:10] if item2.get('created_at') else ''
    if date1 and date2:
        metadata_score += 1.0 if date1 == date2 else 0.0
        metadata_count += 1

    similarities['metadata'] = metadata_score / metadata_count if metadata_count > 0 else 0.5

    # UMUMIY O'XSHASHLIK HISOBLASH
    weighted_score = sum(similarities[key] * weights[key] for key in similarities)

    return weighted_score, similarities


def find_unique_items_advanced(json_data, similarity_threshold=0.85, strict_mode=True):
    """Kengaytirilgan takroriylik aniqlash tizimi"""

    # Har bir element uchun unique hash yaratish (tez tekshirish uchun)
    item_hashes = {}
    for i, item in enumerate(json_data):
        text = clean_text(item.get('text', ''))
        duration = str(item.get('duration_ms', ''))
        audio_id = item.get('utt_id', item.get('id', ''))

        # Hash yaratish
        hash_string = f"{text}_{duration}_{audio_id}"
        item_hash = hashlib.md5(hash_string.encode()).hexdigest()

        if item_hash not in item_hashes:
            item_hashes[item_hash] = []
        item_hashes[item_hash].append((i, item))

    # To'liq bir xil elementlarni topish
    exact_duplicates = {h: items for h, items in item_hashes.items() if len(items) > 1}

    # O'xshash elementlarni topish
    unique_items = []
    duplicate_groups = {}
    processed_indices = set()

    progress_bar = st.progress(0)
    total_comparisons = len(json_data)

    for i, item in enumerate(json_data):
        progress_bar.progress((i + 1) / total_comparisons)

        if i in processed_indices:
            continue

        # Joriy element bilan o'xshash elementlarni topish
        similar_items = [item]
        similar_indices = [i]

        for j, other_item in enumerate(json_data[i + 1:], i + 1):
            if j in processed_indices:
                continue

            # O'xshashlikni hisoblash
            similarity_score, detailed_scores = calculate_comprehensive_similarity(item, other_item)

            if similarity_score >= similarity_threshold:
                # Agar strict mode bo'lsa, qo'shimcha tekshiruvlar
                if strict_mode:
                    # Matn juda o'xshash bo'lishi kerak
                    if detailed_scores['text'] < 0.9:
                        continue

                    # Audio ID lar farq qilishi kerak
                    if detailed_scores['audio_name'] > 0.8:
                        continue

                similar_items.append(other_item)
                similar_indices.append(j)

        # Agar o'xshash elementlar topilsa
        if len(similar_items) > 1:
            # Eng yaxshi variantni tanlash
            best_item = select_best_item(similar_items)
            unique_items.append(best_item)

            # Takroriy guruhga qo'shish
            group_key = f"Group_{len(duplicate_groups) + 1}"
            duplicate_groups[group_key] = {
                'count': len(similar_items),
                'selected_item': best_item,
                'all_items': similar_items,
                'similarity_scores': [calculate_comprehensive_similarity(item, other)[0] for other in similar_items[1:]]
            }

            # Ishlatilgan indekslarni belgilash
            processed_indices.update(similar_indices)
        else:
            # Noyob element
            unique_items.append(item)
            processed_indices.add(i)

    progress_bar.empty()

    return unique_items, duplicate_groups, exact_duplicates


def select_best_item(items):
    """Bir nechta o'xshash elementdan eng yaxshisini tanlash"""
    if len(items) == 1:
        return items[0]

    scores = []
    for item in items:
        score = 0

        # Matn uzunligi (uzunroq matn yaxshiroq)
        text_length = len(item.get('text', ''))
        score += text_length * 0.3

        # Davomiylik mavjudligi
        if item.get('duration_ms'):
            score += 100

        # Metadata to'liqligi
        metadata_fields = ['speaker_id', 'gender', 'category', 'region', 'created_at']
        filled_fields = sum(1 for field in metadata_fields if item.get(field))
        score += filled_fields * 50

        # Audio ID mavjudligi
        if item.get('utt_id') or item.get('id'):
            score += 200

        scores.append(score)

    # Eng yuqori ball olgan elementni qaytarish
    best_index = scores.index(max(scores))
    return items[best_index]


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


def match_audio_files(unique_items, all_audio_files):
    """Audio fayllarni JSON bilan moslashtirish"""
    matched_files = []
    unmatched_files = []

    for item in unique_items:
        audio_found = False
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
        page_title="Kengaytirilgan Noyob Dataset Yig'uvchi",
        page_icon="🎯",
        layout="wide"
    )

    st.title("🎯 Kengaytirilgan Noyob Dataset Yig'uvchi")
    st.markdown("Bu dastur ko'p mezonli tahlil orqali haqiqiy noyob matnlarni topadi")
    st.markdown("---")

    # Session state initialization
    if 'selected_folders' not in st.session_state:
        st.session_state.selected_folders = []

    # **PAPKA TANLASH**
    st.subheader("📂 Papkalarni Tanlang")

    col1, col2 = st.columns([3, 1])

    with col1:
        folder_path = st.text_input(
            "Papka yo'lini kiriting:",
            value=st.session_state.get('last_folder_path', os.getcwd()),
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

        # **SOZLAMALAR**
        st.subheader("⚙️ Aniqlash Sozlamalari")

        col1, col2, col3 = st.columns(3)

        with col1:
            similarity_threshold = st.slider(
                "🎯 O'xshashlik chegarasi (%)",
                min_value=70, max_value=95, value=85, step=5,
                help="Qanchalik o'xshash elementlarni takroriy deb hisoblash"
            ) / 100.0

        with col2:
            strict_mode = st.checkbox(
                "",
                value=False,
                help="Qo'shimcha tekshiruvlar bilan aniqroq natija"
            )

        with col3:
            show_details = st.checkbox(
                "📊 Batafsil hisobot",
                value=False,
                help="Har bir takroriy guruh uchun batafsil ma'lumot"
            )

        # TAHLIL BOSHLASH
        if st.button("🚀 Kengaytirilgan Tahlil", type="primary", use_container_width=True):
            st.session_state.start_processing = True
            st.session_state.similarity_threshold = similarity_threshold
            st.session_state.strict_mode = strict_mode
            st.session_state.show_details = show_details
            st.rerun()

    st.markdown("---")

    # **TAHLIL NATIJALARI**
    if hasattr(st.session_state,
               'start_processing') and st.session_state.start_processing and st.session_state.selected_folders:

        with st.spinner("🔍 Kengaytirilgan tahlil amalga oshirilmoqda..."):
            # Barcha papkalardan ma'lumotlarni yig'ish
            all_json_files = []
            all_audio_files = {}

            for folder in st.session_state.selected_folders:
                json_files, audio_files = scan_folder_for_files(folder)
                all_json_files.extend(json_files)
                all_audio_files.update(audio_files)

            # JSON ma'lumotlarni yuklash
            all_json_data, failed_files = load_all_json_files(all_json_files)

            st.info(f"📊 Jami {len(all_json_data)} ta element tahlil qilinmoqda...")

            # Kengaytirilgan noyob elementlarni topish
            unique_items, duplicate_groups, exact_duplicates = find_unique_items_advanced(
                all_json_data,
                st.session_state.similarity_threshold,
                st.session_state.strict_mode
            )

            matched_files, unmatched_files = match_audio_files(unique_items, all_audio_files)

        # NATIJALAR
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("📂 Papkalar", len(st.session_state.selected_folders))
        with col2:
            st.metric("📊 Jami JSON", len(all_json_data))
        with col3:
            st.metric("🎯 Noyob elementlar", len(unique_items))
        with col4:
            st.metric("🔄 Takroriy guruhlar", len(duplicate_groups))
        with col5:
            st.metric("🎵 Audio moslashgan", len(matched_files))

        # Samaradorlik ko'rsatkichi
        efficiency = (len(unique_items) / len(all_json_data)) * 100 if all_json_data else 0
        st.success(
            f"Samaradorlik: {efficiency:.1f}% - {len(all_json_data) - len(unique_items)} ta takroriy element olib tashlandi")

        st.markdown("---")

        # BATAFSIL TABLAR
        tab1, tab2, tab3, tab4 = st.tabs(
            ["📋 Noyob Ma'lumotlar", "📊 Takroriy Tahlil", "⚙️ Sozlamalar", "⬇️ Yuklab Olish"]
        )

        with tab1:
            if unique_items:
                df = create_statistics_dataframe(unique_items)
                st.dataframe(df, use_container_width=True, height=400)

                csv = df.to_csv(index=False, encoding='utf-8')
                st.download_button(
                    label="📄 CSV formatda yuklab olish",
                    data=csv.encode('utf-8'),
                    file_name="noyob_dataset.csv",
                    mime="text/csv"
                )

        with tab2:
            if duplicate_groups:
                st.subheader(f"🔍 Takroriy Guruhlar Tahlili ({len(duplicate_groups)} ta)")

                for group_name, info in list(duplicate_groups.items())[:5]:  # Faqat birinchi 5 ta
                    with st.expander(f"📦 {group_name} - {info['count']} ta element"):
                        col1, col2 = st.columns(2)

                        with col1:
                            st.write("**Tanlangan variant:**")
                            selected = info['selected_item']
                            st.json({
                                'text': selected.get('text', '')[:100] + '...',
                                'utt_id': selected.get('utt_id', 'N/A'),
                                'duration_ms': selected.get('duration_ms', 'N/A'),
                                'speaker_id': selected.get('speaker_id', 'N/A')
                            })

                        with col2:
                            st.write("**Takroriy elementlar:**")
                            for i, item in enumerate(info['all_items'][:3]):  # Faqat 3 ta
                                st.write(f"{i + 1}. {item.get('text', '')[:50]}...")

                            if len(info['all_items']) > 3:
                                st.write(f"... va yana {len(info['all_items']) - 3} ta")

                if st.session_state.show_details:
                    st.write("**Batafsil statistika:**")
                    duplicate_stats = pd.DataFrame([
                        {
                            'Guruh': group_name,
                            'Element soni': info['count'],
                            'Tanlangan matn': info['selected_item'].get('text', '')[:50] + '...'
                        }
                        for group_name, info in duplicate_groups.items()
                    ])
                    st.dataframe(duplicate_stats)

            else:
                st.success("🎉 Takroriy guruhlar topilmadi! Barcha elementlar noyob.")

        with tab3:
            st.subheader("⚙️ Joriy Sozlamalar")
            st.json({
                "O'xshashlik chegarasi": f"{st.session_state.similarity_threshold * 100:.0f}%",
                "Qattiq rejim": st.session_state.strict_mode,
                "Batafsil hisobot": st.session_state.show_details,
                "Tanlangan papkalar soni": len(st.session_state.selected_folders)
            })

            st.markdown("### 🔧 Algoritm Haqida:")
            st.markdown("""
            - **Matn tahlili**: Normalizatsiya va o'xshashlik hisoblash
            - **Davomiylik tekshiruvi**: Audio uzunligi bo'yicha taqqoslash  
            - **Metadata tahlili**: Kategoriya, jins, spiker ID tekshiruvi
            - **Audio fayl**: Noyob nomlanish tekshiruvi
            - **Eng yaxshi tanlash**: To'liq metadata bilan variantni afzal ko'rish
            """)

        with tab4:
            st.subheader("📦 Final Noyob Dataset")

            default_output = os.path.join(os.getcwd(), "Unique_dataset")
            output_folder = st.text_input(
                "Chiqish papkasi:",
                value=default_output
            )

            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                if st.button("📁 Dataset Yaratish", type="primary"):
                    if output_folder:
                        with st.spinner("📦 Dataset yaratilmoqda..."):
                            report = create_output_package(unique_items, matched_files, output_folder)
                            st.success("✅ Dataset tayyor!")
                            st.json({
                                "Jami tahlil qilingan": len(all_json_data),
                                "Final noyob elementlar": report['umumiy_json'],
                                "Moslashgan audio": report['moslashgan_audio'],
                                "Takroriy olib tashlangan": len(all_json_data) - report['umumiy_json'],
                                "Samaradorlik": f"{efficiency:.1f}%",
                                "Dataset joylashuvi": report['json_fayl']
                            })

            with col_btn2:
                if st.button("📦 ZIP Dataset"):
                    with st.spinner("🗜️ ZIP arxiv yaratilmoqda..."):
                        temp_dir = tempfile.mkdtemp()
                        report = create_output_package(unique_items, matched_files, temp_dir)
                        zip_path = os.path.join(temp_dir, "kengaytirilgan_unique_dataset.zip")

                        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            for root, dirs, files in os.walk(temp_dir):
                                for file in files:
                                    if file != "kengaytirilgan_unique_dataset.zip":
                                        file_path = os.path.join(root, file)
                                        arcname = os.path.relpath(file_path, temp_dir)
                                        zipf.write(file_path, arcname)

                        with open(zip_path, 'rb') as f:
                            st.download_button(
                                label="⬇️ Kengaytirilgan Dataset ZIP",
                                data=f.read(),
                                file_name="kengaytirilgan_unique_dataset.zip",
                                mime="application/zip"
                            )

    else:
        # BOSHLASH KO'RSATMALARI
        st.info("👆 Papkalarni qo'shing va kengaytirilgan tahlilni boshlang")
        st.markdown("""
        ### 🎯 Kengaytirilgan Xususiyatlar:

        #### 📊 Ko'p Mezonli Tahlil:
        - **Matn o'xshashligi** (60%): Normalizatsiya bilan
        - **Davomiylik tekshiruvi** (15%): Audio uzunligi bo'yicha
        - **Audio fayl nomi** (10%): Noyob identifikatorlar
        - **Metadata tekshiruvi** (15%): Kategoriya, jins, spiker va sana

        #### ⚡ Optimallashtirish:
        - Hash-based tez tekshirish
        - Progress tracking
        - Eng yaxshi variant avtomatik tanlash
        """)


if __name__ == "__main__":
    main()