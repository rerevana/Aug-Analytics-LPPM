# llm.py

import google.generativeai as genai
import json
import re
import logging
from config import GOOGLE_API_KEY

# Konfigurasi API Google
genai.configure(api_key=GOOGLE_API_KEY)
logger = logging.getLogger(__name__)

# def call_gemini_api(messages: list, model_name: str = "gemini-1.5-flash", temperature: float = 0.1, is_json_output: bool = False):
#     """Fungsi terpusat untuk memanggil Google Gemini API."""
#     model = genai.GenerativeModel(model_name)
    
#     # Gabungkan pesan sistem dan pengguna menjadi satu prompt
#     prompt = "\n".join([msg['content'] for msg in messages])
    
#     generation_config = genai.types.GenerationConfig(
#         temperature=temperature,
#         response_mime_type="application/json" if is_json_output else "text/plain"
#     )

#     try:
#         response = model.generate_content(prompt, generation_config=generation_config)
#         return response.text
#     except Exception as e:
#         logger.error(f"Gagal memanggil Gemini API: {e}", exc_info=True)
#         return json.dumps({"error": str(e)}) if is_json_output else f"Error: {str(e)}"

# def classify_user_intent(user_query: str) -> dict:
#     """Mengklasifikasikan intent pengguna: 'metadata_query' atau 'content_query'."""
#     system_prompt = (
#         "Anda adalah asisten AI yang bertugas mengklasifikasikan pertanyaan pengguna. "
#         "Tentukan apakah pertanyaan tersebut dapat dijawab dengan melihat metadata tabel database (seperti jumlah, daftar, tanggal) "
#         "atau memerlukan pembacaan isi/konten sebuah dokumen (seperti ringkasan, penjelasan, metodologi, kesimpulan).\n\n"
#         "Jawab HANYA dengan JSON yang berisi kunci 'intent' dengan salah satu dari dua nilai ini:\n"
#         "1. 'metadata_query': Untuk pertanyaan tentang data terstruktur (siapa, kapan, berapa banyak, daftar).\n"
#         "   Contoh: 'siapa penulis penelitian terbaru?', 'berapa banyak publikasi di tahun 2023?'.\n"
#         "2. 'content_query': Untuk pertanyaan yang membutuhkan pemahaman isi dokumen (apa, jelaskan, ringkas, bagaimana).\n"
#         "   Contoh: 'jelaskan kesimpulan dari penelitian X', 'apa metodologi yang dipakai di makalah Y?'.\n\n"
#         "3. **'analytical_query'**: Untuk pertanyaan yang memerlukan analisis, perbandingan, tren, atau inferensi kompleks dari data terstruktur, yang mungkin melibatkan agregasi, perbandingan lintas tabel, atau identifikasi pola. "
#            "Contoh: 'bandingkan performa riset di tahun 2022 dan 2023', 'apa tren publikasi di bidang AI selama 5 tahun terakhir?', 'penulis mana yang paling produktif dalam tiga tahun terakhir dan di bidang apa saja?', 'bagaimana korelasi antara jumlah dana riset dan jumlah publikasi?', 'rekomendasikan topik riset berdasarkan tren saat ini'.\n\n"
#         "Contoh output: {\"intent\": \"metadata_query\"}"
#     )
    
#     messages = [
#         {"role": "system", "content": system_prompt},
#         {"role": "user", "content": f"Pertanyaan pengguna: {user_query}"}
#     ]
    
#     response_text = call_gemini_api(messages, is_json_output=True, temperature=0.0)
#     try:
#         return json.loads(response_text)
#     except json.JSONDecodeError:
#         return {"error": "Gagal mem-parse intent", "raw_response": response_text}

def generate_json_map_from_schema_and_query(user_query: str, table_schemas: dict) -> dict:
    """Membuat JSON map untuk query SQL."""
    schemas_str = json.dumps(table_schemas, indent=2)
    system_prompt = (
        "Anda adalah AI yang menerjemahkan permintaan pengguna menjadi JSON terstruktur untuk query SQL.\n\n"
        "Format JSON yang Diharapkan:\n"
        "{\n"
        '  "tabel": "nama_tabel_utama",\n'
        '  "kolom": ["kolom1", "kolom2"],\n'
        '  "join": [ { "tabel": "nama_tabel_join", "on": "tabel1.kolom = tabel2.kolom" } ],\n'
        '  "filter": [ { "kolom": "nama_kolom", "operator": "=", "nilai": "contoh" } ],\n'
        '  "order_by": { "kolom": "nama_kolom", "urutan": "DESC" }\n'
        '  "limit": 10 '
        "}\n\n"
        "Aturan Penting:\n"
        "- Output HARUS berupa JSON valid tanpa komentar atau teks tambahan.\n"
        "- Gunakan limit jika diperlukan"
        "- Pastikan `kolom` yang dipilih sesuai dengan pertanyaan pengguna. Jika pengguna bertanya 'siapa', pilih kolom nama (misal `author.nama`). Jika bertanya 'apa judulnya', pilih kolom judul.\n"
        "- Gunakan operator `ILIKE` untuk pencarian nama atau teks agar tidak case-sensitive.\n"
        "- Perhatikan tipe data dari skema. Jika kolom bertipe `INT64`, `NUMERIC`, atau `FLOAT64`, nilai filter HARUS berupa angka, bukan string (contoh: `\"nilai\": 2022`, bukan `\"nilai\": \"2022\"`).\n"
        "- Jika query menanyakan isi dokumen (misal 'jelaskan makalah...'), pastikan `kolom` yang dipilih adalah kolom yang berisi URL PDF (misal `PDF_makalah`, `PDF_buku`).\n"
        "- Selalu lakukan JOIN jika filter atau kolom yang dipilih membutuhkan data dari tabel lain (misal, filter nama author dari tabel `author`).\n\n"
        f"Tabel Tersedia:\n{schemas_str}"
    )
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Permintaan pengguna: {user_query}"}
    ]
    response_text = call_gemini_api(messages, is_json_output=True, temperature=0.1)
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        return {"error": "Gagal membuat JSON map", "raw_response": response_text}


def generate_sql_from_json_map(json_map: dict, project_id: str, dataset_id: str) -> str:
    """Mengubah JSON map menjadi query SQL BigQuery."""
    json_map_str = json.dumps(json_map, indent=2)
    system_prompt = f"""Anda adalah AI ahli SQL untuk BigQuery. Ubah JSON map berikut menjadi query SQL yang valid.
    Aturan:
    1. Gunakan format nama tabel lengkap: `{project_id}.{dataset_id}.nama_tabel`.
    2. Gunakan backtick (`) untuk semua nama tabel dan kolom.
    3. **Buat Alias**: Untuk tabel utama (`tabel` dari JSON), berikan alias `t1`. Untuk setiap tabel di `join`, berikan alias `t2`, `t3`, dan seterusnya secara berurutan.
    4. **Gunakan Alias di SEMUA Tempat**: Setelah alias dibuat, Anda HARUS menggunakan alias tersebut untuk merujuk ke kolom di klausa `SELECT`, `ON`, `WHERE`, dan `ORDER BY`. CONTOH: `SELECT t1.kolom, t2.kolom ...`. JANGAN PERNAH menggunakan nama tabel lengkap setelah alias dibuat.
    5. **Menentukan Alias untuk Kolom**:
    - Jika nama kolom di JSON mengandung titik (misal: `author.nama`), gunakan alias yang sesuai untuk tabel `author` (misal: `t2.nama`).
    - Jika nama kolom tidak mengandung titik (misal: `laporan_akhir`), asumsikan kolom itu milik tabel utama dan gunakan alias `t1` (misal: `t1.laporan_akhir`).
    6. **ILIKE**: Untuk operator 'ILIKE' digunakan untuk data yang bertipe string, gunakan format `LOWER(alias.kolom) LIKE LOWER('%nilai%')`.
    7. **Format**: Gunakan backtick (`) untuk nama kolom. Hasilkan HANYA kode SQL murni.
    8. Gunakan operator = untuk data berupa int(INT)
    9. **LIMIT**: Implementasikan `limit` jika ditentukan.
    """
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"JSON Map:\n{json_map_str}"}
    ]
    
    response_sql = call_gemini_api(messages, temperature=0.0)
    # Membersihkan output dari markdown
    match = re.search(r"```sql\s*([\s\S]+?)\s*```|([\s\S]+)", response_sql, re.IGNORECASE)
    return (match.group(1) or match.group(2)).strip() if match else response_sql.strip()

# def answer_from_documents(question: str, context_chunks: str) -> str:
#     """Menjawab pertanyaan berdasarkan konteks dokumen (RAG)."""
#     system_prompt = (
#         "Anda adalah asisten AI yang menjawab pertanyaan HANYA berdasarkan konteks dari dokumen yang diberikan. "
#         "Jawab dengan ringkas dan jelas dalam Bahasa Indonesia. "
#         "Jika jawaban tidak ada di dalam konteks, katakan: 'Maaf, saya tidak dapat menemukan jawaban di dalam dokumen yang tersedia.'"
#     )
    
#     messages = [
#         {"role": "system", "content": system_prompt},
#         {"role": "user", "content": f"Konteks Dokumen:\n---\n{context_chunks}\n---\n\nPertanyaan: {question}\n\nJawaban:"}
#     ]
#     return call_gemini_api(messages, temperature=0.3)

# --- Fungsi Utilitas ---
def parse_json_from_response(response_text: str) -> dict:
    """
    Mengekstrak dan mem-parse JSON dari string respons,
    menangani kasus di mana respons dibungkus dalam blok kode markdown.
    """
    # Mencari pola blok kode JSON (```json ... ```)
    match = re.search(r"```json\s*([\s\S]+?)\s*```", response_text, re.IGNORECASE)
    if match:
        json_str = match.group(1).strip()
    else:
        # Jika tidak ada blok kode, asumsikan seluruh respons adalah JSON
        json_str = response_text.strip()
    
    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        logger.error(f"Gagal mem-parse JSON dari respons: {e}. Respons mentah: {json_str}", exc_info=True)
        return {"error": "JSON_PARSE_ERROR", "details": str(e), "raw_response": response_text}

# --- Fungsi API Call ---
def call_gemini_api(messages: list, model_name: str = "gemini-1.5-flash", temperature: float = 0.1, is_json_output: bool = False):
    """
    Fungsi terpusat untuk memanggil Google Gemini API.
    Penyesuaian untuk roles agar sesuai dengan API Gemini.
    """
    model = genai.GenerativeModel(model_name)
    
    combined_prompt_parts = [msg['content'] for msg in messages]

    generation_config = genai.types.GenerationConfig(
        temperature=temperature,
        response_mime_type="application/json" if is_json_output else "text/plain"
    )

    try:
        response = model.generate_content("\n".join(combined_prompt_parts), generation_config=generation_config)
        
        if not response.text:
            logger.warning(f"Respon Gemini API kosong. Prompt: {combined_prompt_parts}")
            return json.dumps({"error": "Empty API response"}) if is_json_output else "Error: Empty API response"

        return response.text
    except genai.types.BlockedPromptException as e:
        logger.error(f"Prompt diblokir oleh Gemini API: {e}", exc_info=True)
        return json.dumps({"error": "PROMPT_BLOCKED", "details": str(e)}) if is_json_output else f"Error: {str(e)}"
    except Exception as e:
        logger.error(f"Gagal memanggil Gemini API: {e}", exc_info=True)
        return json.dumps({"error": "API_CALL_FAILED", "details": str(e)}) if is_json_output else f"Error: {str(e)}"

# --- Fungsi Klasifikasi Intent Pengguna (DIPERBAIKI) ---
def classify_user_intent(user_query: str) -> dict:
    """Mengklasifikasikan intent pengguna: 'metadata_query', 'content_query', atau 'analytical_query'."""
    system_prompt = (
        "Klasifikasikan pertanyaan pengguna ke salah satu intent:\n"
        "1. **'metadata_query'**: Data terstruktur sederhana (Siapa, Kapan, Berapa sederhana, Daftar).\n"
        "   Contoh: 'siapa penulis terbaru?', 'berapa publikasi 2023 di AI?', 'daftar jurnal 2020'.\n"
        "2. **'content_query'**: Pemahaman isi dokumen (Ringkasan, Penjelasan, Metodologi, Kesimpulan, Dampak).\n"
        "   Contoh: 'jelaskan kesimpulan X', 'metodologi Y?', 'ringkas laporan ini'.\n"
        "3. **'analytical_query'**: Analisis data terstruktur (perbandingan, tren, korelasi, rekomendasi).\n"
        "   Contoh: 'bandingkan riset 2022-2023', 'tren publikasi AI 5 tahun terakhir?', 'penulis paling produktif 3 tahun terakhir?'.\n\n"
        "Jawab HANYA dalam JSON: `{\"intent\": \"[jenis_intent]\"}`."
    )
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Pertanyaan pengguna: {user_query}"}
    ]
    
    response_text = call_gemini_api(messages, is_json_output=True, temperature=0.0)
    return parse_json_from_response(response_text)

# # --- Fungsi Pembuatan JSON Map untuk SQL (DIPERBAIKI SECARA SIGNIFIKAN) ---
# def generate_json_map_from_schema_and_query(user_query: str, table_schemas: dict) -> dict:
#     """
#     Membuat JSON map terstruktur untuk query SQL, mendukung analisis data.
#     Prompt ditingkatkan untuk inferensi yang lebih cerdas dan dukungan analisis.
#     """
#     schemas_str = json.dumps(table_schemas, indent=2)
#     system_prompt = (
#         "Terjemahkan permintaan pengguna ke JSON SQL untuk BigQuery, fokus akurasi & analisis mendalam.\n\n"
#         "**Format JSON:**\n"
#         "```json\n"
#         "{\n"
#         '  "tabel": "nama_tabel_utama",\n'
#         '  "kolom": ["kolom1", {"fungsi_agregasi": "COUNT", "kolom": "id", "alias": "total_data"}],\n'
#         '  "join": [ { "tabel": "nama_tabel_join", "on": "tabel1.kolom_fk = tabel2.kolom_pk", "jenis": "INNER JOIN" } ],\n'
#         '  "filter": [ { "kolom": "nama_kolom", "operator": "=", "nilai": "contoh", "kondisi_gabung": "AND" } ],\n'
#         '  "group_by": ["kolom_untuk_grouping"],\n'
#         '  "having": [ { "kolom": "agregasi_kolom", "operator": ">", "nilai": 10 } ],\n'
#         '  "order_by": { "kolom": "nama_kolom", "urutan": "DESC" },\n'
#         '  "limit": 10\n'
#         "}\n"
#         "```\n\n"
#         "**Aturan:**\n"
#         "- Output **HARUS** JSON valid dalam blok ```json ... ```.\n"
#         "- Pilih `tabel` utama yang paling relevan.\n"
#         "- **`kolom`**: Pilih kolom relevan. Agregasi: `{\"fungsi_agregasi\": \"FUNGSI\", \"kolom\": \"kolom\", \"alias\": \"alias\"}`.\n"
#         "- **`join`**: Selalu gunakan `JOIN` jika lintas tabel.\n"
#         "- **`filter`**: `ILIKE` untuk teks (`\"%kata_kunci%\"`). Tipe data nilai harus cocok. Gunakan fungsi SQL (`EXTRACT`, `DATE_SUB`) untuk rentang waktu. Gabungkan dengan `AND`/`OR`.\n"
#         "- **`group_by`, `having`**: Untuk agregasi per kategori / filter agregasi.\n"
#         "- **`order_by`, `limit`**: Untuk pengurutan / pembatasan hasil ('top 5', 'terbaru'). `limit` hanya jika ada `order_by` dan menyiratkan peringkat.\n"
#         "- **`url_pdf`**: Sertakan kolom `url_pdf` atau ID dokumen jika pertanyaan butuh isi dokumen.\n"
#         "- Jika gagal, hasilkan `{\"error\": \"true\", \"reason\": \"[alasan]\"}`.\n\n"
#         f"**Tabel & Skema:**\n{schemas_str}\n\n"
#         # Contoh di sini tetap penting untuk few-shot learning
#         # Saya tidak akan menghapusnya karena akan sangat mempengaruhi performa
#     )
    
#     messages = [
#         {"role": "system", "content": system_prompt},
#         {"role": "user", "content": f"Permintaan pengguna: {user_query}"}
#     ]
#     response_text = call_gemini_api(messages, is_json_output=True, temperature=0.2)
    
#     parsed_response = parse_json_from_response(response_text)
#     if parsed_response.get("error"):
#         logger.error(f"Error from generate_json_map_from_schema_and_query: {parsed_response}")
#         return parsed_response
        
#     return parsed_response


# # --- Fungsi Pembuatan SQL (DIPERBAIKI) ---
# def generate_sql_from_json_map(json_map: dict, project_id: str, dataset_id: str) -> str:
#     """
#     Mengubah JSON map menjadi query SQL BigQuery yang lebih kompleks,
#     termasuk agregasi, GROUP BY, dan HAVING.
#     """
#     json_map_str = json.dumps(json_map, indent=2)
#     system_prompt = f"""Ubah JSON map ini menjadi query SQL BigQuery yang valid & optimal.

# **Aturan:**
# 1.  **Nama Tabel**: `{project_id}.{dataset_id}.`nama_tabel`.
# 2.  **Backtick**: Gunakan ` untuk semua nama tabel & kolom.
# 3.  **Aliasing**: `t1` (utama), `t2`, `t3` dst. (JOIN). Gunakan alias ini di **SEMUA** klausa (SELECT, ON, WHERE, GROUP BY, HAVING, ORDER BY).
# 4.  **Kolom**: Jika JSON `kolom` ada titik (`author.nama`), gunakan alias tabel (`t2.nama`). Tanpa titik (`id_penelitian`), pakai `t1` (`t1.id_penelitian`).
# 5.  **SELECT**:
#     * Sertakan semua kolom yang diminta.
#     * Agregasi: `FUNGSI(alias.kolom) AS alias_agregasi`.
#     * **Penting**: Semua kolom non-agregasi di `SELECT` **harus juga ada** di `GROUP BY`. Jika tidak diagregasi/di `GROUP BY`, JANGAN sertakan di `SELECT` (kecuali tanpa `GROUP BY`).
# 6.  **JOIN**: Implementasikan semua `join`. Default `INNER JOIN`.
# 7.  **WHERE**: `LOWER(alias.kolom) LIKE LOWER('%nilai%')` untuk `ILIKE`. Nilai sesuai tipe data. Gabung filter dengan `AND`/`OR`. Fungsi SQL (`EXTRACT`, `DATE_SUB`) langsung. `IN` format: `alias.kolom IN (nilai1, nilai2)`.
# 8.  **GROUP BY, HAVING, ORDER BY, LIMIT**: Implementasikan jika ada di JSON.

# Hasilkan HANYA kode SQL murni dalam blok ```sql```.
# """
    
#     messages = [
#         {"role": "system", "content": system_prompt},
#         {"role": "user", "content": f"JSON Map:\n{json_map_str}"}
#     ]
    
#     response_sql = call_gemini_api(messages, temperature=0.0)
#     match = re.search(r"```sql\s*([\s\S]+?)\s*```|([\s\S]+)", response_sql, re.IGNORECASE)
#     cleaned_sql = (match.group(1) or match.group(2)).strip() if match else response_sql.strip()
    
#     logger.info(f"SQL yang dihasilkan:\n{cleaned_sql}")
    
#     return cleaned_sql

# --- Fungsi Jawaban dari Dokumen (RAG - DIPERBAIKI) ---
def answer_from_documents(question: str, context_chunks: str) -> str:
    """
    Menjawab pertanyaan analisis berdasarkan konteks dokumen (RAG).
    Prompt ditingkatkan untuk kemampuan analisis, sintesis, dan peringkasan.
    """
    system_prompt = (
        "Analisis & jawab pertanyaan berdasarkan 'Konteks Dokumen'. Berikan jawaban informatif, ringkas, formal.\n\n"
        "**Panduan Jawaban:**\n"
        "- **Sintesis**: Gabung informasi tersebar secara logis.\n"
        "- **Detail**: Penjelasan cukup detail, hindari info tak relevan.\n"
        "- **Inferensi**: Lakukan inferensi masuk akal didukung teks.\n"
        "- **Tanpa Halusinasi**: JANGAN tambah info di luar konteks.\n"
        "- **Ketidaktersediaan**: Jika tak ada di konteks, jawab: 'Maaf, informasi tidak tersedia dalam dokumen yang diberikan.'\n"
        "- **Analisis Data**: Jika konteks hasil query (JSON), analisis data (tren, perbandingan, nilai tertinggi/terendah), sajikan insight naratif, bukan data mentah.\n\n"
        "Contoh (hasil SQL): \n"
        "Konteks: `[{\"tahun\": 2022, \"jumlah_publikasi\": 150}, {\"tahun\": 2023, \"jumlah_publikasi\": 180}]`\n"
        "Pertanyaan: Bandingkan jumlah publikasi di bidang Teknologi antara tahun 2022 dan 2023\n"
        "Jawaban: Jumlah publikasi di bidang Teknologi meningkat dari 150 (2022) menjadi 180 (2023), naik 30 publikasi atau 20%.\n"
    )
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Konteks Dokumen:\n---\n{context_chunks}\n---\n\nPertanyaan: {question}\n\nJawaban:"}
    ]
    return call_gemini_api(messages, temperature=0.3)