# llm.py

import google.generativeai as genai
import json
import re
import logging
from config import GOOGLE_API_KEY

# Konfigurasi API Google
genai.configure(api_key=GOOGLE_API_KEY)
logger = logging.getLogger(__name__)

def call_gemini_api(messages: list, model_name: str = "gemini-1.5-flash", temperature: float = 0.1, is_json_output: bool = False):
    """Fungsi terpusat untuk memanggil Google Gemini API."""
    model = genai.GenerativeModel(model_name)
    
    # Gabungkan pesan sistem dan pengguna menjadi satu prompt
    prompt = "\n".join([msg['content'] for msg in messages])
    
    generation_config = genai.types.GenerationConfig(
        temperature=temperature,
        response_mime_type="application/json" if is_json_output else "text/plain"
    )

    try:
        response = model.generate_content(prompt, generation_config=generation_config)
        return response.text
    except Exception as e:
        logger.error(f"Gagal memanggil Gemini API: {e}", exc_info=True)
        return json.dumps({"error": str(e)}) if is_json_output else f"Error: {str(e)}"

def classify_user_intent(user_query: str) -> dict:
    """Mengklasifikasikan intent pengguna: 'metadata_query' atau 'content_query'."""
    system_prompt = (
        "Anda adalah asisten AI yang bertugas mengklasifikasikan pertanyaan pengguna. "
        "Tentukan apakah pertanyaan tersebut dapat dijawab dengan melihat metadata tabel database (seperti jumlah, daftar, tanggal) "
        "atau memerlukan pembacaan isi/konten sebuah dokumen (seperti ringkasan, penjelasan, metodologi, kesimpulan).\n\n"
        "Jawab HANYA dengan JSON yang berisi kunci 'intent' dengan salah satu dari dua nilai ini:\n"
        "1. 'metadata_query': Untuk pertanyaan tentang data terstruktur (siapa, kapan, berapa banyak, daftar).\n"
        "   Contoh: 'siapa penulis penelitian terbaru?', 'berapa banyak publikasi di tahun 2023?'.\n"
        "2. 'content_query': Untuk pertanyaan yang membutuhkan pemahaman isi dokumen (apa, jelaskan, ringkas, bagaimana).\n"
        "   Contoh: 'jelaskan kesimpulan dari penelitian X', 'apa metodologi yang dipakai di makalah Y?'.\n\n"
        "Contoh output: {\"intent\": \"metadata_query\"}"
    )
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Pertanyaan pengguna: {user_query}"}
    ]
    
    response_text = call_gemini_api(messages, is_json_output=True, temperature=0.0)
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        return {"error": "Gagal mem-parse intent", "raw_response": response_text}

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
        "}\n\n"
        "Aturan Penting:\n"
        "- Output HARUS berupa JSON valid tanpa komentar atau teks tambahan.\n"
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
"""
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"JSON Map:\n{json_map_str}"}
    ]
    
    response_sql = call_gemini_api(messages, temperature=0.0)
    # Membersihkan output dari markdown
    match = re.search(r"```sql\s*([\s\S]+?)\s*```|([\s\S]+)", response_sql, re.IGNORECASE)
    return (match.group(1) or match.group(2)).strip() if match else response_sql.strip()

def answer_from_documents(question: str, context_chunks: str) -> str:
    """Menjawab pertanyaan berdasarkan konteks dokumen (RAG)."""
    system_prompt = (
        "Anda adalah asisten AI yang menjawab pertanyaan HANYA berdasarkan konteks dari dokumen yang diberikan. "
        "Jawab dengan ringkas dan jelas dalam Bahasa Indonesia. "
        "Jika jawaban tidak ada di dalam konteks, katakan: 'Maaf, saya tidak dapat menemukan jawaban di dalam dokumen yang tersedia.'"
    )
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Konteks Dokumen:\n---\n{context_chunks}\n---\n\nPertanyaan: {question}\n\nJawaban:"}
    ]
    return call_gemini_api(messages, temperature=0.2)