from openrouter_api import call_openrouter
import json
import re

def understand_user_query(user_query: str, model_name: str = "meta-llama/llama-3.3-70b-instruct") -> dict:
    """
    Menggunakan LLM untuk memahami query pengguna, mengekstrak intent dan entitas.
    Ini adalah langkah yang Anda sebut "tokenisasi menggunakan LLM".
    """
    messages = [
        {
            "role": "system",
            "content": (
                "Anda adalah asisten AI yang bertugas memproses permintaan analisis data. "
                "Fokus utama Anda adalah mengidentifikasi dan mengekstrak komponen-komponen kunci dari permintaan pengguna. "
                "Komponen ini termasuk: maksud utama (intent), entitas spesifik (seperti nama metrik, dimensi, filter, periode waktu), "
                "dan nama-nama tabel atau sumber data yang disebutkan atau diimplikasikan. "
                "Jawab dalam format JSON dengan kunci: 'intent', 'entities' (list), 'potential_tables' (list)."
            )
        },
        {"role": "user", "content": f"Permintaan pengguna: {user_query}"}
    ]

    try:
        response_content = call_openrouter(messages, model=model_name, max_tokens=500, temperature=0.2)
        match = re.search(r"```json\s*([\s\S]*?)\s*```|(\{[\s\S]*\})", response_content)
        if match:
            json_str = match.group(1) or match.group(2) # Ambil grup yang cocok
            return json.loads(json_str)
        else:
            return json.loads(response_content)
    except json.JSONDecodeError:
        return {"error": "Failed to parse LLM response as JSON", "raw_response": response_content}
    except Exception as e:
        return {"error": str(e), "raw_response": ""}

def generate_json_map_from_schema_and_query(user_query: str, table_schemas: dict, model_name: str = "meta-llama/llama-3.3-70b-instruct") -> dict:
    """
    Menggunakan LLM untuk membuat JSON nested map berdasarkan query pengguna dan skema tabel.
    JSON map ini bisa merepresentasikan struktur query yang lebih abstrak.
    """

    schemas_str = json.dumps(table_schemas, indent=2)

    messages = [
        {
            "role": "system",
            "content": (
                "Anda adalah AI yang menerjemahkan permintaan pengguna menjadi JSON yang menggambarkan struktur query SQL.\n\n"
                "Format JSON:\n"
                "{\n"
                '  "tabel": "nama_tabel_utama",\n'
                '  "kolom": ["kolom1", "kolom2"],\n'
                '  "join": [ { "tabel": "nama_tabel", "on": "tabel1.kolom = tabel2.kolom" } ],\n'
                '  "filter": [ { "kolom": "nama_kolom", "operator": "=", "nilai": 2023 } ],\n'
                '  "agregasi": [ { "fungsi": "GROUP BY", "kolom": "author.nama" } ],\n'
                '  "order_by": { "kolom": "tahun.tahun", "urutan": "DESC" }\n'
                "}\n\n"
                "Catatan penting:\n"
                "- Output hanya JSON valid. Tanpa markdown, komentar, atau penjelasan.\n"
                "- Gunakan kutip ganda untuk semua key dan value string.\n"
                "- Jika ada kolom foreign key seperti `author_id` atau `tahun_id`, WAJIB JOIN ke tabel referensinya.\n"
                "  Contoh: `luaran_penelitian.author_id = author.id`, lalu filter pakai `author.nama LIKE ...`\n"
                "- Jika permintaan terkait luaran (haki, publikasi, dll) dan menyebut tahun/author, JOIN via `luaran_penelitian` ke `tahun` atau `author`.\n"
                "- Jika mencari berdasarkan nama (misalnya nama penulis), gunakan filter dengan operator `LIKE`, bukan `=`. abaikan upercase dan lowercase nya\n"
                "- Jika diminta urutan (terbaru/abjad), tambahkan field `order_by`.\n"
                "- Jika tidak ada bagian tertentu (join/filter/agregasi/order_by), boleh kosong atau dihilangkan.\n\n"
                f"Tabel tersedia:\n{schemas_str}"
            )
        },
        {
            "role": "user",
            "content": f"Permintaan pengguna: {user_query}"
        }
    ]


    try:
        response_content = call_openrouter(
            messages,
            model=model_name,
            max_tokens=1000,
            temperature=0.1
        )

        match = re.search(r"```json\s*([\s\S]*?)```|(\{[\s\S]*\})", response_content)
        if match:
            json_str = match.group(1) or match.group(2)
            return json.loads(json_str)
        else:
            return json.loads(response_content)
    except json.JSONDecodeError:
        return {"error": "Failed to parse LLM response for JSON map as JSON", "raw_response": response_content}
    except Exception as e:
        return {"error": str(e), "raw_response": ""}


def match_query_to_actual_tables(user_query_understanding: dict, actual_table_list: list, dataset_context: str = "", model_name: str = "meta-llama/llama-3.3-70b-instruct") -> list:
    """
    Menggunakan LLM untuk memilih tabel/view yang paling relevan dari daftar `actual_table_list`
    berdasarkan pemahaman query pengguna (intent, entities) dan konteks dataset.
    """
    if not actual_table_list:
        print("WARNING: Tidak ada daftar tabel aktual yang diberikan ke LLM untuk pencocokan.")
        return []
    if not user_query_understanding or not user_query_understanding.get("intent"):
        print("WARNING: Pemahaman query pengguna tidak lengkap untuk pencocokan tabel oleh LLM.")
        return []

    prompt_content = (
        f"Permintaan pengguna:\n"
        f"- Intent: {user_query_understanding.get('intent')}\n"
        f"- Entitas: {user_query_understanding.get('entities', [])}\n\n"
        f"Tabel/view tersedia:\n{actual_table_list}\n"
        f"Konteks: {dataset_context}\n\n"
        "Aturan:\n"
        "- Jika menyebut `author` atau `tahun` dalam konteks `luaran` (publikasi, haki, dll), pilih tabel `luaran_*` lalu JOIN ke `luaran_penelitian`, "
        "baru ke `author`/`tahun` jika diperlukan.\n"
        "- Contoh: `luaran_publikasi → luaran_penelitian → author/tahun`\n"
        "- Jangan pilih `author` atau `tahun` langsung jika relasinya melalui `luaran`\n\n"
        "Tugas Anda: Pilih tabel/view paling relevan dari daftar di atas.\n"
        "Hasilkan **list JSON** berisi nama tabel yang dipilih. Contoh: [\"tabel_a\", \"tabel_b\"]"
    )

    messages = [
        {"role": "system", "content": "Anda adalah AI yang ahli dalam mencocokkan permintaan pengguna dengan tabel database yang relevan. Jawab dalam format list JSON."},
        {"role": "user", "content": prompt_content}
    ]
    try:
        response_str = call_openrouter(messages, model=model_name, max_tokens=300, temperature=0.05) # Suhu lebih rendah untuk konsistensi
        selected_tables = json.loads(response_str)
        if isinstance(selected_tables, list):
            return selected_tables
        return [] # Jika bukan list, kembalikan list kosong
    except Exception: # Termasuk JSONDecodeError atau error dari call_openrouter
        return [] # Kembalikan list kosong jika ada error
    
def generate_sql_from_json_map(json_map: dict, project_id: str, dataset_id: str, db_dialect: str = "BigQuery", model_name: str = "meta-llama/llama-3.3-70b-instruct") -> str:
    """
    Menggunakan LLM untuk mengubah JSON nested map menjadi query SQL.
    """

    json_map_str = json.dumps(json_map, indent=2)

    table_qualification_instruction = (
        f"Buat query SQL untuk BigQuery dengan aturan berikut:\n"
        "1. Jika hanya ingin tahu apakah data ada, hasilkan SQL yang mengembalikan boolean (TRUE/FALSE), gunakan:"
        "- SELECT EXISTS (SELECT 1 FROM tabel WHERE kondisi) AS hasil\n"
        "- atau: SELECT CASE WHEN COUNT(*) > 0 THEN TRUE ELSE FALSE END AS hasil\n"
        f"2. Format tabel lengkap: `{project_id}`.`{dataset_id}`.`nama_tabel` AS `alias`\n"
        f"3. Referensi kolom: gunakan `alias.kolom` atau `nama_tabel.kolom`\n"
        f"4. Semua nama (tabel, alias, kolom) wajib pakai backtick (`)\n"
        f"5. Jika ada agregasi: gunakan fungsi seperti COUNT, SUM, AVG sesuai JSON\n"
        f"6. Jika `distinct: true` → pakai SELECT DISTINCT\n"
        f"7. Jika agregasi menggunakan DISTINCT → SELECT DISTINCT `kolom`\n"
        f"8. Hasilkan hanya SQL murni, tanpa markdown, komentar, atau penjelasan\n"
    )


    messages = [
        {
            "role": "system",
            "content": (
                f"Anda adalah AI ahli SQL untuk {db_dialect}. "
                f"Ubah JSON map menjadi SQL dengan mengikuti instruksi berikut.\n\n"
                f"{table_qualification_instruction}"
            )
        },
        {
            "role": "user",
            "content": f"JSON Map:\n{json_map_str}"
        }
    ]

    try:
        # Memanggil LLM
        response_content = call_openrouter(
            messages,
            model=model_name,
            max_tokens=1000,
            temperature=0.0
        )

        # DEBUG
        print("LLM Raw Response:", repr(response_content))

        # Ekstrak SQL tanpa format markdown
        match = re.search(r"```sql\s*([\s\S]+?)\s*```|([\s\S]+)", response_content, re.IGNORECASE)
        if match:
            cleaned_sql = (match.group(1) or match.group(2)).strip()
            return cleaned_sql
        else:
            # Jika tidak ada blok markdown, kembalikan apa adanya (setelah strip)
            return response_content.strip()
    except Exception as e: # Termasuk potensi error dari call_openrouter jika tidak mengembalikan string
        return {"error": f"Error saat menghasilkan SQL dari JSON map: {str(e)}", "raw_response": ""}
