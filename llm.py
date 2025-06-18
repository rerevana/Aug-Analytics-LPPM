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

    # PROMPT UTAMA UNTUK LLM
    messages = [
        {
            "role": "system",
            "content": (
                "Anda adalah AI yang ahli dalam menerjemahkan permintaan pengguna menjadi struktur data JSON "
                "yang menggambarkan bagaimana query SQL harus dibentuk.\n\n"
                "Tugas Anda adalah mengubah permintaan pengguna menjadi JSON nested map dengan format sebagai berikut:\n\n"
                "{\n"
                '  "tabel": "nama_tabel_utama",\n'
                '  "kolom": ["kolom1", "kolom2"],\n'
                '  "join": [\n'
                '    { "tabel": "nama_tabel_lain", "on": "tabel1.kolom = tabel2.kolom" }\n'
                '  ],\n'
                '  "filter": [\n'
                '    { "kolom": "nama_kolom", "operator": "=", "nilai": 2023 }\n'
                '  ],\n'
                '  "agregasi": [\n'
                '    { "fungsi": "GROUP BY", "kolom": "author.nama" }\n'
                '  ]\n'
                '}\n\n'
                "⚠️ PERHATIKAN:\n"
                "- Output HANYA berupa JSON VALID. Tidak ada komentar, tidak ada markdown, tidak ada penjelasan.\n"
                "- Gunakan tanda kutip GANDA untuk semua key dan string value.\n"
                "JOIN RELASI:\n"
                "- Jika ada kolom foreign key seperti `tahun_id`, maka WAJIB melakukan JOIN ke tabel `tahun`\n"
                "- menggunakan `penelitian.tahun_id = tahun.id`, lalu filter nilai tahun pakai `tahun.tahun = XXXX`\n\n"
                "- Jika ada kolom foreign key seperti `author_id`, maka WAJIB melakukan JOIN ke tabel `author`\n"
                "- menggunakan `penelitian.author_id = author.id`, lalu untuk mencari nama pakai `author.nama = XXXX`\n\n"
                "(misal: author_id), maka Anda HARUS melakukan JOIN ke tabel referensi "
                "dan memilih nama atau detail lainnya.\n"
                "- Jika field 'agregasi' berisi DISTINCT, gunakan agregasi DISTINCT.\n"
                "- Jika tidak ada bagian tertentu (join, filter, agregasi), bisa dihilangkan atau kosongkan.\n\n"
                f"Berikut ini adalah skema tabel yang tersedia:\n\n{schemas_str}"
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
        f"Pemahaman permintaan pengguna:\nIntent: {user_query_understanding.get('intent')}\nEntitas: {user_query_understanding.get('entities', [])}\n\n"
        f"Daftar tabel/view yang tersedia di database adalah: {actual_table_list}\n"
        f"Konteks tambahan (jika ada): {dataset_context}\n"
        "Berdasarkan pemahaman permintaan pengguna di atas, pilihlah dan kembalikan HANYA nama-nama tabel/view dari daftar tabel yang tersedia yang paling relevan. "
        "Kembalikan hasilnya sebagai list JSON dari string nama tabel. Contoh: [\"tabel_a\", \"tabel_b\"]"
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
        f"Anda akan membuat query SQL untuk BigQuery. Ikuti aturan berikut:\n\n"
        f"1. KUALIFIKASI TABEL (FROM dan JOIN):\n"
        f"   - Format lengkap: `{project_id}`.`{dataset_id}`.`nama_tabel`\n"
        f"   - Tambahkan alias setelahnya. Contoh: FROM `{project_id}`.`{dataset_id}`.`penelitian` AS `p`\n\n"
        f"2. KUALIFIKASI KOLOM:\n"
        f"   - Jika alias digunakan, referensikan kolom sebagai `alias.kolom` (misal `p.nama`)\n"
        f"   - Jika tanpa alias, gunakan `nama_tabel.kolom`\n\n"
        f"3. SEMUA nama project, dataset, tabel, alias, dan kolom harus menggunakan backtick (`)\n\n"
        f"4. Jika JSON memiliki field 'agregasi', gunakan fungsi agregasi yang sesuai (COUNT, SUM, AVG, dll).\n"
        f"   - Contoh: SELECT COUNT(`p`.`id`) ...\n"
        f"   - Jika ada join dan agregasi, gunakan alias kolom secara benar.\n\n"
        f"5. Jika ada field 'distinct: true' dalam JSON, gunakan SELECT DISTINCT.\n"
        f"6. Hasilkan query SQL murni saja. Jangan sertakan penjelasan atau format markdown.\n"
        f"7. Jika field 'agregasi' berisi DISTINCT, gunakan SELECT DISTINCT `kolom`.\n"
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
