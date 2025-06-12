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
        
        # Mencari blok JSON dalam respons menggunakan regex
        # Ini akan mencari konten di antara ```json ... ``` atau hanya { ... }
        match = re.search(r"```json\s*([\s\S]*?)\s*```|(\{[\s\S]*\})", response_content)
        if match:
            json_str = match.group(1) or match.group(2) # Ambil grup yang cocok
            return json.loads(json_str)
        else:
            # Jika tidak ada blok JSON yang jelas, coba parse langsung (mungkin LLM mengembalikan JSON murni)
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
                "Anda adalah AI yang ahli dalam merancang struktur query data. "
                "Berdasarkan permintaan pengguna dan skema tabel yang diberikan, buatlah representasi JSON nested map "
                "yang mendeskripsikan bagaimana data harus diambil dan diproses. "
                "JSON map ini harus mencakup elemen seperti: tabel yang digunakan, kolom yang dipilih, filter, join (jika perlu), dan agregasi. "
                "Pastikan outputnya HANYA JSON yang valid."
                f"\n\nSkema Tabel yang Tersedia:\n{schemas_str}"
            )
        },
        {"role": "user", "content": f"Permintaan pengguna: {user_query}"}
    ]
    try:
        response_content = call_openrouter(messages, model=model_name, max_tokens=1000, temperature=0.1)
        # Mencari blok JSON dalam respons menggunakan regex
        match = re.search(r"```json\s*([\s\S]*?)\s*```|(\{[\s\S]*\})", response_content)
        if match:
            json_str = match.group(1) or match.group(2) # Ambil grup yang cocok
            return json.loads(json_str)
        else:
            # Jika tidak ada blok JSON yang jelas, coba parse langsung
            # (mungkin LLM mengembalikan JSON murni atau error lain)
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
        f"PENTING: Semua referensi tabel dalam query SQL HARUS memenuhi syarat lengkap dengan project ID dan dataset ID. "
        f"Gunakan format backtick (`) untuk mengapit nama project, dataset, dan tabel: `{project_id}`.`{dataset_id}`.`nama_tabel_dari_json_map`. "
        f"Contoh yang BENAR: `{project_id}.{dataset_id}.users`. "
        f"JANGAN menuliskan `{project_id}.{dataset_id}.users` tanpa backtick jika ada karakter khusus atau merupakan reserved keyword. Selalu gunakan backtick untuk keamanan."
    )
    messages = [
        {
            "role": "system",
            "content": (
                f"Anda adalah AI yang ahli dalam menerjemahkan struktur data JSON menjadi query SQL. "
                f"{table_qualification_instruction} "
                f"Berdasarkan JSON map berikut, buatlah query SQL yang valid untuk dialek {db_dialect}. "
                "Pastikan outputnya HANYA string SQL query yang valid, tanpa teks tambahan atau penjelasan."
            )
        },
        {"role": "user", "content": f"JSON Map:\n{json_map_str}"}
    ]
    try:
        # LLM diharapkan mengembalikan string SQL murni
        response_content = call_openrouter(messages, model=model_name, max_tokens=1000, temperature=0.0)
        # Membersihkan penanda blok kode Markdown jika ada
        match = re.search(r"```(?:sql\s*)?([\s\S]*?)\s*```", response_content, re.IGNORECASE)
        if match:
            cleaned_sql = match.group(1).strip()
            return cleaned_sql
        else:
            # Jika tidak ada blok markdown, kembalikan apa adanya (setelah strip)
            return response_content.strip()
    except Exception as e: # Termasuk potensi error dari call_openrouter jika tidak mengembalikan string
        return {"error": f"Error saat menghasilkan SQL dari JSON map: {str(e)}", "raw_response": ""}

# def generate_insights_with_llama(prompt: str, data_context: str = "", model_name: str = "meta-llama/llama-3.3-70b-instruct") -> str:
#     """
#     Menggunakan model Llama (via OpenRouter) untuk menghasilkan insight atau jawaban
#     berdasarkan prompt dan konteks data yang diberikan.
#     """
#     messages = [
#         {"role": "system", "content": "Anda adalah asisten analitik data yang cerdas."},
#         {"role": "user", "content": f"{prompt}\n\nData Kontekstual (jika ada):\n{data_context}"}
#     ]
#     try:
#         response_content = call_openrouter(messages, model=model_name, max_tokens=2000, temperature=0.5)
#         return response_content
#     except Exception as e:
#         return f"Error saat memanggil LLM: {str(e)}"