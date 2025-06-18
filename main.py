from llm import (
    understand_user_query,
    generate_json_map_from_schema_and_query, # Fungsi baru
    generate_sql_from_json_map,             # Fungsi baru
    match_query_to_actual_tables # Fungsi baru dari llm.py
)

# Import konfigurasi jika diperlukan di main, meskipun openrouter_api sudah mengambilnya
from config import API_KEY, PROJECT_TITLE, REFERER
from google.cloud import bigquery # Import BigQuery
import json
import requests # Untuk interaksi API Superset

BIGQUERY_PROJECT_ID = "swift-kiln-461800-u0"
BIGQUERY_DATASET_ID = "proposal_penelitian" # nama dataset

def get_actual_tables_from_bigquery() -> list:
    """
    Mengambil daftar nama tabel/view aktual dari dataset BigQuery yang dikonfigurasi.
    """
    actual_tables = []
    try:
        service_account_json_path_bq = r"D:\Riset\keys.json"
        client = bigquery.Client.from_service_account_json(service_account_json_path_bq, project=BIGQUERY_PROJECT_ID)
        tables_iterator = client.list_tables(f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}")
        for table_item in tables_iterator:
            actual_tables.append(table_item.table_id)
        print(f"INFO: Daftar tabel/view aktual di dataset '{BIGQUERY_DATASET_ID}': {actual_tables}")
    except Exception as e:
        print(f"ERROR: Gagal mendapatkan daftar tabel dari BigQuery untuk dataset '{BIGQUERY_DATASET_ID}': {e}")
    return actual_tables

def select_relevant_bigquery_tables(user_query_understanding: dict, actual_tables_in_db: list) -> list:
    """
    Menggunakan LLM (via llm.py) untuk memilih tabel/view yang paling relevan dari daftar tabel aktual di DB
    berdasarkan pemahaman query pengguna.
    """
    if not actual_tables_in_db:
        print("INFO: Tidak ada daftar tabel aktual dari DB untuk diseleksi oleh LLM.")
        return []
    
    print(f"INFO: Meminta LLM untuk memilih tabel relevan dari daftar aktual: {actual_tables_in_db} berdasarkan pemahaman query.")
    dataset_description = f"Dataset '{BIGQUERY_DATASET_ID}' dalam proyek '{BIGQUERY_PROJECT_ID}' berisi data terkait penelitian dan kinerja dosen." # Contoh deskripsi
    
    selected_tables_by_llm = match_query_to_actual_tables(user_query_understanding, actual_tables_in_db, dataset_context=dataset_description)
            
    print(f"INFO: Tabel yang dipilih oleh LLM sebagai relevan: {selected_tables_by_llm}")
    return selected_tables_by_llm

def get_table_schemas_from_bigquery(table_names: list) -> dict:
    """
    Mengambil skema (struktur kolom) dari tabel/view yang ditentukan di BigQuery.
    """
    if not table_names:
        return {}
    service_account_json_path_bq = r"D:\Riset\keys.json" # Menggunakan raw string untuk path Windows
    client = bigquery.Client.from_service_account_json(service_account_json_path_bq, project=BIGQUERY_PROJECT_ID)
    schemas = {}
    for table_name in table_names:
        try:
            # Asumsikan table_name bisa jadi belum qualified, atau sudah qualified
            if '.' not in table_name:
                table_ref = client.dataset(BIGQUERY_DATASET_ID).table(table_name)
            else: # Jika sudah ada project.dataset.table
                table_ref = client.get_table(table_name).reference
            
            table = client.get_table(table_ref)
            schemas[table.table_id] = [{"name": field.name, "type": field.field_type} for field in table.schema]
            print(f"INFO: Skema untuk tabel {table.table_id} berhasil diambil.")
        except Exception as e:
            print(f"ERROR: Gagal mengambil skema untuk tabel {table_name}: {e}")
    return schemas

def execute_query_on_bigquery(sql_query: str) -> str:
    """
    Mengeksekusi query SQL yang diberikan di Google BigQuery dan mengembalikan hasilnya sebagai string JSON.
    """
    if not sql_query:
        return "Tidak ada query SQL untuk dieksekusi."
    print(f"INFO: Mengeksekusi query di BigQuery: {sql_query}")
    try:
        service_account_json_path_bq = r"D:\Riset\keys.json" # Menggunakan raw string
        client = bigquery.Client.from_service_account_json(service_account_json_path_bq, project=BIGQUERY_PROJECT_ID)
        query_job = client.query(sql_query)
        results = query_job.result()
        data_list = [dict(row) for row in results]
        return json.dumps(data_list, indent=2, default=str) # default=str untuk menangani tipe data seperti tanggal
    except Exception as e:
        print(f"ERROR: Gagal mengeksekusi query di BigQuery: {e}")
        return f'{{"error": "Gagal mengeksekusi query di BigQuery: {str(e)}"}}'

def test_generated_sql_query(sql_query: str, limit: int = 5):
    """
    Fungsi untuk menguji query SQL yang dihasilkan dengan menjalankannya di BigQuery
    dan mencetak beberapa baris hasil.
    """
    if not sql_query or "SELECT" not in sql_query.upper():
        print("TEST_QUERY: Query SQL tidak valid atau kosong.")
        return
    if "LIMIT" in sql_query.upper():
        test_query = sql_query
    else:
        test_query = f"{sql_query} LIMIT {limit}" # Tambahkan LIMIT 
    print(f"\n--- Menguji Query SQL di BigQuery ---")
    print(f"Test Query: {test_query}")

    query_result_json_str = execute_query_on_bigquery(test_query)
    
    if '"error":' in query_result_json_str:
        print(f"\nHasil Test Query (JSON dengan Error): {query_result_json_str}")
    else:
        try:
            query_result_list = json.loads(query_result_json_str)
            if isinstance(query_result_list, list) and not query_result_list:
                print("\nHasil Test Query: Tidak ada data yang dikembalikan oleh query.")
            else:
                print(f"\nHasil Test Query (JSON): {query_result_json_str}")
        except json.JSONDecodeError:
            # Jika bukan JSON valid dan tidak mengandung "error":
            print(f"Hasil Test Query (Format tidak dikenali): {query_result_json_str}")

def augmented_analytics_workflow():
    print("Selamat datang di Sistem Augmented Analytics!")
    user_input = input("Masukkan pertanyaan atau permintaan analisis Anda: ")

    # 1. Pemahaman Query Pengguna (Tokenisasi/NLU dengan LLM)
    print("\n--- Menganalisis Permintaan Pengguna ---")
    parsed_query = understand_user_query(user_input)
    print(f"Hasil Pemahaman Query: {parsed_query}")

    if parsed_query.get("error"):
        print(f"Error saat memahami query: {parsed_query['error']}")
        return

    # 2. Dapatkan daftar tabel aktual dari BigQuery
    print("\n--- Mengambil Daftar Tabel Aktual dari BigQuery ---")
    actual_bq_tables = get_actual_tables_from_bigquery()
    if not actual_bq_tables:
        print("Tidak dapat melanjutkan karena gagal mengambil daftar tabel aktual dari BigQuery.")
        return

    # 2.1. View Selection menggunakan LLM berdasarkan pemahaman query dan daftar tabel aktual
    relevant_tables = select_relevant_bigquery_tables(parsed_query, actual_bq_tables)
    if not relevant_tables:
        print("Tidak ada tabel relevan yang dapat diproses lebih lanjut.")
        return
    print(f"Tabel yang relevan: {relevant_tables}")

    # 3. Pengambilan Skema Tabel/View
    print("\n--- Mengambil Skema Tabel/View dari BigQuery ---")
    table_schemas = get_table_schemas_from_bigquery(relevant_tables)
    if not table_schemas:
        print("Tidak dapat mengambil skema untuk tabel yang relevan.")
        return
    print(f"Skema Tabel yang Diperoleh: {json.dumps(table_schemas, indent=2)}")

    # 4. Mengubah Struktur Data menjadi JSON Nested Map dengan LLM
    print("\n--- Membuat JSON Nested Map dengan LLM ---")
    json_nested_map = generate_json_map_from_schema_and_query(user_input, table_schemas)
    if isinstance(json_nested_map, dict) and json_nested_map.get("error"):
        print(f"Error saat membuat JSON map: {json_nested_map.get('raw_response', json_nested_map.get('error'))}")
        return
    print(f"JSON Nested Map yang Dihasilkan: {json.dumps(json_nested_map, indent=2)}")

    # 5. Mengubah JSON Map menjadi SQL Query
    print("\n--- Membuat SQL Query dari JSON Map dengan LLM ---")
    sql_query = generate_sql_from_json_map(json_nested_map, BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID)
    if isinstance(sql_query, dict) and sql_query.get("error"): # Jika LLM mengembalikan error
         print(f"Error saat membuat SQL query: {sql_query.get('raw_response', sql_query.get('error'))}")
         return
    if not sql_query or "SELECT" not in sql_query.upper(): # Pemeriksaan dasar
        print(f"Gagal menghasilkan SQL query yang valid. Output LLM: {sql_query}")
        return
    print(f"SQL Query yang Dihasilkan: {sql_query}")

    # Panggil fungsi tes di sini jika ingin selalu menguji

    test_generated_sql_query(sql_query)


if __name__ == "__main__":
    augmented_analytics_workflow()