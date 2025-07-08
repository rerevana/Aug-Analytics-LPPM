from llm import (
    generate_json_map_from_schema_and_query,
    generate_sql_from_json_map,
    answer_from_documents
)
from config import BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID
from utils.bigquery_utils import (
    get_actual_tables,
    get_table_schemas,
    execute_query
)
from utils.document_utils import (
    find_pdf_url_in_results,
    extract_text_from_pdf_url
)
from utils.logging_config import setup_logging
import logging
import json

# Setup logging untuk aplikasi
setup_logging()
logger = logging.getLogger(__name__)

def _generate_sql_from_user_input(user_input: str) -> str | None:
    """Fungsi helper untuk mengambil skema, membuat map, dan menghasilkan SQL."""
    actual_tables = get_actual_tables()
    if not actual_tables:
        return None

    table_schemas = get_table_schemas(actual_tables)
    if not table_schemas:
        return None
    logger.info(f"Menggunakan skema dari tabel: {list(table_schemas.keys())}")

    logger.info("STEP 1: Membuat JSON Map...")
    json_map = generate_json_map_from_schema_and_query(user_input, table_schemas)
    if json_map.get("error"):
        logger.error(f"Gagal membuat JSON map: {json_map}")
        return None
    logger.info(f"Hasil JSON Map: {json.dumps(json_map, indent=2)}")

    logger.info("STEP 2: Membuat SQL Query...")
    sql_query = generate_sql_from_json_map(json_map, BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID)
    if "error" in sql_query.lower():
        logger.error(f"Gagal membuat SQL query: {sql_query}")
        return None
    logger.info(f"Hasil SQL Query: {sql_query}")
    return sql_query

def unified_workflow(user_input: str):
    """Alur kerja terpadu yang menangani Text-to-SQL dan RAG secara dinamis."""
    logger.info("--- Alur Kerja Terpadu Dimulai ---")
    
    # Langkah 1 & 2: Selalu coba hasilkan dan jalankan SQL
    sql_query = _generate_sql_from_user_input(user_input)
    if not sql_query:
        print("\n--- Jawaban Akhir ---\nMaaf, saya tidak dapat membuat query SQL untuk pertanyaan tersebut.")
        return
    
    logger.info("STEP 3: Eksekusi Query...")
    query_result = execute_query(sql_query)

    # Langkah 4: Periksa hasil query untuk URL dokumen (modifikasi di sini)
    document_urls = find_pdf_url_in_results(query_result)

    # Langkah 5: Tentukan alur selanjutnya secara dinamis
    if document_urls:
        # Alur RAG (Analisis Konten Dokumen) untuk banyak dokumen
        all_document_texts = []
        for doc_url in document_urls:
            logger.info(f"Dokumen ditemukan: {doc_url}")
            logger.info(f"STEP 4: Mengekstrak teks dari dokumen: {doc_url}...")
            doc_text = extract_text_from_pdf_url(doc_url)
            if doc_text:
                all_document_texts.append(doc_text)
            else:
                logger.warning(f"Gagal mengekstrak teks dari dokumen: {doc_url}. Melanjutkan ke dokumen berikutnya jika ada.")

        if not all_document_texts:
            logger.error("Tidak ada teks yang berhasil diekstrak dari dokumen manapun.")
            print("\n--- Jawaban Akhir ---\nGagal mengekstrak konten dari dokumen. Silakan periksa URL atau format file.")
            return

        # Gabungkan semua teks dari dokumen yang berhasil diekstrak
        combined_document_text = "\n".join(all_document_texts)
        logger.info("Ekstraksi teks dari semua dokumen berhasil.")
        logger.info("STEP 5: Menjawab pertanyaan dari konteks dokumen gabungan...")
        
        final_answer = answer_from_documents(user_input, combined_document_text)
        print("\n--- Jawaban Akhir ---\n" + final_answer)
    else:
        # Alur Metadata (Tampilkan hasil query langsung)
        logger.info("Tidak ada dokumen yang ditemukan. Menampilkan hasil query mentah.")
        print("\n--- Jawaban Akhir ---\n" + query_result)

def main():
    """Fungsi utama untuk menjalankan loop interaktif."""
    logger.info("Aplikasi dimulai. Selamat datang!")
    print("Selamat datang! Ajukan pertanyaan tentang data penelitian Anda. Ketik 'keluar' untuk berhenti.")

    while True:
        user_input = input("\nMasukkan pertanyaan Anda: ")
        if user_input.lower() in ['keluar', 'exit', 'quit']:
            break
        
        try:
            unified_workflow(user_input)
        except Exception as e:
            logger.exception(f"Terjadi error yang tidak terduga di loop utama: {e}")
            print("\nTerjadi error. Silakan coba lagi.")

if __name__ == "__main__":
    main()