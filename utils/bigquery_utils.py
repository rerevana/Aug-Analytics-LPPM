import json
import logging
from google.cloud import bigquery
from config import BIGQUERY_PROJECT_ID, BIGQUERY_DATASET_ID, SERVICE_ACCOUNT_KEY_PATH

logger = logging.getLogger(__name__)

# Inisialisasi client sekali saja untuk efisiensi
try:
    BQ_CLIENT = bigquery.Client.from_service_account_json(
        SERVICE_ACCOUNT_KEY_PATH, project=BIGQUERY_PROJECT_ID
    )
except Exception as e:
    logger.critical(f"Gagal menginisialisasi BigQuery Client: {e}", exc_info=True)
    BQ_CLIENT = None

def get_actual_tables() -> list:
    """Mengambil daftar nama tabel aktual dari dataset BigQuery."""
    if not BQ_CLIENT: return []
    try:
        tables_iterator = BQ_CLIENT.list_tables(f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}")
        return [table.table_id for table in tables_iterator]
    except Exception as e:
        logger.error(f"Gagal mendapatkan daftar tabel dari BigQuery: {e}", exc_info=True)
        return []

def get_table_schemas(table_names: list) -> dict:
    """Mengambil skema dari tabel yang ditentukan di BigQuery."""
    if not BQ_CLIENT or not table_names: return {}
    schemas = {}
    for table_name in table_names:
        try:
            table_ref = BQ_CLIENT.dataset(BIGQUERY_DATASET_ID).table(table_name)
            table = BQ_CLIENT.get_table(table_ref)
            schemas[table.table_id] = [{"name": field.name, "type": field.field_type} for field in table.schema]
        except Exception as e:
            logger.error(f"Gagal mengambil skema untuk tabel {table_name}: {e}", exc_info=True)
    return schemas

def execute_query(sql_query: str) -> str:
    """Mengeksekusi query SQL di BigQuery dan mengembalikan hasil sebagai string JSON."""
    if not BQ_CLIENT or not sql_query: return "[]"
    try:
        query_job = BQ_CLIENT.query(sql_query)
        results = [dict(row) for row in query_job.result()]
        return json.dumps(results, indent=2, default=str)
    except Exception as e:
        logger.error(f"Gagal mengeksekusi query: {sql_query} - {e}", exc_info=True)
        return json.dumps([{"error": str(e)}])