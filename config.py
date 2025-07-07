import os
from dotenv import load_dotenv

load_dotenv()

# Ganti dengan kunci API yang Anda dapatkan dari Google AI Studio
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# Konfigurasi BigQuery
BIGQUERY_PROJECT_ID = "swift-kiln-461800-u0"
BIGQUERY_DATASET_ID = "proposal_penelitian"

# Pastikan path ini benar atau gunakan metode autentikasi lain
SERVICE_ACCOUNT_KEY_PATH = os.getenv("SERVICE_ACCOUNT_KEY_PATH")