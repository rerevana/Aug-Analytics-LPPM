import json
import requests
import pdfplumber
import io
import logging

logger = logging.getLogger(__name__)

def find_pdf_url_in_results(results_json: str) -> str or None:
    """Mencari URL PDF pertama dalam hasil query JSON."""
    try:
        data = json.loads(results_json)
        for row in data:
            for value in row.values():
                if isinstance(value, str) and value.lower().endswith('.pdf'):
                    return value
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"Gagal mem-parse JSON untuk mencari URL: {e}")
        return None
    return None

def extract_text_from_pdf_url(pdf_url: str) -> str:
    """Mengunduh dan mengekstrak teks dari URL PDF."""
    try:
        response = requests.get(pdf_url, timeout=30)
        response.raise_for_status()
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            return "".join(page.extract_text() for page in pdf.pages if page.extract_text())
    except Exception as e:
        logger.error(f"Gagal memproses PDF dari URL {pdf_url}: {e}", exc_info=True)
        return ""