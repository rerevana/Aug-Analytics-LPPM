import fitz  # PyMuPDF
import requests
import logging
import re
import json
from typing import Optional
from ocr import ocr_pdf_from_bytes # Impor fungsi OCR yang baru kita buat

logger = logging.getLogger(__name__)


MIN_TEXT_LENGTH_FOR_NON_OCR = 100

def find_pdf_url_in_results(results_json: str) -> list[str]:
    """Mencari semua URL PDF dalam hasil query JSON."""
    pdf_urls = []
    try:
        data = json.loads(results_json)
        for row in data:
            for value in row.values():
                if isinstance(value, str) and value.lower().endswith('.pdf'):
                    pdf_urls.append(value)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"Gagal mem-parse JSON untuk mencari URL: {e}")
    return pdf_urls # This now returns a list

def extract_text_from_pdf_url(pdf_url: str) -> Optional[str]:
    """
    Mengekstrak teks dari PDF di sebuah URL.
    Mencoba ekstraksi teks langsung (cepat), jika gagal atau hasilnya minim,
    maka beralih ke OCR untuk PDF berbasis gambar.
    """
    logger.info(f"Mengunduh PDF dari {pdf_url}...")
    try:
        response = requests.get(pdf_url, timeout=30)
        response.raise_for_status()
        pdf_bytes = response.content
    except requests.exceptions.RequestException as e:
        logger.error(f"Gagal mengunduh PDF dari {pdf_url}: {e}")
        return None

    # Langkah 1: Coba ekstraksi teks standar (cepat)
    text = ""
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for page in doc:
            text += page.get_text()

    # Langkah 2: Jika teks minim, gunakan OCR sebagai fallback
    if len(text.strip()) < MIN_TEXT_LENGTH_FOR_NON_OCR:
        logger.warning(f"Ekstraksi teks standar hanya menghasilkan {len(text.strip())} karakter. Beralih ke mode OCR.")
        return ocr_pdf_from_bytes(pdf_bytes)
    
    logger.info("Ekstraksi teks standar berhasil.")
    return text