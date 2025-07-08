import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import io
import logging

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

logger = logging.getLogger(__name__)

def ocr_pdf_from_bytes(pdf_bytes: bytes) -> str:
    """
    Melakukan OCR pada file PDF yang berbasis gambar (scanned) dari byte stream.

    Args:
        pdf_bytes: Konten file PDF dalam bentuk bytes.

    Returns:
        Teks yang diekstrak dari semua halaman PDF.
    """
    full_text = ""
    try:
        pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
        logger.info(f"Memproses {len(pdf_document)} halaman PDF untuk OCR...")

        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            pix = page.get_pixmap(dpi=300) # Resolusi tinggi untuk akurasi OCR yang lebih baik
            
            img_bytes = pix.tobytes("png")
            image = Image.open(io.BytesIO(img_bytes))
            
            try:
                # 'ind+eng' akan mencoba mengenali bahasa Indonesia dan Inggris
                page_text = pytesseract.image_to_string(image, lang='ind+eng')
                full_text += page_text + "\n\n"
                logger.info(f"Berhasil melakukan OCR pada halaman {page_num + 1}/{len(pdf_document)}")
            except pytesseract.TesseractNotFoundError:
                logger.error("Tesseract tidak ditemukan. Pastikan Tesseract-OCR sudah terinstal dan path-nya dikonfigurasi dengan benar jika perlu.")
                return "Error: Tesseract tidak ditemukan. Silakan periksa instalasi."
            except Exception as e:
                logger.error(f"Gagal melakukan OCR pada halaman {page_num + 1}: {e}")
                continue
        return full_text.strip()
    except Exception as e:
        logger.error(f"Gagal memproses file PDF untuk OCR: {e}", exc_info=True)
        return f"Error: Gagal memproses PDF. {e}"