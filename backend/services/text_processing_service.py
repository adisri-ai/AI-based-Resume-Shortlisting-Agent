import PyPDF2
import io
"""
reader     = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        pages_text = []
        for page in reader.pages:
            page_text = page.extract_text() or ""
            pages_text.append(page_text)
        full_text = "\n".join(pages_text)
        return full_text
"""
class TextProcessingService : 
    def extract_text_from_pdf(pdf_bytes: bytes) -> str:
        reader = PyPDF2.PdfReader(io.BytesIO(bytes))
        pages_text = []
        for page in reader.pages :
            page_text = page.extract_text() or ""
            pages_text.append(page_text)
        full_text = "\n".join(page_text)
        return full_text