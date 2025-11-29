from pathlib import Path
from typing import Tuple

def get_or_create_doc(doc_path: str) -> Tuple[object, Path]:
    """Return a python-docx Document and its path. Adapter to decouple docx from core utils."""
    from docx import Document
    p = Path(doc_path)
    doc = Document(p) if p.exists() else Document()
    return doc, p
