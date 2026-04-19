import os
import pdfplumber
from typing import Optional, List, Union
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd

class PDExtractor:
    def __init__(self, log_file="extractor_errors.log"):
        self.log_file = log_file
        self.dispatch_map = {
            '.pdf': self._extract_from_pdf,
            '.txt': self._extract_from_txt,
            '.html': self._extract_from_html,
            '.parquet': self._extract_from_parquet,
            '.csv': self._extract_from_csv,
            #'.png': self._extract_from_image,
            #'.jpg': self._extract_from_image,
            #'.jpeg': self._extract_from_image,
        }

    def extract(self, file_path: str) -> str:
        """Метод для извлечения текста"""
        ext = os.path.splitext(file_path)[1].lower()
        handler = self.dispatch_map.get(ext)
        if handler:
            return handler(file_path)
        else:
            return ""

    def _write_log(self, message: str):
        """Запись ошибки в локальный файл"""
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(message + "\n")
        except:
            pass

    def _extract_from_csv(self, file_path: str) -> str:
        import pandas as pd
        try:
            df = pd.read_csv(file_path, nrows=50)
            return df.to_string()
        except:
            return ""

    def _extract_from_parquet(self, file_path: str) -> str:
        import pandas as pd
        try:
            df = pd.read_parquet(file_path)[:50]
            return df.to_string()
        except:
            return ""

    def _extract_from_pdf(self, file_path: str) -> str:
        pages_text = []
        try:
            with pdfplumber.open(file_path) as pdf:
                for i, page in enumerate(pdf.pages[:30]):
                    try:
                        content = page.extract_text()
                        if content:
                            pages_text.append(content)
                    except Exception as e:
                        print(f"DEBUG: Ошибка на стр {i} в {file_path}")
                        continue 
        except Exception as e:
            return "" 
        return "\n".join(pages_text)

    def _extract_from_txt(self, file_path: str):
        """Чтение TXT"""
        encodings = ['utf-8', 'cp1251', 'latin-1', 'cp866']
        for enc in encodings:
            try:
                with open(file_path, 'r', encoding=enc) as f:
                    return f.read()
            except (UnicodeDecodeError, Exception):
                continue
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        except Exception as e:
            self._write_log(f"TXT Error [{file_path}]: {str(e)}")
            return ""

    def _extract_from_html(self, file_path: str) -> str:
        with open(file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()
        soup = BeautifulSoup(html_content, 'html.parser')
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        text = '\n'.join(line for line in lines if line)
        return text
