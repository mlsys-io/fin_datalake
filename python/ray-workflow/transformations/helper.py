import re
from html import unescape

from bs4 import BeautifulSoup
import pandas as pd

# Regex extraction

def _regex_extract(x: str, pattern: str) -> str:
    m = re.search(pattern, x)
    if m:
        return m.group(1)
    else:
        raise ValueError(f"Failed to extract with pattern {pattern} from {x}")

def regex_extractor(pattern: str, target_dtype: str = 'string') -> pd.DataFrame:
    def extractor(df: pd.DataFrame) -> pd.DataFrame:
        return df["_source_path"].apply(_regex_extract, args=(pattern,)).astype(target_dtype)
    return  extractor

# HTML text extraction

values = [
    r"[aA]rticle",
    r"[cC]ontent",
    r"[bB]ody",
    r"[mM]ain",
]

def is_body(tag):
    return tag.name == "main" or \
        ( \
            tag.name == "div" and \
            tag.find(attrs=["id", "class"], values=values) \
        )

def process_html_file(html_content: str):
    soup = BeautifulSoup(html_content, "lxml")
    el = soup.find(is_body) or soup
    text = el.get_text("; ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text

# Process SEC Filings

regex_flags = re.IGNORECASE | re.DOTALL | re.MULTILINE

def _process_text(soup):
    texts = []

    for text_block in soup.find_all(re.compile(r"(?<!Table)TextBlock$")):
        text = text_block.get_text(" ").replace("\xa0", " ")
        text = re.sub(r"\s[\s]+", " ", text)
        texts.append(text)

    return " ".join(texts).strip()

def process_sec_file(input_content: str) -> str:
    docs = re.search(r"<DOCUMENT>.*?<TYPE>EX-101\.INS.*?<XBRL>(.*?)</XBRL>.*?</DOCUMENT>", input_content, flags=regex_flags)
    if not docs:
        docs = re.search(r"<DOCUMENT>.*?<FILENAME>\S+?_htm.xml.*?<XML>(.*?)</XML>.*?</DOCUMENT>", input_content, flags=regex_flags)
    if not docs:
        return ""

    soup = BeautifulSoup(unescape(docs.group(1)), "xml")
    processed_text = _process_text(soup)

    return processed_text