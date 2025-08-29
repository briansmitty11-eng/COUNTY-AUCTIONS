import datetime
import csv
import os
import socket
import re
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pdfplumber

COUNTY_SITES = {
    "Sebastian County": "https://www.sebastiancountyar.gov/",
    "Pulaski County": "https://www.pulaskicounty.net/",
    "Crawford County": "https://www.crawfordcountyar.gov/",
    "Franklin County": "https://franklincountyar.gov/",
    # Disabled until correct URLs provided:
    # "Benton County": "https://bentoncountyar.gov/",
    # "Washington County": "https://www.washingtoncountyar.gov/",
    # "Johnson County": "https://johnsoncountyar.gov/",
}

INCLUDE_HINTS = (
    "auction", "commissioner", "sheriff", "sale", "foreclosure",
    "tax sale", "public notice", "trustee",
)
EXCLUDE_HINTS = (
    "inmate", "detention", "jail", "visitation", "k9",
    "pay-taxes", "treasurer", "taxes", "forms", "faq", "resources",
    "records", "jobs", "careers", "swat", "juvenile", "security",
)
DOC_EXT = (".pdf", ".doc", ".docx")

def make_session():
    s = requests.Session()
    retries = Retry(
        total=3, connect=3, read=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        raise_on_status=False,
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    })
    return s

def dns_ok(url: str) -> bool:
    try:
        host = urlparse(url).netloc or urlparse("http://" + url).netloc
        socket.gethostbyname(host)
        return True
    except Exception:
        return False

def looks_relevant(text: str, href: str) -> bool:
    t = (text or "").lower()
    h = (href or "").lower()
    if any(x in t or x in h for x in INCLUDE_HINTS):
        if not any(x in t or x in h for x in EXCLUDE_HINTS):
            return True
    if h.endswith(DOC_EXT) and not any(x in h for x in EXCLUDE_HINTS):
        return True
    return False

def same_site(full: str, base: str) -> bool:
    return urlparse(full).netloc.split(":")[0].endswith(
        urlparse(base).netloc.split(":")[0]
    )

def scrape_page(sess: requests.Session, county: str, url: str):
    out = []
    if not dns_ok(url):
        print(f"[{county}] DNS could not resolve host for URL: {url}")
        return out
    try:
        resp = sess.get(url, timeout=20, allow_redirects=True)
        if resp.status_code == 403:
            print(f"[{county}] HTTP 403 (blocked). Use the county's exact auction page.")
            return out
        if resp.status_code >= 400:
            print(f"[{county}] HTTP {resp.status_code} for {url}")
            return out

        soup = BeautifulSoup(resp.text, "html.parser")
        seen = set()
        for a in soup.find_all("a", href=True):
            title = a.get_text(" ", strip=True) or "(no title)"
            full = urljoin(resp.url, a["href"])
            key = (title, full)
            if key in seen:
                continue
            seen.add(key)
            if looks_relevant(title, full) and (same_site(full, resp.url) or full.lower().endswith(DOC_EXT)):
                details = {"date": "", "location": "", "property": ""}
                if full.lower().endswith(".pdf"):
                    details = extract_pdf_details(full)
                out.append({
                    "county": county,
                    "title": title,
                    "link": full,
                    "date": details["date"],
                    "location": details["location"],
                    "property": details["property"]
                })
        return out
    except requests.exceptions.ConnectionError as e:
        print(f"[{county}] Connection error: {e}")
    except requests.exceptions.Timeout:
        print(f"[{county}] Timeout retrieving {url}")
    except Exception as e:
        print(f"[{county}] Unexpected error: {e}")
    return out

def extract_pdf_details(pdf_url):
    details = {"date": "", "location": "", "property": ""}
    try:
        resp = requests.get(pdf_url, timeout=20)
        temp_file = "temp_notice.pdf"
        with open(temp_file, "wb") as f:
            f.write(resp.content)
        text = ""
        with pdfplumber.open(temp_file) as pdf:
            for page in pdf.pages:
                txt
