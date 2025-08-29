import datetime
import csv
import os
import re
import pathlib
from urllib.parse import urljoin, urlparse, unquote, quote

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pdfplumber

# -------- Counties: use the ACTUAL auction/sale pages --------
COUNTY_SITES = {
    "Pulaski County": "https://pulaskiclerkar.gov/auction-about/auction-notices/",
    "Sebastian County": "https://www.sebastiancountyar.gov/Departments/Circuit-Clerk/Commissioners-Sales",
    "Crawford County": "https://www.crawfordcountyar.gov/officials/circuit_clerk.cshtml",
    "Benton County": "https://bentoncountyar.gov/circuit-clerk/judicial-sales-and-foreclosures/",
    "Washington County": "https://www.washingtoncountyar.gov/how-do-i/view/foreclosure-information",
}
# --------------------------------------------------------------

DOC_EXT = (".pdf", ".doc", ".docx")

# Retry-friendly requests session
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36"
    })
    return s

def ensure_dir(path: str):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^\w\-\. ]", "_", name).strip()
    return cleaned[:180] or "file"

def download_pdf(sess: requests.Session, pdf_url: str, out_dir: str, hint: str = "") -> str:
    try:
        r = sess.get(pdf_url, timeout=25)
        r.raise_for_status()
        base = sanitize_filename(hint) if hint else sanitize_filename(unquote(pathlib.Path(urlparse(pdf_url).path).name))
        if not base.lower().endswith(".pdf"):
            base += ".pdf"
        ensure_dir(out_dir)
        fullpath = os.path.join(out_dir, base)
        with open(fullpath, "wb") as f:
            f.write(r.content)
        return fullpath
    except Exception:
        return ""

# ---------- ADDRESS EXTRACTION ----------
STREET_SUFFIX = r"(Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Circle|Cir\.?|Boulevard|Blvd\.?|Highway|Hwy\.?|Way|Trail|Trl\.?|Place|Pl\.?|Parkway|Pkwy\.?)"
ADDR_LINE = rf"\b\d{{1,6}}\s+[A-Za-z0-9'\.-]+(?:\s+[A-Za-z0-9'\.-]+)*\s+{STREET_SUFFIX}\b(?:\s*(?:Unit|Apt|Suite|Ste\.?|#)\s*[A-Za-z0-9-]+)?"
CITY_STATE_ZIP = r"(?:,\s*[A-Za-z .'-]+,\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?)"
ADDR_REGEXES = [
    rf"(?:Property Address|Site Address|Address|Known as)\s*[:\-]\s*({ADDR_LINE}(?:{CITY_STATE_ZIP})?)",
    rf"({ADDR_LINE}(?:{CITY_STATE_ZIP})?)",
]

def find_addresses_in_text(text: str):
    t = re.sub(r"[ \t]+", " ", text)
    t = re.sub(r"\r", "", t)
    addrs = []
    for pattern in ADDR_REGEXES:
        for m in re.finditer(pattern, t, flags=re.IGNORECASE):
            addr = m.group(1).strip()
            if len(addr) >= 8 and addr not in addrs:
                addrs.append(addr)
    return addrs

def extract_addresses_from_pdf(pdf_path: str):
    try:
        txt = ""
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                ptxt = page.extract_text() or ""
                txt += "\n" + ptxt
        return find_addresses_in_text(txt)
    except Exception as e:
        print(f"PDF parse error ({pdf_path}): {e}")
        return []

def extract_addresses_from_html(soup: BeautifulSoup):
    text = soup.get_text("\n", strip=True)
    return find_addresses_in_text(text)

# ---------- SITE PARSERS ----------
def parse_pulaski(sess, url, county, pdf_dir):
    rows = []
    r = sess.get(url, timeout=25)
    if r.status_code >= 400: return rows
    soup = BeautifulSoup(r.text, "html.parser")
    links = set()
    for a in soup.select('a[href]'):
        href = a.get("href", "")
        if "/auction-" in href:
            links.add(urljoin(url, href))
    for href in links:
        d = sess.get(href, timeout=25)
        if d.status_code >= 400: continue
        ds = BeautifulSoup(d.text, "html.parser")
        addresses = extract_addresses_from_html(ds)
        pdfs = [urljoin(href, a.get("href")) for a in ds.select('a[href$=".pdf"]')]
        for purl in pdfs:
            saved = download_pdf(sess, purl, pdf_dir, hint=f"{county} - {pathlib.Path(urlparse(purl).path).name}")
            if saved:
                addresses += extract_addresses_from_pdf(saved)
        for addr in dict.fromkeys(addresses):
            rows.append({
                "county": county,
                "address": addr,
                "maps": f"https://www.google.com/maps/search/?api=1&query={quote(addr)}",
                "source": href
            })
    return rows

def parse_pdf_listing(sess, url, county, pdf_dir):
    out = []
    r = sess.get(url, timeout=25)
    if r.status_code >= 400: return out
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.select('a[href$=".pdf"]'):
        full = urljoin(url, a.get("href"))
        saved = download_pdf(sess, full, pdf_dir, hint=f"{county} - {pathlib.Path(urlparse(full).path).name}")
        if saved:
            addresses = extract_addresses_from_pdf(saved)
            for addr in addresses:
                out.append({
                    "county": county,
                    "address": addr,
                    "maps": f"https://www.google.com/maps/search/?api=1&query={quote(addr)}",
                    "source": full
                })
    return out

def generic_page(sess, url, county, pdf_dir):
    out = []
    r = sess.get(url, timeout=25)
    if r.status_code >= 400: return out
    soup = BeautifulSoup(r.text, "html.parser")
    addresses = extract_addresses_from_html(soup)
    for addr in addresses:
        out.append({
            "county": county,
            "address": addr,
            "maps": f"https://www.google.com/maps/search/?api=1&query={quote(addr)}",
            "source": url
        })
    for a in soup.select('a[href$=".pdf"]'):
        href = urljoin(url, a.get("href"))
        saved = download_pdf(sess, href, pdf_dir, hint=f"{county} - {pathlib.Path(urlparse(href).path).name}")
        if saved:
            for addr in extract_addresses_from_pdf(saved):
                out.append({
                    "county": county,
                    "address": addr,
                    "maps": f"https://www.google.com/maps/search/?api=1&query={quote(addr)}",
                    "source": href
                })
    return out

# ---------- MAIN ----------
def scrape():
    s = make_session()
    today = datetime.date.today().strftime("%Y-%m-%d")
    pdf_dir = os.path.join("output", today, "pdfs")
    ensure_dir(pdf_dir)
    results = []
    for county, url in COUNTY_SITES.items():
        host = urlparse(url).netloc
        if "pulaskiclerkar.gov" in host:
            results.extend(parse_pulaski(s, url, county, pdf_dir))
        elif "sebastiancountyar.gov" in host or "crawfordcountyar.gov" in host:
            results.extend(parse_pdf_listing(s, url, county, pdf_dir))
        else:
            results.extend(generic_page(s, url, county, pdf_dir))
    # keep only unique (county+address)
    deduped, seen = [], set()
    for r in results:
        key = (r["county"], r["address"].lower())
        if r["address"] and key not in seen:
            deduped.append(r)
            seen.add(key)
    return deduped

# ---------- OUTPUT ----------
def save_csv(rows, today):
    os.makedirs("output", exist_ok=True)
    ts = datetime.datetime.now().strftime("%H%M%S")
    path = os.path.join("output", f"properties_{today}_{ts}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["county", "address", "maps", "source"])
        for r in rows:
            w.writerow([r["county"], r["address"], r["maps"], r["source"]])
    return path

def save_html(rows, today):
    os.makedirs("output", exist_ok=True)
    ts = datetime.datetime.now().strftime("%H%M%S")
    path = os.path.join("output", f"properties_{today}_{ts}.html")
    html = [
        "<!doctype html><meta charset='utf-8'>",
        f"<title>Properties — {today}</title>",
        "<style>body{font-family:Arial, sans-serif;margin:24px}table{border-collapse:collapse;width:100%}th,td{border-bottom:1px solid #eee;padding:8px 10px;text-align:left}a{color:blue;text-decoration:none}</style>",
        f"<h1>Properties ({today})</h1>",
        "<table><thead><tr><th>County</th><th>Address</th><th>Links</th></tr></thead><tbody>"
    ]
    for r in rows:
        html.append(
            f"<tr><td>{r['county']}</td><td>{r['address']}</td>"
            f"<td><a target='_blank' href='{r['maps']}'>Maps</a> | "
            f"<a target='_blank' href='{r['source']}'>Notice</a></td></tr>"
        )
    html.append("</tbody></table>")
    with open(path, "w", encoding="utf-8") as f:
        f.write("".join(html))
    return path

if __name__ == "__main__":
    today = datetime.date.today().strftime("%Y-%m-%d")
    rows = scrape()
    print(f"Found {len(rows)} properties with addresses.")
    for r in rows[:10]:
        print(f"- {r['county']}: {r['address']}")
    csv_path = save_csv(rows, today)
    html_path = save_html(rows, today)
    print(f"\nSaved CSV  → {csv_path}")
    print(f"Saved HTML → {html_path}")
