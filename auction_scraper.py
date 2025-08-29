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

# ========== EXACT COUNTY PAGES (tweak as needed) ==========
COUNTY_SITES = {
    # These are the pages where auctions/commissioner/sheriff sales are actually posted.
    "Pulaski County": "https://pulaskiclerkar.gov/auction-about/auction-notices/",
    "Sebastian County": "https://www.sebastiancountyar.gov/Departments/Circuit-Clerk/Commissioners-Sales",
    "Crawford County":  "https://www.crawfordcountyar.gov/officials/circuit_clerk.cshtml",
    "Benton County":    "https://bentoncountyar.gov/circuit-clerk/judicial-sales-and-foreclosures/",
    "Washington County":"https://www.washingtoncountyar.gov/how-do-i/view/foreclosure-information",
}
# ===========================================================

DOC_EXT = (".pdf", ".doc", ".docx")

# Common auction words to find PDF links on listing pages
INCLUDE_HINTS = ("auction", "commissioner", "sheriff", "sale", "foreclosure", "trustee", "notice")

# --------- HTTP session with retries ----------
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

# ---------- utilities ----------
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

def is_pdf_link(href: str) -> bool:
    return href.lower().endswith(".pdf")

def looks_relevant_link(text: str, href: str) -> bool:
    t = (text or "").lower()
    h = (href or "").lower()
    return any(k in t or k in h for k in INCLUDE_HINTS)

# ---------- ADDRESS EXTRACTION ----------
# Strong street-address matcher (US-style)
STREET_SUFFIX = r"(?:Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Circle|Cir\.?|Boulevard|Blvd\.?|Highway|Hwy\.?|Way|Trail|Trl\.?|Place|Pl\.?|Parkway|Pkwy\.?)"
ADDR_LINE = rf"\b\d{{1,6}}\s+[A-Za-z0-9'\.-]+(?:\s+[A-Za-z0-9'\.-]+)*\s+{STREET_SUFFIX}\b(?:\s*(?:Unit|Apt|Suite|Ste\.?|#)\s*[A-Za-z0-9-]+)?"
CITY_STATE_ZIP = r"(?:,\s*[A-Za-z .'-]+,\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?)"
ADDR_REGEXES = [
    rf"(?:Property Address|Site Address|Address|Known as)\s*[:\-]\s*({ADDR_LINE}(?:{CITY_STATE_ZIP})?)",
    rf"({ADDR_LINE}(?:{CITY_STATE_ZIP})?)",
]

def find_addresses_in_text(text: str):
    # Normalize spaces
    t = re.sub(r"[ \t]+", " ", text)
    t = re.sub(r"\r", "", t)
    addrs = []
    for pattern in ADDR_REGEXES:
        for m in re.finditer(pattern, t, flags=re.IGNORECASE):
            addr = m.group(1).strip()
            # Basic de-dup and sanity
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
    # Pull visible text and hunt addresses (helpful if a county posts addresses in the page body)
    text = soup.get_text("\n", strip=True)
    return find_addresses_in_text(text)

# ---------- SITE-SPECIFIC PARSERS ----------
def parse_pulaski(sess, url, county, pdf_dir):
    # Pulaski: list of notices with detail pages; some link to PDFs, some have addresses on-page.
    rows = []
    r = sess.get(url, timeout=25)
    if r.status_code >= 400:
        return rows
    soup = BeautifulSoup(r.text, "html.parser")

    # Grab detail links in their notices area
    links = set()
    for a in soup.select('a[href]'):
        href = a.get("href", "")
        if not href:
            continue
        full = urljoin(url, href)
        if "/auction-" in full or looks_relevant_link(a.get_text(), full):
            links.add(full)

    for href in links:
        try:
            d = sess.get(href, timeout=25); 
            if d.status_code >= 400:
                continue
            ds = BeautifulSoup(d.text, "html.parser")
            addresses = extract_addresses_from_html(ds)

            # also collect PDFs linked from the detail page
            pdfs = [urljoin(href, a.get("href")) for a in ds.select('a[href$=".pdf"]')]
            for purl in pdfs:
                saved = download_pdf(sess, purl, pdf_dir, hint=f"{county} - {pathlib.Path(urlparse(purl).path).name}")
                if saved:
                    addresses += extract_addresses_from_pdf(saved)

            # de-dup addresses
            addresses = list(dict.fromkeys(addresses))
            for addr in addresses:
                rows.append({
                    "county": county,
                    "address": addr,
                    "maps": f"https://www.google.com/maps/search/?api=1&query={quote(addr)}",
                    "source": href,
                    "local_pdf": ""  # not guaranteed unless found above
                })
        except Exception:
            continue
    return rows

def parse_pdf_listing(sess, url, county, pdf_dir, link_selector='a[href$=".pdf"]'):
    out = []
    r = sess.get(url, timeout=25)
    if r.status_code >= 400:
        return out
    soup = BeautifulSoup(r.text, "html.parser")

    pdf_links = []
    for a in soup.select(link_selector):
        href = a.get("href") or ""
        full = urljoin(url, href)
        if is_pdf_link(full) or looks_relevant_link(a.get_text(), full):
            pdf_links.append(full)

    for pdf_url in set(pdf_links):
        saved = download_pdf(sess, pdf_url, pdf_dir, hint=f"{county} - {pathlib.Path(urlparse(pdf_url).path).name}")
        if not saved:
            continue
        addresses = extract_addresses_from_pdf(saved)
        for addr in addresses:
            out.append({
                "county": county,
                "address": addr,
                "maps": f"https://www.google.com/maps/search/?api=1&query={quote(addr)}",
                "source": pdf_url,
                "local_pdf": saved
            })
    return out

def generic_page_addresses(sess, url, county, pdf_dir):
    """Fallback: scan page text for addresses; follow PDFs and extract from them as well."""
    out = []
    r = sess.get(url, timeout=25)
    if r.status_code >= 400:
        return out
    soup = BeautifulSoup(r.text, "html.parser")

    # Try addresses from visible HTML
    addrs = extract_addresses_from_html(soup)
    for addr in addrs:
        out.append({
            "county": county,
            "address": addr,
            "maps": f"https://www.google.com/maps/search/?api=1&query={quote(addr)}",
            "source": url,
            "local_pdf": ""
        })

    # Also parse PDFs on the page
    for a in soup.select('a[href$=".pdf"]'):
        href = urljoin(url, a.get("href"))
        saved = download_pdf(sess, href, pdf_dir, hint=f"{county} - {pathlib.Path(urlparse(href).path).name}")
        if saved:
            addrs2 = extract_addresses_from_pdf(saved)
            for addr in addrs2:
                out.append({
                    "county": county,
                    "address": addr,
                    "maps": f"https://www.google.com/maps/search/?api=1&query={quote(addr)}",
                    "source": href,
                    "local_pdf": saved
                })
    return out

# ---------- MAIN SCRAPE ----------
def scrape():
    s = make_session()
    today = datetime.date.today().strftime("%Y-%m-%d")
    pdf_dir = os.path.join("output", today, "pdfs")
    ensure_dir(pdf_dir)

    results = []

    for county, url in COUNTY_SITES.items():
        host = urlparse(url).netloc
        try:
            if "pulaskiclerkar.gov" in host:
                results.extend(parse_pulaski(s, url, county, pdf_dir))
            elif "sebastiancountyar.gov" in host:
                results.extend(parse_pdf_listing(s, url, county, pdf_dir))
            elif "crawfordcountyar.gov" in host:
                results.extend(parse_pdf_listing(s, url, county, pdf_dir))
            else:
                # Benton/Washington: try generic page scan + PDFs
                results.extend(generic_page_addresses(s, url, county, pdf_dir))
        except Exception as e:
            print(f"[{county}] Error: {e}")

    # Keep ONLY rows with an address
    rows = [r for r in results if r.get("address")]
    # Dedupe by (county, address)
    deduped = []
    seen = set()
    for r in rows:
        key = (r["county"], r["address"].lower())
        if key not in seen:
            deduped.append(r)
            seen.add(key)
    return deduped

# ---------- OUTPUTS (CSV + super simple HTML) ----------
def save_csv(rows, today):
    ensure_dir("output")
    ts = datetime.datetime.now().strftime("%H%M%S")
    path = os.path.join("output", f"properties_{today}_{ts}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["county", "address", "maps", "source", "local_pdf"])
        for r in rows:
            w.writerow([r["county"], r["address"], r["maps"], r["source"], r["local_pdf"]])
    return path

def save_html(rows, today):
    ensure_dir("output")
    ts = datetime.datetime.now().strftime("%H%M%S")
    path = os.path.join("output", f"properties_{today}_{ts}.html")
    html = [
        "<!doctype html><meta charset='utf-8'>",
        f"<title>Properties — {today}</title>",
        "<style>body{font-family:system-ui,Segoe UI,Roboto,Arial;margin:24px}table{border-collapse:collapse;width:100%}th,td{border-bottom:1px solid #eee;padding:8px 10px;text-align:left}a.button{padding:6px 10px;border:1px solid #ccc;border-radius:8px;text-decoration:none;margin-right:6px;display:inline-block}</style>",
        f"<h1>Properties <span style='color:#666;font-size:14px'>({today})</span></h1>",
        "<table><thead><tr><th>County</th><th>Address</th><th>Open</th></tr></thead><tbody>"
    ]
    def esc(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
    for r in rows:
        maps = f"<a class='button' target='_blank' href='{r['maps']}'>Maps</a>"
        src  = f"<a class='button' target='_blank' href='{r['source']}'>Notice</a>"
        pdf  = f"<a class='button' target='_blank' href='{esc(os.path.relpath(r['local_pdf'], start=os.path.dirname(path)))}'>PDF</a>" if r.get("local_pdf") else ""
        html.append(f"<tr><td>{esc(r['county'])}</td><td>{esc(r['address'])}</td><td>{maps}{src}{pdf}</td></tr>")
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
    print(f"Saved HTML → {html_path}  (double-click to open)")
