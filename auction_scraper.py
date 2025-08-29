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
                txt = page.extract_text() or ""
                text += "\n" + txt

        # Date: look for mm/dd/yyyy
        m = re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", text)
        if m:
            details["date"] = m.group(0)

        # Location: crude match for courthouse/office
        if "courthouse" in text.lower():
            details["location"] = "Courthouse"
        elif "sheriff" in text.lower():
            details["location"] = "Sheriff's Office"

        # Property: look for Parcel, Lot, or Address-like text
        pm = re.search(r"(Parcel\s*\d+|Lot\s*\d+|Address:\s*.+)", text)
        if pm:
            details["property"] = pm.group(0)

    except Exception as e:
        print(f"PDF parse error: {e}")
    return details

def scrape_auctions():
    s = make_session()
    results = []
    for county, url in COUNTY_SITES.items():
        results.extend(scrape_page(s, county, url))
    # Deduplicate by link
    deduped, seen = [], set()
    for r in results:
        if r["link"] not in seen:
            deduped.append(r)
            seen.add(r["link"])
    return deduped

def write_csv(rows):
    os.makedirs("output", exist_ok=True)
    today = datetime.date.today().strftime("%Y-%m-%d")
    path = os.path.join("output", f"auctions_{today}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "county", "title", "auction_date", "auction_location", "property_details", "link"])
        for r in rows:
            w.writerow([
                today,
                r["county"],
                r["title"],
                r.get("date", ""),
                r.get("location", ""),
                r.get("property", ""),
                r["link"],
            ])
    return path

if __name__ == "__main__":
    today = datetime.date.today().strftime("%Y-%m-%d")
    auctions = scrape_auctions()
    print(f"Auction Results for {today}")
    print("=" * 60)
    if not auctions:
        print("No matches yet. Next step: plug in each county’s EXACT auction page URL.")
    for a in auctions:
        print(f"{a['county']} — {a['title']} | Date: {a['date']} | Location: {a['location']} | Property: {a['property']} ({a['link']})")
    csv_path = write_csv(auctions)
    print(f"\nSaved {len(auctions)} rows to: {csv_path}")
