import datetime
import csv
import os
import socket
import re
import pathlib
from urllib.parse import urljoin, urlparse, unquote

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pdfplumber

# -------- SETTINGS --------
COUNTY_SITES = {
    "Sebastian County": "https://www.sebastiancountyar.gov/",
    "Pulaski County": "https://www.pulaskicounty.net/",
    "Crawford County": "https://www.crawfordcountyar.gov/",
    "Franklin County": "https://franklincountyar.gov/",
    # Add exact auction pages as you find them for best results
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
# --------------------------

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

def sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^\w\-\. ]", "_", name).strip()
    return cleaned[:180] or "file"

def ensure_dir(path: str):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def download_pdf(sess: requests.Session, pdf_url: str, out_dir: str, hint: str = "") -> str:
    try:
        r = sess.get(pdf_url, timeout=25)
        r.raise_for_status()
        # build a sensible filename
        basename = sanitize_filename(hint) if hint else sanitize_filename(unquote(pathlib.Path(urlparse(pdf_url).path).name))
        if not basename.lower().endswith(".pdf"):
            basename += ".pdf"
        ensure_dir(out_dir)
        fullpath = os.path.join(out_dir, basename)
        with open(fullpath, "wb") as f:
            f.write(r.content)
        return fullpath
    except Exception:
        return ""

def parse_date_strings(text: str) -> str:
    # Grab MM/DD/YYYY or Month D, YYYY or MM/DD/YY
    patterns = [
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{2,4}\b",
    ]
    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            return m.group(0)
    return ""

def extract_property_bits(text: str) -> str:
    # Try common markers; keep it short for dashboard
    for p in [
        r"Address:\s*[^\n]+",
        r"Parcel\s*#?:?\s*\w+[-\w]*",
        r"Lot\s+\d+[^\n]*",
        r"Legal Description:\s*[^\n]+",
        r"Property Address:\s*[^\n]+",
    ]:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            return m.group(0)[:200]
    # backup: grab a line with "Street", "Ave", etc.
    m = re.search(r"[^\n]*(Street|St\.|Avenue|Ave\.|Road|Rd\.|Lane|Ln\.|Drive|Dr\.)[^\n]*", text, flags=re.IGNORECASE)
    if m:
        return m.group(0)[:200]
    return ""

def extract_location(text: str) -> str:
    t = text.lower()
    if "courthouse" in t:
        return "Courthouse"
    if "sheriff" in t and "office" in t:
        return "Sheriff's Office"
    if "county judge" in t:
        return "County Judge Office"
    # fallback: look for "at <...> County Courthouse"
    m = re.search(r"at\s+the\s+[^\n,]*courthouse", text, flags=re.IGNORECASE)
    if m:
        return m.group(0)[:120]
    return ""

def extract_pdf_details_from_file(pdf_path: str) -> dict:
    details = {"auction_date": "", "auction_location": "", "property_details": ""}
    try:
        text = ""
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                txt = page.extract_text() or ""
                text += "\n" + txt

        details["auction_date"] = parse_date_strings(text)
        details["auction_location"] = extract_location(text)
        details["property_details"] = extract_property_bits(text)
    except Exception as e:
        print(f"PDF parse error ({pdf_path}): {e}")
    return details

def scrape_page(sess: requests.Session, county: str, url: str, pdf_dir: str):
    out = []
    if not dns_ok(url):
        print(f"[{county}] DNS could not resolve host for URL: {url}")
        return out
    try:
        resp = sess.get(url, timeout=25, allow_redirects=True)
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
                row = {
                    "county": county,
                    "title": title,
                    "link": full,
                    "auction_date": "",
                    "auction_location": "",
                    "property_details": "",
                    "local_pdf": ""
                }

                if full.lower().endswith(".pdf"):
                    # download and parse locally
                    saved = download_pdf(sess, full, pdf_dir, hint=f"{county} - {title}")
                    row["local_pdf"] = saved
                    if saved:
                        det = extract_pdf_details_from_file(saved)
                        row.update(det)

                out.append(row)

        return out

    except requests.exceptions.ConnectionError as e:
        print(f"[{county}] Connection error: {e}")
    except requests.exceptions.Timeout:
        print(f"[{county}] Timeout retrieving {url}")
    except Exception as e:
        print(f"[{county}] Unexpected error: {e}")
    return out

def scrape_auctions():
    s = make_session()
    today = datetime.date.today().strftime("%Y-%m-%d")
    pdf_dir = os.path.join("output", today, "pdfs")
    ensure_dir(pdf_dir)

    results = []
    for county, url in COUNTY_SITES.items():
        results.extend(scrape_page(s, county, url, pdf_dir))

    # Deduplicate by link
    deduped, seen = [], set()
    for r in results:
        if r["link"] not in seen:
            deduped.append(r)
            seen.add(r["link"])
    return deduped

def write_csv(rows, today):
    ensure_dir("output")
    # unique filename each run
    ts = datetime.datetime.now().strftime("%H%M%S")
    path = os.path.join("output", f"auctions_{today}_{ts}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "county", "title", "auction_date", "auction_location", "property_details", "link", "local_pdf"])
        for r in rows:
            w.writerow([
                today, r["county"], r["title"], r["auction_date"], r["auction_location"],
                r["property_details"], r["link"], r["local_pdf"]
            ])
    return path

def write_html(rows, today):
    ensure_dir("output")
    ts = datetime.datetime.now().strftime("%H%M%S")
    path = os.path.join("output", f"auctions_{today}_{ts}.html")

    # Simple sortable table with search box
    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>County Auctions — {today}</title>
<style>
body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }}
h1 {{ margin-top: 0; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border-bottom: 1px solid #e5e7eb; padding: 8px 10px; vertical-align: top; }}
th {{ cursor: pointer; text-align: left; }}
.badge {{ display:inline-block; padding:2px 8px; border-radius:9999px; background:#eef2ff; color:#3730a3; font-size:12px; margin-left:6px; }}
.toolbar {{ margin: 12px 0 18px; }}
input[type="search"] {{ padding:8px 10px; width: 320px; max-width: 100%; border:1px solid #d1d5db; border-radius:8px; }}
.small {{ color:#6b7280; font-size:12px; }}
.wrap {{ white-space: normal; word-break: break-word; }}
a.button {{ display:inline-block; padding:6px 10px; border:1px solid #d1d5db; border-radius:8px; text-decoration:none; }}
</style>
</head>
<body>
  <h1>County Auctions <span class="small">({today})</span></h1>
  <div class="toolbar">
    <input id="q" type="search" placeholder="Filter by county, title, address…">
  </div>
  <table id="t">
    <thead>
      <tr>
        <th onclick="sortTable(0)">County</th>
        <th onclick="sortTable(1)">Title</th>
        <th onclick="sortTable(2)">Auction Date</th>
        <th onclick="sortTable(3)">Location</th>
        <th onclick="sortTable(4)">Property</th>
        <th>Links</th>
      </tr>
    </thead>
    <tbody>
"""
    def esc(s):
        return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

    for r in rows:
        link_web = f'<a class="button" href="{esc(r["link"])}" target="_blank">Open notice</a>'
        link_local = f' | <a class="button" href="{esc(os.path.relpath(r["local_pdf"], start=os.path.dirname(path))) }" target="_blank">Open local PDF</a>' if r.get("local_pdf") else ""
        date_badge = f'<span class="badge">{esc(r["auction_date"])}</span>' if r["auction_date"] else '<span class="badge" style="background:#fee2e2;color:#991b1b;">No date</span>'

        html += f"""      <tr>
        <td>{esc(r["county"])}</td>
        <td class="wrap">{esc(r["title"])}</td>
        <td>{date_badge}</td>
        <td class="wrap">{esc(r["auction_location"])}</td>
        <td class="wrap">{esc(r["property_details"])}</td>
        <td>{link_web}{link_local}</td>
      </tr>
"""

    html += """    </tbody>
  </table>

<script>
const q = document.getElementById('q');
q.addEventListener('input', () => {
  const term = q.value.toLowerCase();
  const rows = document.querySelectorAll('#t tbody tr');
  rows.forEach(tr => {
    tr.style.display = tr.innerText.toLowerCase().includes(term) ? '' : 'none';
  });
});

function sortTable(col) {
  const tbody = document.querySelector('#t tbody');
  const rows = Array.from(tbody.querySelectorAll('tr'));
  const dir = tbody.getAttribute('data-dir') === 'asc' ? 'desc' : 'asc';
  rows.sort((a,b) => {
    const ta = a.children[col].innerText.toLowerCase();
    const tb = b.children[col].innerText.toLowerCase();
    if (ta < tb) return dir === 'asc' ? -1 : 1;
    if (ta > tb) return dir === 'asc' ? 1 : -1;
    return 0;
  });
  tbody.innerHTML = '';
  rows.forEach(r => tbody.appendChild(r));
  tbody.setAttribute('data-dir', dir);
}
</script>

</body>
</html>"""

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path

if __name__ == "__main__":
    today = datetime.date.today().strftime("%Y-%m-%d")
    rows = scrape_auctions()
    print(f"Auction Results for {today}")
    print("=" * 60)
    for a in rows:
        print(f"{a['county']} — {a['title']} | Date: {a['auction_date']} | Location: {a['auction_location']} | Property: {a['property_details']}")

    csv_path = write_csv(rows, today)
    html_path = write_html(rows, today)
    print(f"\nSaved CSV → {csv_path}")
    print(f"Saved Dashboard → {html_path}")
    print("Tip: double-click the HTML to review, sort, and click into notices.")
