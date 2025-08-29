import datetime
import socket
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ⛳ Replace these with the ACTUAL official auction pages you want.
# Some of the ones we tried were placeholders or 404s.
COUNTY_SITES = {
    "Benton County": "https://bentoncountyar.gov/",            # TODO: put exact auction page URL
    "Washington County": "https://www.washingtoncountyar.gov/",# TODO
    "Sebastian County": "https://www.sebastiancountyar.gov/",  # TODO
    "Crawford County": "https://www.crawfordcountyar.gov/",    # TODO
    "Pulaski County": "https://pulaskicounty.net/",            # TODO (note: different domain)
    "Franklin County": "https://www.franklincountyar.gov/",    # TODO
    "Johnson County": "https://johnsoncountyar.gov/",          # TODO (note: no 'www.')
}

KEYWORDS = ("auction", "sheriff", "sale", "foreclosure", "tax", "trustee", "notice")

def make_session():
    sess = requests.Session()
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        raise_on_status=False,
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    adapter = HTTPAdapter(max_retries=retries)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    sess.headers.update({
        # Friendly browser-like headers
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close"
    })
    return sess

def dns_ok(url: str) -> bool:
    try:
        host = urlparse(url).netloc or urlparse("http://" + url).netloc
        socket.gethostbyname(host)
        return True
    except Exception:
        return False

def scrape_page(sess: requests.Session, county: str, url: str):
    out = []
    if not dns_ok(url):
        print(f"[{county}] DNS could not resolve host for URL: {url}")
        return out

    try:
        resp = sess.get(url, timeout=20, allow_redirects=True)
        if resp.status_code >= 400:
            print(f"[{county}] HTTP {resp.status_code} for {url}")
            return out

        soup = BeautifulSoup(resp.text, "html.parser")

        # Grab links and keep those that look relevant
        for a in soup.find_all("a", href=True):
            text = (a.get_text(" ", strip=True) or "").lower()
            href = a["href"]
            full = urljoin(resp.url, href)
            # Keep links on same site or obvious document links
            same_host = urlparse(full).netloc.endswith(urlparse(resp.url).netloc.split(":")[0])
            looks_relevant = any(k in text for k in KEYWORDS) or any(k in full.lower() for k in KEYWORDS)
            if looks_relevant and (same_host or full.lower().endswith((".pdf", ".doc", ".docx"))):
                out.append({"county": county, "title": a.get_text(strip=True), "link": full})
        return out

    except requests.exceptions.SSLError as e:
        print(f"[{county}] SSL error: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"[{county}] Connection error: {e}")
    except requests.exceptions.Timeout:
        print(f"[{county}] Timeout retrieving {url}")
    except Exception as e:
        print(f"[{county}] Unexpected error: {e}")
    return out

def scrape_auctions():
    sess = make_session()
    results = []
    for county, url in COUNTY_SITES.items():
        items = scrape_page(sess, county, url)
        results.extend(items)
    return results

if __name__ == "__main__":
    today = datetime.date.today().strftime("%Y-%m-%d")
    auctions = scrape_auctions()
    print(f"Auction Results for {today}")
    print("=" * 60)
    if not auctions:
        print("No matches found yet. Verify the county URLs point to the actual auction page.")
    for a in auctions:
        print(f"{a['county']} — {a['title']} ({a['link']})")
