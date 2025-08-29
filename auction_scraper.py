import requests
from bs4 import BeautifulSoup
import datetime

# List of county auction websites (replace with the actual ones you want)
COUNTY_SITES = {
    "Benton County": "https://bentoncountyar.gov/auction-listings/",
    "Washington County": "https://www.washingtoncountyar.gov/auction-listings/",
    "Sebastian County": "https://www.sebastiancountyar.gov/auction-listings/",
    "Crawford County": "https://www.crawfordcountyar.gov/auction-listings/",
    "Pulaski County": "https://www.pulaskicounty.net/auction-listings/",
    "Franklin County": "https://www.franklincountyar.gov/auction-listings/",
    "Johnson County": "https://www.johnsoncountyar.gov/auction-listings/"
}

def scrape_auctions():
    results = []
    for county, url in COUNTY_SITES.items():
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                
                # Adjust this depending on each county site’s layout
                items = soup.find_all("a")  # Example: look for links
                
                for item in items:
                    title = item.get_text(strip=True)
                    link = item.get("href")
                    if title and link:
                        results.append({
                            "county": county,
                            "title": title,
                            "link": link
                        })
        except Exception as e:
            print(f"Error scraping {county}: {e}")
    return results

if __name__ == "__main__":
    today = datetime.date.today().strftime("%Y-%m-%d")
    auctions = scrape_auctions()
    
    print(f"Auction Results for {today}")
    print("="*50)
    for auction in auctions:
        print(f"{auction['county']} - {auction['title']} ({auction['link']})")
