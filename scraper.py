import requests
from bs4 import BeautifulSoup
from datetime import datetime

response = requests.get("https://quotes.toscrape.com")
soup = BeautifulSoup(response.text, "html.parser")
text = "\n".join(p.get_text(strip=True) for p in soup.find_all("p"))

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filename = f"scraped_{timestamp}.txt"

with open(filename, "w", encoding="utf-8") as f:
    f.write(f"Scraped at: {datetime.now()}\n{'='*50}\n{text}")

print(f"Saved to {filename}")