import requests

url = "https://api.github.com/search/repositories"

params = {
    "q": "stars:>=500",
    "per_page": 1000000
}

r = requests.get(url, params=params)
data = r.json()

print("Nombre de dépôts :", data["total_count"])