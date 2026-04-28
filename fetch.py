#fetches the results of a Dune query and saves it as a CSV file locally
#api key hidden for obvious reasons
import requests

QUERY_ID = "your_query_id"
API_KEY = "your_api_key"

response = requests.get(
    f"https://api.dune.com/api/v1/query/{QUERY_ID}/results/csv",
    headers={"X-Dune-Api-Key": API_KEY}
)

with open("results.csv", "w") as f:
    f.write(response.text)