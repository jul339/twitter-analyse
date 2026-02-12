import os
import requests
from dotenv import load_dotenv

load_dotenv()

response = requests.get(
    "https://api.x.com/2/users/by/username/xdevelopers",
    headers={"Authorization": f"Bearer {os.getenv('BEARER_TOKEN')}"},
)

print(response.json())
