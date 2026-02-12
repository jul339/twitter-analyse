from atproto import Client
import os
from dotenv import load_dotenv

load_dotenv()

client = Client()
session = client.login(os.getenv("BLEUSKY_USERNAME"), os.getenv("BLEUSKY_PASSWORD"))
print("Access JWT:", session.access_jwt[:50] + "...")
print("Refresh JWT:", session.refresh_jwt[:50] + "...")
# ou afficher seulement handle / did
print("Logged in as:", session.handle, session.did)
