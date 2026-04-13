import os
from dotenv import load_dotenv

load_dotenv() # carrega .env para os.environ

API_KEY = os.environ["API_KEY"]
API_BASE_URL = os.environ["API_BASE_URL"]