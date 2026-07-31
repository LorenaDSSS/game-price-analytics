import os
from pathlib import Path
from dotenv import load_dotenv

# Caminho absoluto → sempre encontra o .env na raiz do projeto
BASE_DIR = Path(__file__).resolve().parents[1]
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
load_dotenv(BASE_DIR / f".env.{ENVIRONMENT}")

# Seleciona o arquivo .env de acordo com a variável ENVIRONMENT (dev | test | prod)
# Exemplo de uso: ENVIRONMENT=prod python src/main.py
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
load_dotenv(f".env.{ENVIRONMENT}")

# PostgreSQL
DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")

# MinIO
MINIO_HOST = os.getenv("MINIO_HOST")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

# APIs
STEAM_API_URL = os.getenv("STEAM_API_URL")
CHEAPSHARK_API_URL = os.getenv("CHEAPSHARK_API_URL")

# HTTP
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", 30))
HTTP_RETRY = int(os.getenv("HTTP_RETRY", 3))