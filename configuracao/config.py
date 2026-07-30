import os
from dotenv import load_dotenv


# Carrega o ambiente de desenvolvimento
load_dotenv(".env.dev")


# Ambiente atual
ENVIRONMENT = os.getenv("ENVIRONMENT")


# =========================
# PostgreSQL
# =========================

DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")


# =========================
# MinIO
# =========================

MINIO_HOST = os.getenv("MINIO_HOST")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")


# =========================
# APIs
# =========================

STEAM_API_URL = os.getenv("STEAM_API_URL")
CHEAPSHARK_API_URL = os.getenv("CHEAPSHARK_API_URL")

# Uma observação importante

# Esse código está funcionando, mas depois vamos melhorar.

# Hoje ele está fixo:

# load_dotenv(".env.dev") Mas a versão profissional seria: ENVIRONMENT=dev
# e o próprio código escolher: .env.dev
# .env.test
# .env.prod

# dependendo do ambiente.

# Vamos evoluir para isso depois.