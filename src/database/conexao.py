import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import psycopg2

from configuracao.config import DATABASE_HOST, DATABASE_PORT, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD
from configuracao.logging import configurar_logger

logger = configurar_logger()


def criar_conexao():
    try:
        conexao = psycopg2.connect(
            host=DATABASE_HOST,
            port=DATABASE_PORT,
            database=DATABASE_NAME,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD,
        )
        logger.info("[DATABASE] Conectado ao PostgreSQL")
        return conexao

    except Exception as erro:
        logger.error(f"[DATABASE] Erro conexão PostgreSQL: {erro}")
        raise