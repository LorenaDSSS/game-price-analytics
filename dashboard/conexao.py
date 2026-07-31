import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import psycopg2

from configuracao.config import (       # ← isso carrega o .env
    DATABASE_HOST, DATABASE_PORT, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD
)

def criar_conexao():
    return psycopg2.connect(
        host=DATABASE_HOST, port=DATABASE_PORT,
        database=DATABASE_NAME, user=DATABASE_USER, password=DATABASE_PASSWORD,
    )

def carregar_ofertas():
    conn = criar_conexao()
    df = pd.read_sql("""
        SELECT nome, loja, preco_oferta, moeda_oferta, preco_steam, preco_steam_original,
               moeda_steam, desconto, avaliacao, avaliacao_percentual, nivel_oferta, economia,
               data_ingestao
        FROM fato_ofertas ORDER BY desconto DESC
    """, conn)
    conn.close()
    return df

def carregar_ranking_generos():
    conn = criar_conexao()
    df = pd.read_sql("""
        SELECT genero_nome, quantidade_jogos, desconto_medio, preco_medio_oferta,
               preco_medio_normal, maior_desconto, avaliacao_media
        FROM ranking_generos ORDER BY quantidade_jogos DESC
    """, conn)
    conn.close()
    return df

def carregar_ranking_desenvolvedores():
    conn = criar_conexao()
    df = pd.read_sql("""
        SELECT desenvolvedor_nome, quantidade_jogos, desconto_medio, preco_medio_oferta,
               preco_medio_normal, maior_desconto, avaliacao_media
        FROM ranking_desenvolvedores ORDER BY quantidade_jogos DESC LIMIT 20
    """, conn)
    conn.close()
    return df

def carregar_historico(nome_jogo):
    conn = criar_conexao()
    df = pd.read_sql("""
        SELECT data_ingestao, preco_oferta, preco_normal, preco_steam, moeda_steam, desconto
        FROM fato_historico_precos WHERE nome ILIKE %s ORDER BY data_ingestao
    """, conn, params=(f"%{nome_jogo}%",))
    conn.close()
    return df

def listar_jogos():
    conn = criar_conexao()
    df = pd.read_sql("SELECT DISTINCT nome FROM fato_historico_precos ORDER BY nome", conn)
    conn.close()
    return df["nome"].tolist()