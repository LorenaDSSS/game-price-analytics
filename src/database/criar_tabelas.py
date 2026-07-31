import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # raiz → configuracao
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # src/ → database

from configuracao.logging import configurar_logger
from database.conexao import criar_conexao

logger = configurar_logger()


def criar_tabelas():
    logger.info("[DATABASE] Criando tabelas PostgreSQL")

    conexao = criar_conexao()
    cursor = conexao.cursor()

    comandos = [
        "DROP TABLE IF EXISTS fato_historico_precos CASCADE;",
        "DROP TABLE IF EXISTS fato_ofertas CASCADE;",
        "DROP TABLE IF EXISTS ranking_desenvolvedores CASCADE;",
        "DROP TABLE IF EXISTS ranking_generos CASCADE;",
        """
        CREATE TABLE fato_historico_precos (
            id SERIAL PRIMARY KEY,
            steam_app_id INTEGER,
            nome VARCHAR(255),
            preco_oferta NUMERIC(10,2),
            preco_normal NUMERIC(10,2),
            preco_steam NUMERIC(10,2),
            moeda_steam VARCHAR(10),
            desconto NUMERIC(5,2),
            data_ingestao DATE,
            UNIQUE(steam_app_id, data_ingestao)
        );
        """,
        """
        CREATE TABLE fato_ofertas (
            id SERIAL PRIMARY KEY,
            steam_app_id INTEGER,
            nome VARCHAR(255),
            preco_oferta NUMERIC(10,2),
            preco_normal NUMERIC(10,2),
            moeda_oferta VARCHAR(10),
            desconto NUMERIC(5,2),
            avaliacao VARCHAR(100),
            avaliacao_percentual INTEGER,
            loja VARCHAR(100),
            preco_steam NUMERIC(10,2),
            preco_steam_original NUMERIC(10,2),
            moeda_steam VARCHAR(10),
            economia NUMERIC(10,2),
            nivel_oferta VARCHAR(50),
            data_ingestao DATE
        );
        """,
        """
        CREATE TABLE ranking_desenvolvedores (
            id SERIAL PRIMARY KEY,
            desenvolvedor_nome VARCHAR(255),
            quantidade_jogos INTEGER,
            desconto_medio NUMERIC(5,2),
            preco_medio_oferta NUMERIC(10,2),
            preco_medio_normal NUMERIC(10,2),
            maior_desconto NUMERIC(5,2),
            avaliacao_media NUMERIC(5,1),
            data_ingestao DATE
        );
        """,
        """
        CREATE TABLE ranking_generos (
            id SERIAL PRIMARY KEY,
            genero_id INTEGER,
            genero_nome VARCHAR(255),
            quantidade_jogos INTEGER,
            desconto_medio NUMERIC(5,2),
            preco_medio_oferta NUMERIC(10,2),
            preco_medio_normal NUMERIC(10,2),
            maior_desconto NUMERIC(5,2),
            avaliacao_media NUMERIC(5,1),
            data_ingestao DATE
        );
        """,
    ]

    try:
        for comando in comandos:
            cursor.execute(comando)
        conexao.commit()
        logger.info("[DATABASE] Tabelas criadas com sucesso")

    except Exception as erro:
        conexao.rollback()
        logger.error(f"[DATABASE] Erro criando tabelas: {erro}")
        raise

    finally:
        cursor.close()
        conexao.close()


if __name__ == "__main__":
    criar_tabelas()