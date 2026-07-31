import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))  # raiz → configuracao
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # src/ → database

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from configuracao.config import DATABASE_HOST, DATABASE_PORT, DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD
from configuracao.logging import configurar_logger
from database.conexao import criar_conexao

logger = configurar_logger()


def criar_spark():
    return (
        SparkSession.builder
        .appName("GamePriceAnalytics-Carregar-Gold-Postgres")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )


def deletar_particao(tabela, data_ingestao):
    conexao = criar_conexao()
    cursor = conexao.cursor()
    try:
        cursor.execute(f"DELETE FROM {tabela} WHERE data_ingestao = %s", (str(data_ingestao),))
        conexao.commit()
        logger.info(f"[DATABASE] Registros antigos removidos: {tabela}")
    except Exception as erro:
        conexao.rollback()
        logger.error(f"[DATABASE] Erro removendo {tabela}: {erro}")
        raise
    finally:
        cursor.close()
        conexao.close()


def preparar_dataframe(df):
    """Ajusta tipos do Spark para bater com PostgreSQL."""
    if "steam_app_id" in df.columns:
        df = df.withColumn("steam_app_id", col("steam_app_id").cast("integer"))
    if "data_ingestao" in df.columns:
        df = df.withColumn("data_ingestao", col("data_ingestao").cast("date"))
    return df


def carregar_dataframe_postgres(df, tabela):
    logger.info(f"[DATABASE] Inserindo dados em {tabela}")
    df = preparar_dataframe(df)
    df.printSchema()

    jdbc_url = f"jdbc:postgresql://{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"

    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", tabela)
        .option("user", DATABASE_USER)
        .option("password", DATABASE_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def carregar_gold_postgres():
    logger.info("[DATABASE] Iniciando carga Gold -> PostgreSQL")

    spark = criar_spark()

    tabelas = [
        {"origem": "data/gold/historico_precos/",        "destino": "fato_historico_precos"},
        {"origem": "data/gold/ofertas/",                 "destino": "fato_ofertas"},
        {"origem": "data/gold/ranking_desenvolvedores/", "destino": "ranking_desenvolvedores"},
        {"origem": "data/gold/ranking_generos/",         "destino": "ranking_generos"},
    ]

    for tabela in tabelas:
        logger.info(f"[GOLD] Lendo {tabela['origem']}")

        df = spark.read.parquet(tabela["origem"])
        df = preparar_dataframe(df)
        df.printSchema()

        data_ingestao = str(df.select("data_ingestao").orderBy(col("data_ingestao").desc()).first()[0])
        logger.info(f"[GOLD] Snapshot encontrado: {data_ingestao}")

        deletar_particao(tabela["destino"], data_ingestao)
        carregar_dataframe_postgres(df, tabela["destino"])

        logger.info(f"[DATABASE] Carga concluída: {tabela['destino']}")

    spark.stop()
    logger.info("[DATABASE] Todas as tabelas Gold carregadas")


if __name__ == "__main__":
    carregar_gold_postgres()