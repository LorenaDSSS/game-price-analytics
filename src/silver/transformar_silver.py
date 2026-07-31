import sys
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, explode, lit, round

from configuracao.logging import configurar_logger

logger = configurar_logger()


def criar_spark():
    return (
        SparkSession.builder
        .appName("GamePriceAnalytics-Transformacao")
        .getOrCreate()
    )


def remover_particao(caminho):
    caminho = Path(caminho)
    if caminho.exists():
        logger.info(f"[SILVER] Removendo partição existente: {caminho}")
        shutil.rmtree(caminho)


def transformar_bronze_para_silver():
    """Lê Bronze Enriched, trata dados, aplica regras de qualidade e salva Silver."""

    logger.info("[SILVER] Iniciando transformação Bronze -> Silver")

    spark = criar_spark()

    try:
        data_ingestao = datetime.now().strftime("%Y-%m-%d")
        timestamp_processamento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        caminho_bronze = f"data/bronze/enriched/jogos_precos/data_ingestao={data_ingestao}/jogos_precos.json"
        caminho_silver = "data/silver/"

        logger.info(f"[BRONZE] Lendo arquivo: {caminho_bronze}")

        df = spark.read.option("multiline", "true").json(caminho_bronze)

        logger.info("[DEBUG] Schema original Bronze:")
        df.printSchema()

        # Controle de ingestão
        df = (
            df
            .withColumn("data_ingestao", lit(data_ingestao))
            .withColumn("timestamp_processamento", lit(timestamp_processamento))
        )

        # Explode jogos
        logger.info("[BRONZE] Expandindo array de jogos")
        df = df.select(explode(col("dados")).alias("jogo"), "data_ingestao", "timestamp_processamento")
        df = df.select("jogo.*", "data_ingestao", "timestamp_processamento")

        logger.info("[DEBUG] Schema após explode:")
        df.printSchema()

        # ── Jogos Silver ──────────────────────────────────────────────────────
        logger.info("[SILVER] Tratando jogos")

        jogos = df.select(
            "steam_app_id",
            "nome",
            # CheapShark
            col("preco_oferta").cast("double").alias("preco_oferta"),
            col("preco_normal").cast("double").alias("preco_normal"),
            lit("USD").alias("moeda_oferta"),  # CheapShark sempre em USD
            col("desconto").cast("double").alias("desconto"),
            "avaliacao",
            col("avaliacao_percentual").cast("integer").alias("avaliacao_percentual"),
            "loja",
            # Steam
            round(when(col("preco_steam.final").isNull(), None).otherwise(col("preco_steam.final") / 100), 2).alias("preco_steam"),
            round(when(col("preco_steam.initial").isNull(), None).otherwise(col("preco_steam.initial") / 100), 2).alias("preco_steam_original"),
            col("preco_steam.currency").alias("moeda_steam"),
            col("preco_steam.discount_percent").cast("double").alias("desconto_steam"),
            "data_ingestao",
            "timestamp_processamento",
        )

        jogos = (
            jogos
            .withColumn("nome", trim(col("nome")))
            .withColumn("nome", when(col("nome").isNull(), "DESCONHECIDO").otherwise(col("nome")))
        )

        logger.info("[SILVER] Aplicando filtros de qualidade")
        jogos = (
            jogos
            .filter(col("steam_app_id").isNotNull())
            .filter(col("moeda_steam").isin("BRL", "USD"))  # somente moedas aceitas
            .dropDuplicates(["steam_app_id", "data_ingestao"])
        )

        # ── Gêneros Silver ────────────────────────────────────────────────────
        logger.info("[SILVER] Tratando gêneros")

        generos = df.select("steam_app_id", explode(col("generos")).alias("genero"), "data_ingestao", "timestamp_processamento")
        generos = generos.select(
            "steam_app_id",
            col("genero.id").cast("integer").alias("genero_id"),
            col("genero.description").alias("genero_nome"),
            "data_ingestao",
            "timestamp_processamento",
        )
        generos = (
            generos
            .withColumn("genero_nome", trim(col("genero_nome")))
            .withColumn("genero_nome", when(col("genero_nome").isNull(), "DESCONHECIDO").otherwise(col("genero_nome")))
            .filter(col("steam_app_id").isNotNull())
            .dropDuplicates()
        )

        # ── Desenvolvedores Silver ────────────────────────────────────────────
        logger.info("[SILVER] Tratando desenvolvedores")

        desenvolvedores = df.select("steam_app_id", explode(col("desenvolvedores")).alias("desenvolvedor"), "data_ingestao", "timestamp_processamento")
        desenvolvedores = desenvolvedores.select(
            "steam_app_id",
            col("desenvolvedor").alias("desenvolvedor_nome"),
            "data_ingestao",
            "timestamp_processamento",
        )
        desenvolvedores = (
            desenvolvedores
            .withColumn("desenvolvedor_nome", trim(col("desenvolvedor_nome")))
            .withColumn("desenvolvedor_nome", when(col("desenvolvedor_nome").isNull(), "DESCONHECIDO").otherwise(col("desenvolvedor_nome")))
            .filter(col("steam_app_id").isNotNull())
            .dropDuplicates()
        )

        # Contagens
        logger.info(f"[SILVER] Jogos: {jogos.count()}")
        logger.info(f"[SILVER] Gêneros: {generos.count()}")
        logger.info(f"[SILVER] Desenvolvedores: {desenvolvedores.count()}")

        # Limpeza de partições existentes
        remover_particao(f"{caminho_silver}jogos_precos/data_ingestao={data_ingestao}")
        remover_particao(f"{caminho_silver}jogos_generos/data_ingestao={data_ingestao}")
        remover_particao(f"{caminho_silver}jogos_desenvolvedores/data_ingestao={data_ingestao}")

        # Escrita Silver
        logger.info("[SILVER] Salvando dados")
        jogos.write.mode("overwrite").parquet(f"{caminho_silver}jogos_precos/data_ingestao={data_ingestao}")
        generos.write.mode("overwrite").parquet(f"{caminho_silver}jogos_generos/data_ingestao={data_ingestao}")
        desenvolvedores.write.mode("overwrite").parquet(f"{caminho_silver}jogos_desenvolvedores/data_ingestao={data_ingestao}")

        logger.info("[SILVER] Transformação concluída com sucesso")

    finally:
        spark.stop()


if __name__ == "__main__":
    transformar_bronze_para_silver()