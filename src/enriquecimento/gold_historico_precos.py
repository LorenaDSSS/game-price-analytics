from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    round
)

from configuracao.logging import configurar_logger

logger = configurar_logger()


def criar_spark():

    return SparkSession.builder \
        .appName(
            "GamePriceAnalytics-Gold-Historico"
        ) \
        .getOrCreate()


def criar_gold_historico_precos(data_ingestao):

    """
    Cria histórico de preços.

    Origem:
        Silver jogos_precos

    Estratégia:
        Snapshot diário de preços.

    Regra:
        Cada data_ingestao possui uma única partição.

        Caso execute novamente no mesmo dia:
            remove a partição existente
            e recria o snapshot.

    Partição:
        data_ingestao
    """

    logger.info(
        "[GOLD] Iniciando histórico de preços"
    )

    spark = criar_spark()

    caminho_silver = (
        f"data/silver/jogos_precos/"
        f"data_ingestao={data_ingestao}"
    )

    caminho_gold = (
        "data/gold/historico_precos/"
    )

    logger.info(
        f"[SILVER] Lendo: {caminho_silver}"
    )

    df = spark.read.parquet(
        caminho_silver
    )

    logger.info(
        "[DEBUG] Schema Silver:"
    )

    df.printSchema()

    # =========================
    # Criar snapshot histórico
    # =========================

    historico = df.select(

        "steam_app_id",

        "nome",

        "preco_oferta",

        "preco_normal",

        "desconto",

        "data_ingestao"

    )

    # =========================
    # Normalização
    # =========================

    historico = historico.withColumn(

        "preco_oferta",

        round(
            col("preco_oferta"),
            2
        )

    )

    historico = historico.withColumn(

        "preco_normal",

        round(
            col("preco_normal"),
            2
        )

    )

    historico = historico.withColumn(

        "desconto",

        round(
            col("desconto"),
            2
        )

    )

    # =========================
    # Remover duplicidade
    # =========================

    historico = historico.dropDuplicates(

        [
            "steam_app_id",
            "data_ingestao"
        ]

    )

    logger.info(
        f"[GOLD] Gerando snapshot: {data_ingestao}"
    )

    logger.info(
        f"[GOLD] Registros: {historico.count()}"
    )

    # =========================
    # Delete da partição antiga
    # =========================

    caminho_particao = Path(
        f"{caminho_gold}/data_ingestao={data_ingestao}"
    )

    if caminho_particao.exists():

        logger.info(
            f"[GOLD] Removendo partição existente: {caminho_particao}"
        )

        shutil.rmtree(
            caminho_particao
        )

    # =========================
    # Salvar snapshot
    # =========================

    logger.info(
        "[GOLD] Salvando snapshot"
    )

    historico.write \
        .mode("append") \
        .partitionBy(
            "data_ingestao"
        ) \
        .parquet(
            caminho_gold
        )

    logger.info(
        "[GOLD] Histórico de preços criado com sucesso"
    )

    spark.stop()


if __name__ == "__main__":

    criar_gold_historico_precos(
        "2026-07-30"
    )