from datetime import datetime
from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    round
)

from configuracao.logging import configurar_logger

logger = configurar_logger()


def criar_spark():

    return SparkSession.builder \
        .appName(
            "GamePriceAnalytics-Gold-Ofertas"
        ) \
        .getOrCreate()


def criar_gold_ofertas(data_ingestao):

    """
    Cria tabela Gold de ofertas.

    Origem:
        Silver jogos_precos

    Objetivo:
        Gerar visão analítica de promoções
        para consumo de negócio.

    Partição:
        data_ingestao

    Estratégia:
        Remove a partição do dia
        e recria o snapshot.
    """

    logger.info(
        "[GOLD] Iniciando criação tabela ofertas"
    )

    spark = criar_spark()

    caminho_silver = (
        f"data/silver/jogos_precos/"
        f"data_ingestao={data_ingestao}"
    )

    caminho_gold = (
        "data/gold/ofertas/"
    )

    logger.info(
        f"[SILVER] Lendo dados: {caminho_silver}"
    )

    df = spark.read.parquet(
        caminho_silver
    )

    logger.info(
        "[DEBUG] Schema Silver:"
    )

    df.printSchema()

    # =========================
    # Regras de negócio
    # =========================

    ofertas = df.select(

        "steam_app_id",

        "nome",

        "preco_oferta",

        "preco_normal",

        "desconto",

        "avaliacao",

        "avaliacao_percentual",

        "data_ingestao"

    )

    ofertas = ofertas.withColumn(

        "economia",

        round(

            col("preco_normal")
            -
            col("preco_oferta"),

            2

        )

    )

    ofertas = ofertas.withColumn(

        "nivel_oferta",

        when(

            col("desconto") >= 75,

            "EXCELENTE"

        )

        .when(

            col("desconto") >= 50,

            "BOA"

        )

        .when(

            col("desconto") >= 25,

            "MEDIA"

        )

        .otherwise(

            "BAIXA"

        )

    )

    ofertas = ofertas.withColumn(

        "jogo_gratis",

        when(

            col("preco_oferta") == 0,

            True

        )

        .otherwise(

            False

        )

    )

    logger.info(
        f"[GOLD] Atualizando partição: {data_ingestao}"
    )

    # =========================
    # Remove partição existente
    # =========================

    particao = Path(

        f"{caminho_gold}"
        f"data_ingestao={data_ingestao}"

    )

    if particao.exists():

        logger.info(
            f"[GOLD] Removendo partição existente: {particao}"
        )

        shutil.rmtree(
            particao
        )

    logger.info(

        f"[GOLD] Total ofertas: {ofertas.count()}"

    )

    # =========================
    # Salvar Gold
    # =========================

    logger.info(
        f"[GOLD] Salvando em {caminho_gold}"
    )

    ofertas.write \
        .mode("append") \
        .partitionBy(
            "data_ingestao"
        ) \
        .parquet(
            caminho_gold
        )

    logger.info(
        "[GOLD] gold_ofertas criada com sucesso"
    )

    spark.stop()


if __name__ == "__main__":

    data_ingestao = datetime.now().strftime(
        "%Y-%m-%d"
    )

    criar_gold_ofertas(
        data_ingestao
    )