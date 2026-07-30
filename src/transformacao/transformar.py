from datetime import datetime
from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    when,
    explode,
    lit
)

from configuracao.logging import configurar_logger

logger = configurar_logger()


def criar_spark():

    return SparkSession.builder \
        .appName("GamePriceAnalytics-Transformacao") \
        .getOrCreate()


def remover_particao(caminho):

    caminho = Path(caminho)

    if caminho.exists():

        logger.info(
            f"[SILVER] Removendo partição existente: {caminho}"
        )

        shutil.rmtree(caminho)


def transformar_bronze_para_silver():

    """
    Lê Bronze Enriched,
    transforma dados
    e salva Silver.
    """

    logger.info(
        "[SILVER] Iniciando transformação Bronze -> Silver"
    )

    spark = criar_spark()

    data_ingestao = datetime.now().strftime(
        "%Y-%m-%d"
    )

    timestamp_processamento = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    caminho_bronze = (
        f"data/bronze/enriched/"
        f"jogos_precos/"
        f"data_ingestao={data_ingestao}/"
        f"jogos_precos.json"
    )

    caminho_silver = (
        "data/silver/"
    )

    logger.info(
        f"[BRONZE] Lendo arquivo: {caminho_bronze}"
    )

    df = spark.read \
        .option(
            "multiline",
            "true"
        ) \
        .json(
            caminho_bronze
        )

    logger.info(
        "[DEBUG] Schema original Bronze:"
    )

    df.printSchema()

    # =====================================================
    # Controle de ingestão
    # =====================================================

    df = df.withColumn(
        "data_ingestao",
        lit(data_ingestao)
    )

    df = df.withColumn(
        "timestamp_processamento",
        lit(timestamp_processamento)
    )

    # =====================================================
    # Expandir array dados
    # =====================================================

    logger.info(
        "[BRONZE] Expandindo array de jogos"
    )

    df = df.select(
        explode(
            col("dados")
        ).alias("jogo"),
        "data_ingestao",
        "timestamp_processamento"
    )

    df = df.select(
        "jogo.*",
        "data_ingestao",
        "timestamp_processamento"
    )

    logger.info(
        "[DEBUG] Schema após explode:"
    )

    df.printSchema()

    # =====================================================
    # Silver Jogos
    # =====================================================

    jogos = df.select(
        "steam_app_id",
        "nome",
        "preco_oferta",
        "preco_normal",
        "desconto",
        "avaliacao",
        "avaliacao_percentual",
        "preco_steam",
        "data_ingestao",
        "timestamp_processamento"
    )

    jogos = jogos.withColumn(
        "nome",
        trim(col("nome"))
    )

    jogos = jogos.withColumn(
        "nome",
        when(
            col("nome").isNull(),
            "DESCONHECIDO"
        ).otherwise(
            col("nome")
        )
    )

    jogos = jogos.withColumn(
        "preco_oferta",
        col("preco_oferta").cast("double")
    )

    jogos = jogos.withColumn(
        "preco_normal",
        col("preco_normal").cast("double")
    )

    jogos = jogos.withColumn(
        "desconto",
        col("desconto").cast("double")
    )

    jogos = jogos.withColumn(
        "avaliacao_percentual",
        col("avaliacao_percentual").cast("integer")
    )

    jogos = jogos.dropDuplicates(
        [
            "steam_app_id"
        ]
    )

    # =====================================================
    # Silver Gêneros
    # =====================================================

    generos = df.select(
        "steam_app_id",
        explode(
            col("generos")
        ).alias("genero"),
        "data_ingestao",
        "timestamp_processamento"
    )

    generos = generos.select(
        "steam_app_id",
        col("genero.id").alias("genero_id"),
        col("genero.description").alias("genero_nome"),
        "data_ingestao",
        "timestamp_processamento"
    )

    generos = generos.dropDuplicates()

    # =====================================================
    # Silver Desenvolvedores
    # =====================================================

    desenvolvedores = df.select(
        "steam_app_id",
        explode(
            col("desenvolvedores")
        ).alias("desenvolvedor"),
        "data_ingestao",
        "timestamp_processamento"
    )

    desenvolvedores = desenvolvedores.select(
        "steam_app_id",
        col("desenvolvedor").alias(
            "desenvolvedor_nome"
        ),
        "data_ingestao",
        "timestamp_processamento"
    )

    desenvolvedores = desenvolvedores.dropDuplicates()

    logger.info(
        f"[SILVER] Jogos: {jogos.count()}"
    )

    logger.info(
        f"[SILVER] Gêneros: {generos.count()}"
    )

    logger.info(
        f"[SILVER] Desenvolvedores: {desenvolvedores.count()}"
    )

    # =====================================================
    # Remover partições existentes
    # =====================================================

    remover_particao(
        f"{caminho_silver}jogos_precos/data_ingestao={data_ingestao}"
    )

    remover_particao(
        f"{caminho_silver}jogos_generos/data_ingestao={data_ingestao}"
    )

    remover_particao(
        f"{caminho_silver}jogos_desenvolvedores/data_ingestao={data_ingestao}"
    )

    # =====================================================
    # Salvar Silver
    # =====================================================

    logger.info(
        "[SILVER] Salvando tabelas Silver"
    )

    jogos.write \
        .mode("overwrite") \
        .parquet(
            f"{caminho_silver}"
            f"jogos_precos/"
            f"data_ingestao={data_ingestao}"
        )

    generos.write \
        .mode("overwrite") \
        .parquet(
            f"{caminho_silver}"
            f"jogos_generos/"
            f"data_ingestao={data_ingestao}"
        )

    desenvolvedores.write \
        .mode("overwrite") \
        .parquet(
            f"{caminho_silver}"
            f"jogos_desenvolvedores/"
            f"data_ingestao={data_ingestao}"
        )

    logger.info(
        "[SILVER] Transformação concluída com sucesso"
    )

    spark.stop()


if __name__ == "__main__":

    transformar_bronze_para_silver()