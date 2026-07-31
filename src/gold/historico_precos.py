import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round

from configuracao.logging import configurar_logger

logger = configurar_logger()


def criar_spark():
    return SparkSession.builder.appName("GamePriceAnalytics-Gold-Historico").getOrCreate()


def criar_gold_historico_precos(data_ingestao):
    """
    Snapshot diário de preços a partir de Silver jogos_precos.
    Partição: data_ingestao (reprocessável no mesmo dia).
    """

    logger.info("[GOLD] Iniciando histórico de preços")

    spark = criar_spark()
    caminho_silver = f"data/silver/jogos_precos/data_ingestao={data_ingestao}"
    caminho_gold = "data/gold/historico_precos/"

    logger.info(f"[SILVER] Lendo: {caminho_silver}")
    df = spark.read.parquet(caminho_silver)
    df.printSchema()

    historico = (
        df.select("steam_app_id", "nome", "preco_oferta", "preco_normal", "preco_steam", "moeda_steam", "desconto", "data_ingestao")
        .withColumn("preco_oferta", round(col("preco_oferta"), 2))
        .withColumn("preco_normal", round(col("preco_normal"), 2))
        .withColumn("preco_steam", round(col("preco_steam"), 2))
        .withColumn("desconto", round(col("desconto"), 2))
        .dropDuplicates(["steam_app_id", "data_ingestao"])
    )

    logger.info(f"[GOLD] Snapshot: {data_ingestao} | Registros: {historico.count()}")

    caminho_particao = Path(f"{caminho_gold}data_ingestao={data_ingestao}")
    if caminho_particao.exists():
        logger.info(f"[GOLD] Removendo partição existente: {caminho_particao}")
        shutil.rmtree(caminho_particao)

    historico.write.mode("append").partitionBy("data_ingestao").parquet(caminho_gold)
    logger.info("[GOLD] Histórico de preços criado com sucesso")

    spark.stop()


if __name__ == "__main__":
    criar_gold_historico_precos("2026-07-30")