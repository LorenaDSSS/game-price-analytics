import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, avg, max, round, lit, when

from configuracao.logging import configurar_logger

logger = configurar_logger()


def criar_spark():
    return SparkSession.builder.appName("GamePriceAnalytics-Gold-Ranking-Desenvolvedores").getOrCreate()


def criar_gold_ranking_desenvolvedores(data_ingestao):
    """
    Ranking analítico de desenvolvedores (presença e melhores ofertas).
    Origens: Silver jogos_precos + jogos_desenvolvedores. Partição: data_ingestao.
    """

    logger.info("[GOLD] Iniciando ranking desenvolvedores")

    spark = criar_spark()
    caminho_jogos = f"data/silver/jogos_precos/data_ingestao={data_ingestao}"
    caminho_desenvolvedores = f"data/silver/jogos_desenvolvedores/data_ingestao={data_ingestao}"
    caminho_gold = "data/gold/ranking_desenvolvedores/"

    jogos = spark.read.parquet(caminho_jogos)
    desenvolvedores = spark.read.parquet(caminho_desenvolvedores)

    jogos.printSchema()
    desenvolvedores.printSchema()

    jogos = jogos.dropDuplicates(["steam_app_id"])
    desenvolvedores = desenvolvedores.dropDuplicates(["steam_app_id", "desenvolvedor_nome"])

    logger.info("[GOLD] Realizando join")
    df = jogos.join(desenvolvedores, "steam_app_id", "inner")
    df = df.withColumn("desenvolvedor_nome", when(col("desenvolvedor_nome").isNull(), "DESCONHECIDO").otherwise(col("desenvolvedor_nome")))

    ranking = (
        df.groupBy("desenvolvedor_nome")
        .agg(
            countDistinct("steam_app_id").alias("quantidade_jogos"),
            round(avg("desconto"), 2).alias("desconto_medio"),
            round(avg("preco_oferta"), 2).alias("preco_medio_oferta"),
            round(avg("preco_normal"), 2).alias("preco_medio_normal"),
            round(max("desconto"), 2).alias("maior_desconto"),
            round(avg("avaliacao_percentual"), 1).alias("avaliacao_media"),
        )
        .withColumn("data_ingestao", lit(data_ingestao))
    )

    logger.info(f"[GOLD] Desenvolvedores: {ranking.count()}")

    caminho_particao = Path(f"{caminho_gold}data_ingestao={data_ingestao}")
    if caminho_particao.exists():
        logger.info(f"[GOLD] Removendo partição antiga: {caminho_particao}")
        shutil.rmtree(caminho_particao)

    ranking.write.mode("append").partitionBy("data_ingestao").parquet(caminho_gold)
    logger.info("[GOLD] Ranking desenvolvedores criado com sucesso")

    spark.stop()


if __name__ == "__main__":
    criar_gold_ranking_desenvolvedores("2026-07-30")