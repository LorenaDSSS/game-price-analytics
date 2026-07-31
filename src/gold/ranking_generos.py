import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, avg, max, round, lit, desc

from configuracao.logging import configurar_logger

logger = configurar_logger()


def criar_spark():
    return SparkSession.builder.appName("GamePriceAnalytics-Gold-Ranking-Generos").getOrCreate()


def criar_gold_ranking_generos():
    """
    Ranking analítico de gêneros (jogos e melhores promoções).
    Origens: Silver jogos_precos + jogos_generos. Partição: data_ingestao (snapshot mais recente).
    """

    logger.info("[GOLD] Iniciando ranking de gêneros")

    spark = criar_spark()
    caminho_jogos = "data/silver/jogos_precos/"
    caminho_generos = "data/silver/jogos_generos/"
    caminho_gold = "data/gold/ranking_generos/"

    jogos = spark.read.parquet(caminho_jogos)
    generos = spark.read.parquet(caminho_generos)

    jogos.printSchema()
    generos.printSchema()

    # Usa o snapshot mais recente disponível
    data_ingestao = jogos.select("data_ingestao").orderBy(desc("data_ingestao")).first()[0]
    logger.info(f"[GOLD] Snapshot utilizado: {data_ingestao}")

    jogos = jogos.filter(col("data_ingestao") == data_ingestao).dropDuplicates(["steam_app_id"])
    generos = generos.filter(col("data_ingestao") == data_ingestao).dropDuplicates(["steam_app_id", "genero_id"])

    df = jogos.join(generos, "steam_app_id", "inner")

    ranking = (
        df.groupBy("genero_id", "genero_nome")
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

    logger.info(f"[GOLD] Gêneros encontrados: {ranking.count()}")

    caminho_particao = Path(f"{caminho_gold}data_ingestao={data_ingestao}")
    if caminho_particao.exists():
        logger.info(f"[GOLD] Removendo partição existente: {caminho_particao}")
        shutil.rmtree(caminho_particao)

    ranking.write.mode("append").partitionBy("data_ingestao").parquet(caminho_gold)
    logger.info("[GOLD] Ranking de gêneros criado com sucesso")

    spark.stop()


if __name__ == "__main__":
    criar_gold_ranking_generos()