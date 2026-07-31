import sys
from datetime import datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round

from configuracao.logging import configurar_logger

logger = configurar_logger()


def criar_spark():
    return SparkSession.builder.appName("GamePriceAnalytics-Gold-Ofertas").getOrCreate()


def criar_gold_ofertas(data_ingestao):
    """
    Visão analítica de promoções a partir de Silver jogos_precos.
    Partição: data_ingestao (reprocessável no mesmo dia).
    """

    logger.info("[GOLD] Iniciando criação tabela ofertas")

    spark = criar_spark()
    caminho_silver = f"data/silver/jogos_precos/data_ingestao={data_ingestao}"
    caminho_gold = "data/gold/ofertas/"

    logger.info(f"[SILVER] Lendo dados: {caminho_silver}")
    df = spark.read.parquet(caminho_silver)
    df.printSchema()

    ofertas = (
        df.select("steam_app_id", "nome", "preco_oferta", "preco_normal", "moeda_oferta", "desconto", "avaliacao", "avaliacao_percentual", "loja", "preco_steam", "preco_steam_original", "moeda_steam", "data_ingestao")
        .withColumn("preco_oferta", col("preco_oferta").cast("double"))
        .withColumn("preco_normal", col("preco_normal").cast("double"))
        .withColumn("desconto", col("desconto").cast("double"))
        .withColumn("avaliacao_percentual", col("avaliacao_percentual").cast("integer"))
        # economia só faz sentido quando preco_oferta e preco_steam estão na mesma moeda (USD)
        .withColumn("economia", when(col("moeda_steam") == "USD",
            round(col("preco_steam") - col("preco_oferta"), 2)
        ).otherwise(None))
        .withColumn("nivel_oferta",
            when(col("desconto") >= 75, "EXCELENTE")
            .when(col("desconto") >= 50, "BOA")
            .when(col("desconto") >= 25, "MEDIA")
            .otherwise("BAIXA")
        )
        .dropDuplicates(["steam_app_id", "data_ingestao"])
    )

    logger.info(f"[GOLD] Total ofertas: {ofertas.count()}")

    particao = Path(f"{caminho_gold}data_ingestao={data_ingestao}")
    if particao.exists():
        logger.info(f"[GOLD] Removendo partição existente: {particao}")
        shutil.rmtree(particao)

    ofertas.write.mode("append").partitionBy("data_ingestao").parquet(caminho_gold)
    logger.info("[GOLD] gold_ofertas criada com sucesso")

    spark.stop()


if __name__ == "__main__":
    criar_gold_ofertas(datetime.now().strftime("%Y-%m-%d"))