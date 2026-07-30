from pathlib import Path
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    avg,
    max,
    round,
    lit
)

from configuracao.logging import configurar_logger

logger = configurar_logger()


def criar_spark():

    return SparkSession.builder \
        .appName(
            "GamePriceAnalytics-Gold-Ranking-Desenvolvedores"
        ) \
        .getOrCreate()



def criar_gold_ranking_desenvolvedores():

    """
    Cria ranking analítico de desenvolvedores.

    Origem:

        Silver:
            jogos_precos
            jogos_desenvolvedores


    Objetivo:

        Identificar desenvolvedores
        com maior presença e melhores ofertas.


    Partição:

        data_ingestao
    """

    logger.info(
        "[GOLD] Iniciando ranking desenvolvedores"
    )


    spark = criar_spark()


    caminho_jogos = (
        "data/silver/jogos_precos/"
    )


    caminho_desenvolvedores = (
        "data/silver/jogos_desenvolvedores/"
    )


    caminho_gold = (
        "data/gold/ranking_desenvolvedores/"
    )


    logger.info(
        "[SILVER] Lendo jogos"
    )


    jogos = spark.read.parquet(
        caminho_jogos
    )


    logger.info(
        "[SILVER] Lendo desenvolvedores"
    )


    desenvolvedores = spark.read.parquet(
        caminho_desenvolvedores
    )


    jogos.printSchema()

    desenvolvedores.printSchema()



    # =========================
    # Descobrir snapshot
    # =========================

    data_ingestao = (
        jogos
        .select(
            "data_ingestao"
        )
        .first()[0]
    )


    logger.info(
        f"[GOLD] Snapshot: {data_ingestao}"
    )



    # =========================
    # Join
    # =========================


    df = jogos.join(

        desenvolvedores,

        "steam_app_id",

        "inner"

    )



    # =========================
    # Métricas
    # =========================


    ranking = df.groupBy(

        "desenvolvedor_nome"

    ).agg(

        countDistinct(
            "steam_app_id"
        )
        .alias(
            "quantidade_jogos"
        ),


        round(
            avg(
                "desconto"
            ),
            2
        )
        .alias(
            "desconto_medio"
        ),


        round(
            avg(
                "preco_oferta"
            ),
            2
        )
        .alias(
            "preco_medio_oferta"
        ),


        round(
            max(
                "desconto"
            ),
            2
        )
        .alias(
            "maior_desconto"
        )

    )



    # =========================
    # Adicionar partição
    # =========================


    ranking = ranking.withColumn(

        "data_ingestao",

        lit(
            data_ingestao
        )

    )



    logger.info(
        f"[GOLD] Desenvolvedores: {ranking.count()}"
    )



    # =========================
    # Remover snapshot antigo
    # =========================


    caminho_particao = Path(

        f"{caminho_gold}"
        f"data_ingestao={data_ingestao}"

    )



    if caminho_particao.exists():

        logger.info(
            "[GOLD] Removendo partição antiga"
        )


        shutil.rmtree(
            caminho_particao
        )



    # =========================
    # Salvar Gold
    # =========================


    logger.info(
        "[GOLD] Salvando ranking desenvolvedores"
    )


    ranking.write \
        .mode("append") \
        .partitionBy(
            "data_ingestao"
        ) \
        .parquet(
            caminho_gold
        )



    logger.info(
        "[GOLD] Ranking desenvolvedores criado"
    )


    spark.stop()



if __name__ == "__main__":

    criar_gold_ranking_desenvolvedores()