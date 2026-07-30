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
            "GamePriceAnalytics-Gold-Ranking-Generos"
        ) \
        .getOrCreate()



def criar_gold_ranking_generos():

    """
    Cria ranking analítico de gêneros.

    Origem:

        Silver:
            jogos_precos
            jogos_generos


    Objetivo:

        Identificar quais gêneros possuem
        mais jogos e melhores promoções.


    Partição:

        data_ingestao
    """


    logger.info(
        "[GOLD] Iniciando ranking de gêneros"
    )


    spark = criar_spark()



    caminho_jogos = (
        "data/silver/jogos_precos/"
    )


    caminho_generos = (
        "data/silver/jogos_generos/"
    )


    caminho_gold = (
        "data/gold/ranking_generos/"
    )



    logger.info(
        "[SILVER] Lendo jogos"
    )


    jogos = spark.read.parquet(
        caminho_jogos
    )



    logger.info(
        "[SILVER] Lendo gêneros"
    )


    generos = spark.read.parquet(
        caminho_generos
    )



    logger.info(
        "[DEBUG] Schema jogos:"
    )

    jogos.printSchema()


    logger.info(
        "[DEBUG] Schema gêneros:"
    )

    generos.printSchema()



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
    # Remover duplicidade Silver
    # =========================


    generos = generos.dropDuplicates(
        [
            "steam_app_id",
            "genero_id"
        ]
    )



    # =========================
    # Join jogos + gêneros
    # =========================


    df = jogos.join(

        generos,

        "steam_app_id",

        "inner"

    )



    # =========================
    # Métricas analíticas
    # =========================


    ranking = df.groupBy(

        "genero_id",

        "genero_nome"

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
            avg(
                "preco_normal"
            ),
            2
        )
        .alias(
            "preco_medio_normal"
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
        f"[GOLD] Gêneros encontrados: {ranking.count()}"
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
            f"[GOLD] Removendo partição existente: {caminho_particao}"
        )


        shutil.rmtree(
            caminho_particao
        )



    # =========================
    # Salvar Gold
    # =========================


    logger.info(
        "[GOLD] Salvando ranking de gêneros"
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
        "[GOLD] Ranking de gêneros criado com sucesso"
    )



    spark.stop()



if __name__ == "__main__":

    criar_gold_ranking_generos()