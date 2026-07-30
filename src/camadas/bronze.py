import json
import uuid

from datetime import datetime
from pathlib import Path

from configuracao.logging import configurar_logger

logger = configurar_logger()


PIPELINE_VERSION = "1.0.0"
AMBIENTE = "dev"


# =====================================================
# Bronze RAW
# =====================================================

def salvar_bronze_raw(resultado):
    """
    Salva o dado bruto recebido da CheapShark API.

    Camada:
        Bronze RAW

    Responsabilidade:
        - Guardar dado original
        - Não aplicar transformação
        - Garantir rastreabilidade
    """

    data_ingestao = datetime.now().strftime(
        "%Y-%m-%d"
    )

    timestamp_ingestao = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    execution_id = str(
        uuid.uuid4()
    )

    caminho = Path(
        f"data/bronze/raw/cheapshark/"
        f"data_ingestao={data_ingestao}/"
        f"cheapshark.json"
    )

    caminho.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    raw = {

        "metadata": {

            "camada": "bronze_raw",

            "pipeline_version": PIPELINE_VERSION,

            "execution_id": execution_id,

            "ambiente": AMBIENTE,

            "data_ingestao": data_ingestao,

            "timestamp_ingestao": timestamp_ingestao,

            "origem": "CheapShark API",

            "total_registros": len(
                resultado["raw_cheapshark"]
            )

        },

        "dados": resultado["raw_cheapshark"]

    }

    try:

        with open(
            caminho,
            "w",
            encoding="utf-8"
        ) as arquivo:

            json.dump(
                raw,
                arquivo,
                indent=4,
                ensure_ascii=False
            )

        logger.info(
            f"[BRONZE RAW] Arquivo salvo: {caminho}"
        )

    except Exception as erro:

        logger.error(
            f"[BRONZE RAW] Erro salvando arquivo: {erro}"
        )

        raise


# =====================================================
# Bronze ENRICHED
# =====================================================

def salvar_bronze_enriched(resultado):
    """
    Salva dados enriquecidos.

    Origem:

        CheapShark API
                +
        Steam API

    Camada:
        Bronze ENRICHED

    Responsabilidade:
        - Manter dado enriquecido
        - Ainda sem regra de negócio
        - Preparar para Silver
    """

    data_ingestao = datetime.now().strftime(
        "%Y-%m-%d"
    )

    timestamp_ingestao = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    execution_id = str(
        uuid.uuid4()
    )

    caminho = Path(
        f"data/bronze/enriched/"
        f"jogos_precos/"
        f"data_ingestao={data_ingestao}/"
        f"jogos_precos.json"
    )

    caminho.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    enriched = {

        "metadata": {

            "camada": "bronze_enriched",

            "pipeline_version": PIPELINE_VERSION,

            "execution_id": execution_id,

            "ambiente": AMBIENTE,

            "data_ingestao": data_ingestao,

            "timestamp_ingestao": timestamp_ingestao,

            "fontes": [
                "CheapShark API",
                "Steam API"
            ],

            "total_ofertas_recebidas":
                resultado["total_ofertas"],

            "total_jogos_processados":
                len(resultado["dados"]),

            "total_registros":
                len(resultado["dados"])

        },

        "dados": resultado["dados"]

    }

    try:

        with open(
            caminho,
            "w",
            encoding="utf-8"
        ) as arquivo:

            json.dump(
                enriched,
                arquivo,
                indent=4,
                ensure_ascii=False
            )

        logger.info(
            f"[BRONZE ENRICHED] Arquivo salvo: {caminho}"
        )

    except Exception as erro:

        logger.error(
            f"[BRONZE ENRICHED] Erro salvando arquivo: {erro}"
        )

        raise