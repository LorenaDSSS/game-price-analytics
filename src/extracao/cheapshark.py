import requests

from configuracao.config import (
    CHEAPSHARK_API_URL,
    HTTP_TIMEOUT
)

from configuracao.http import criar_sessao_http

from configuracao.logging import configurar_logger


logger = configurar_logger()


sessao = criar_sessao_http()



def buscar_ofertas_cheapshark():
    """
    Busca ofertas de jogos na CheapShark API.
    Retorna o JSON bruto recebido.
    """


    logger.info(
        "[CHEAPSHARK] Iniciando consulta de ofertas"
    )


    endpoint = f"{CHEAPSHARK_API_URL}/deals"


    headers = {
        "User-Agent": "GamePriceAnalytics/1.0"
    }



    try:


        response = sessao.get(
            endpoint,
            headers=headers,
            timeout=HTTP_TIMEOUT
        )


        response.raise_for_status()


        dados = response.json()


        logger.info(
            f"[CHEAPSHARK] Ofertas recebidas: {len(dados)}"
        )


        return dados



    except requests.exceptions.Timeout as erro:


        logger.error(
            f"[CHEAPSHARK] Timeout após {HTTP_TIMEOUT}s: {erro}"
        )


        raise



    except requests.exceptions.HTTPError as erro:


        logger.error(
            f"[CHEAPSHARK] Erro HTTP: {erro}"
        )


        raise



    except requests.exceptions.RequestException as erro:


        logger.error(
            f"[CHEAPSHARK] Erro de conexão: {erro}"
        )


        raise