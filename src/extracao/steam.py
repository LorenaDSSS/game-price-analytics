import requests

from configuracao.config import (
    STEAM_API_URL,
    HTTP_TIMEOUT
)

from configuracao.http import criar_sessao_http

from configuracao.logging import configurar_logger


logger = configurar_logger()


sessao = criar_sessao_http()



def buscar_detalhes_steam(steam_app_id):
    """
    Busca detalhes de um jogo na Steam Store API.
    """


    logger.info(
        f"[STEAM] Buscando detalhes do jogo {steam_app_id}"
    )


    params = {
        "appids": steam_app_id
    }



    try:


        response = sessao.get(
            STEAM_API_URL,
            params=params,
            timeout=HTTP_TIMEOUT
        )


        response.raise_for_status()


        return response.json()



    except requests.exceptions.Timeout as erro:


        logger.warning(
            f"[STEAM] Timeout para App ID {steam_app_id}: {erro}"
        )


        return {}



    except requests.exceptions.HTTPError as erro:


        logger.warning(
            f"[STEAM] Erro HTTP para App ID {steam_app_id}: {erro}"
        )


        return {}



    except requests.exceptions.RequestException as erro:


        logger.warning(
            f"[STEAM] Erro consultando App ID {steam_app_id}: {erro}"
        )


        return {}