import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import requests

from configuracao.config import CHEAPSHARK_API_URL, HTTP_TIMEOUT
from configuracao.http import criar_sessao_http
from configuracao.logging import configurar_logger

logger = configurar_logger()
sessao = criar_sessao_http()


def buscar_ofertas_cheapshark():
    """Busca ofertas de jogos na CheapShark API. Retorna o JSON bruto recebido."""

    logger.info("[CHEAPSHARK] Iniciando consulta de ofertas")

    try:
        response = sessao.get(
            f"{CHEAPSHARK_API_URL}/deals",
            headers={"User-Agent": "GamePriceAnalytics/1.0"},
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        dados = response.json()
        logger.info(f"[CHEAPSHARK] Ofertas recebidas: {len(dados)}")
        return dados

    except requests.exceptions.Timeout as erro:
        logger.error(f"[CHEAPSHARK] Timeout após {HTTP_TIMEOUT}s: {erro}")
        raise

    except requests.exceptions.HTTPError as erro:
        logger.error(f"[CHEAPSHARK] Erro HTTP: {erro}")
        raise

    except requests.exceptions.RequestException as erro:
        logger.error(f"[CHEAPSHARK] Erro de conexão: {erro}")
        raise


def buscar_lojas_cheapshark():
    """Busca o mapeamento de storeID → storeName da CheapShark API."""

    logger.info("[CHEAPSHARK] Buscando lista de lojas")

    try:
        response = sessao.get(
            f"{CHEAPSHARK_API_URL}/stores",
            headers={"User-Agent": "GamePriceAnalytics/1.0"},
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        lojas = {str(loja["storeID"]): loja["storeName"] for loja in response.json()}
        logger.info(f"[CHEAPSHARK] Lojas mapeadas: {len(lojas)}")
        return lojas

    except Exception as erro:
        logger.warning(f"[CHEAPSHARK] Erro ao buscar lojas, usando mapeamento padrão: {erro}")
        return {
            "1": "Steam", "2": "GamersGate", "3": "GreenManGaming",
            "7": "GOG", "8": "Origin", "11": "Humble Store",
            "13": "Fanatical", "15": "Gamesplanet", "21": "WinGameStore",
            "23": "GameBillet", "24": "Voidu", "25": "Epic Games Store",
            "27": "Games Republic", "28": "Gamesload", "29": "2Game",
        }