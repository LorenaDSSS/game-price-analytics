import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from configuracao.config import HTTP_RETRY


def criar_sessao_http(
    backoff_factor=1
):
    """
    Cria uma sessão HTTP reutilizável
    com retry automático.
    """

    sessao = requests.Session()


    retry = Retry(
        total=HTTP_RETRY,

        backoff_factor=backoff_factor,

        status_forcelist=[
            429,
            500,
            502,
            503,
            504
        ],

        allowed_methods=[
            "GET"
        ]
    )


    adapter = HTTPAdapter(
        max_retries=retry
    )


    sessao.mount(
        "http://",
        adapter
    )


    sessao.mount(
        "https://",
        adapter
    )


    return sessao