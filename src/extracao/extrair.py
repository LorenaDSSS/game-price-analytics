from configuracao.logging import configurar_logger

from extracao.cheapshark import (
    buscar_ofertas_cheapshark
)

from extracao.steam import (
    buscar_detalhes_steam
)

from camadas.bronze import (
    salvar_bronze_raw,
    salvar_bronze_enriched
)


logger = configurar_logger()


# =========================
# Extração principal
# =========================

def executar_extracao():

    """
    Executa extração e enriquecimento dos dados.

    Responsabilidade:
    - Buscar dados das APIs
    - Validar registros
    - Enriquecer dados da CheapShark com informações da Steam

    Fluxo:

    CheapShark API
          ↓
    Dados brutos de ofertas
          ↓
    Steam API
          ↓
    Enriquecimento dos jogos
          ↓
    Dados preparados para camada Bronze
    """


    logger.info(
        "[EXTRACAO] Iniciando processo de extração de dados"
    )


    logger.info(
        "[CHEAPSHARK] Buscando ofertas de jogos"
    )


    ofertas = buscar_ofertas_cheapshark()


    logger.info(
        f"[CHEAPSHARK] Dados recebidos: {len(ofertas)} ofertas"
    )


    jogos = []


    steam_ids_processados = set()


    contador_sem_steam = 0
    contador_duplicados = 0
    contador_erros_steam = 0


    logger.info(
        "[ENRIQUECIMENTO] Iniciando cruzamento CheapShark + Steam"
    )


    for oferta in ofertas:


        steam_app_id = oferta.get(
            "steamAppID"
        )


        if not steam_app_id:


            contador_sem_steam += 1


            logger.warning(
                "[VALIDACAO] Oferta ignorada: Steam App ID ausente"
            )


            continue



        if steam_app_id in steam_ids_processados:


            contador_duplicados += 1


            logger.info(
                f"[VALIDACAO] Jogo duplicado ignorado: {steam_app_id}"
            )


            continue



        steam_ids_processados.add(
            steam_app_id
        )


        detalhes = buscar_detalhes_steam(
            steam_app_id
        )


        jogo_steam = detalhes.get(
            steam_app_id
        )


        if not jogo_steam:


            contador_erros_steam += 1


            logger.warning(
                f"[STEAM] Sem retorno para App ID {steam_app_id}"
            )


            continue



        if not jogo_steam.get(
            "success"
        ):


            contador_erros_steam += 1


            logger.warning(
                f"[STEAM] Retorno inválido para App ID {steam_app_id}"
            )


            continue



        dados = jogo_steam["data"]



        jogo = {


            "steam_app_id": steam_app_id,


            "nome": dados.get(
                "name"
            ),


            "preco_steam": dados.get(
                "price_overview"
            ),


            "preco_oferta": oferta.get(
                "salePrice"
            ),


            "preco_normal": oferta.get(
                "normalPrice"
            ),


            "desconto": oferta.get(
                "savings"
            ),


            "avaliacao": oferta.get(
                "steamRatingText"
            ),


            "avaliacao_percentual": oferta.get(
                "steamRatingPercent"
            ),


            "generos": dados.get(
                "genres"
            ),


            "desenvolvedores": dados.get(
                "developers"
            )

        }


        jogos.append(
            jogo
        )



    logger.info(
        f"[ENRIQUECIMENTO] Jogos processados com sucesso: {len(jogos)}"
    )


    logger.info(
        f"[VALIDACAO] Registros sem Steam ID: {contador_sem_steam}"
    )


    logger.info(
        f"[VALIDACAO] Jogos duplicados ignorados: {contador_duplicados}"
    )


    logger.info(
        f"[VALIDACAO] Erros consulta Steam: {contador_erros_steam}"
    )


    logger.info(
        "[EXTRACAO] Processo finalizado. Dados preparados para camada Bronze"
    )


    return {


        "raw_cheapshark": ofertas,


        "dados": jogos,


        "total_ofertas": len(ofertas)

    }



# =========================
# Execução
# =========================

if __name__ == "__main__":


    resultado = executar_extracao()


    logger.info(
        "[CAMADA] Enviando dados extraídos para persistência Bronze"
    )


    salvar_bronze_raw(
        resultado
    )


    salvar_bronze_enriched(
        resultado
    )


    logger.info(
        "[CAMADA] Persistência Bronze concluída"
    )


    logger.info(
        "[FINALIZADO] Pipeline de extração concluído com sucesso"
    )