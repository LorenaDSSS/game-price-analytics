import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st
import pandas as pd
from conexao import (
    carregar_ofertas, carregar_ranking_generos,
    carregar_ranking_desenvolvedores, carregar_historico, listar_jogos,
)
from graficos import (
    grafico_ranking_generos, grafico_ranking_desenvolvedores,
    grafico_historico, grafico_nivel_oferta, grafico_dispersao,
)

st.set_page_config(page_title="GamePrice Analytics", page_icon="🎮", layout="wide")

@st.cache_data(ttl=3600)
def dados():
    return (
        carregar_ofertas(),
        carregar_ranking_generos(),
        carregar_ranking_desenvolvedores(),
    )

ofertas, generos, desenvolvedores = dados()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Controles")
    if st.button("🔄 Atualizar dados"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Cache de 1 hora")

# ── Header com data do snapshot ───────────────────────────────────────────────
col_titulo, col_data = st.columns([3, 1])
with col_titulo:
    st.title("🎮 GamePrice Analytics")
    st.caption("Análise de preços e promoções de jogos digitais")
with col_data:
    data_snapshot = pd.to_datetime(ofertas["data_ingestao"]).max()
    st.metric("Dados referentes a", data_snapshot.strftime("%d/%m/%Y"))

st.divider()

# ── KPIs ──────────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total de ofertas", len(ofertas))
col2.metric("Desconto médio", f"{ofertas['desconto'].mean():.1f}%")
col3.metric("Avaliação média", f"{ofertas['avaliacao_percentual'].mean():.0f}%")
col4.metric("Ofertas EXCELENTES", len(ofertas[ofertas["nivel_oferta"] == "EXCELENTE"]))

st.divider()

def formatar_preco(valor, moeda):
    if pd.isna(valor):
        return "—"
    simbolo = "R$" if moeda == "BRL" else "$"
    return f"{simbolo}{valor:.2f}"

# ── Abas ──────────────────────────────────────────────────────────────────────
aba1, aba2, aba3, aba4, aba5 = st.tabs([
    "🏷️ Melhores Ofertas", "🎯 Gêneros", "👨‍💻 Desenvolvedores", "📈 Histórico", "🔵 Análise"
])

with aba1:
    st.subheader("Melhores ofertas do dia")

    col_filtro1, col_filtro2 = st.columns([2, 1])
    with col_filtro1:
        busca = st.text_input("🔍 Buscar jogo", placeholder="Ex: Hollow Knight, Doom...")
    with col_filtro2:
        nivel_filtro = st.multiselect(
            "Nível da oferta", ["EXCELENTE", "BOA", "MEDIA", "BAIXA"],
            default=["EXCELENTE", "BOA"],
        )

    df_filtrado = ofertas.copy()
    if busca:
        df_filtrado = df_filtrado[df_filtrado["nome"].str.contains(busca, case=False, na=False)]
    if nivel_filtro:
        df_filtrado = df_filtrado[df_filtrado["nivel_oferta"].isin(nivel_filtro)]

    total_ocultos = len(ofertas) - len(df_filtrado)
    if total_ocultos > 0:
        st.caption(f"Exibindo {len(df_filtrado)} de {len(ofertas)} ofertas ({total_ocultos} ocultas pelo filtro)")

    st.download_button(
        "⬇️ Baixar ofertas filtradas (CSV)",
        df_filtrado.to_csv(index=False).encode("utf-8"),
        "ofertas.csv", "text/csv",
    )

    df_exibir = df_filtrado.copy()
    df_exibir["Oferta"] = df_exibir.apply(lambda r: formatar_preco(r["preco_oferta"], "USD"), axis=1)
    df_exibir["Preço Steam"] = df_exibir.apply(lambda r: formatar_preco(r["preco_steam"], r["moeda_steam"]), axis=1)
    df_exibir["Steam (original)"] = df_exibir.apply(lambda r: formatar_preco(r["preco_steam_original"], r["moeda_steam"]), axis=1)

    col_a, col_b = st.columns([3, 1])
    with col_a:
        st.dataframe(
            df_exibir[["nome", "loja", "Oferta", "Preço Steam", "Steam (original)", "desconto", "avaliacao", "nivel_oferta"]].rename(columns={
                "nome": "Jogo", "loja": "Loja", "desconto": "Desconto (%)",
                "avaliacao": "Avaliação", "nivel_oferta": "Nível",
            }),
            use_container_width=True, hide_index=True,
        )
    with col_b:
        st.plotly_chart(grafico_nivel_oferta(df_filtrado), use_container_width=True)

with aba2:
    st.subheader("Ranking de gêneros")
    col_graf, col_tab = st.columns([1, 1])
    with col_graf:
        st.plotly_chart(grafico_ranking_generos(generos), use_container_width=True)
    with col_tab:
        st.dataframe(generos.rename(columns={
            "genero_nome": "Gênero", "quantidade_jogos": "Jogos",
            "desconto_medio": "Desc. médio (%)", "preco_medio_oferta": "Preço médio ($)",
            "preco_medio_normal": "Normal ($)", "maior_desconto": "Maior desc. (%)",
            "avaliacao_media": "Avaliação",
        }), use_container_width=True, hide_index=True, height=500)

with aba3:
    st.subheader("Ranking de desenvolvedores")
    col_graf, col_tab = st.columns([1, 1])
    with col_graf:
        st.plotly_chart(grafico_ranking_desenvolvedores(desenvolvedores), use_container_width=True)
    with col_tab:
        st.dataframe(desenvolvedores.rename(columns={
            "desenvolvedor_nome": "Desenvolvedor", "quantidade_jogos": "Jogos",
            "desconto_medio": "Desc. médio (%)", "preco_medio_oferta": "Preço médio ($)",
            "preco_medio_normal": "Normal ($)", "maior_desconto": "Maior desc. (%)",
            "avaliacao_media": "Avaliação",
        }), use_container_width=True, hide_index=True, height=500)

with aba4:
    st.subheader("Histórico de preços por jogo")
    jogos = listar_jogos()
    if jogos:
        busca_hist = st.text_input("🔍 Buscar jogo no histórico", placeholder="Ex: Celeste, Hades...")
        jogos_filtrados = [j for j in jogos if busca_hist.lower() in j.lower()] if busca_hist else jogos

        if jogos_filtrados:
            jogo_selecionado = st.selectbox(
                f"Selecione um jogo ({len(jogos_filtrados)} encontrados)", jogos_filtrados,
            )
            historico = carregar_historico(jogo_selecionado)
            if not historico.empty:
                st.plotly_chart(grafico_historico(historico, jogo_selecionado), use_container_width=True)

                hist_exibir = historico.copy()
                hist_exibir["Oferta"] = hist_exibir.apply(lambda r: formatar_preco(r["preco_oferta"], "USD"), axis=1)
                hist_exibir["Normal"] = hist_exibir.apply(lambda r: formatar_preco(r["preco_normal"], "USD"), axis=1)
                hist_exibir["Steam"] = hist_exibir.apply(lambda r: formatar_preco(r["preco_steam"], r["moeda_steam"]), axis=1)

                st.dataframe(hist_exibir[["data_ingestao", "Oferta", "Normal", "Steam", "moeda_steam", "desconto"]].rename(columns={
                    "data_ingestao": "Data", "moeda_steam": "Moeda Steam", "desconto": "Desconto (%)",
                }), use_container_width=True, hide_index=True)
            else:
                st.info("Nenhum histórico encontrado para esse jogo.")
        else:
            st.warning(f'Nenhum jogo encontrado com "{busca_hist}".')
    else:
        st.info("Nenhum jogo disponível no histórico.")

with aba5:
    st.subheader("Desconto × Avaliação")
    st.caption("Jogos no quadrante superior direito têm alto desconto E boa avaliação — os melhores negócios.")
    st.plotly_chart(grafico_dispersao(ofertas), use_container_width=True)