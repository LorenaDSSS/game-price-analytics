import plotly.express as px
import plotly.graph_objects as go
import pandas as pd  # ← adicionar esta linha



def grafico_ranking_generos(df):
    fig = px.bar(
        df.head(15), x="quantidade_jogos", y="genero_nome", orientation="h",
        color="desconto_medio", color_continuous_scale="Blues",
        labels={"quantidade_jogos": "Jogos em promoção", "genero_nome": "Gênero", "desconto_medio": "Desconto médio (%)"},
        title="Gêneros com mais jogos em promoção",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    return fig


def grafico_ranking_desenvolvedores(df):
    fig = px.bar(
        df.head(15), x="quantidade_jogos", y="desenvolvedor_nome", orientation="h",
        color="avaliacao_media", color_continuous_scale="Greens",
        labels={"quantidade_jogos": "Jogos em promoção", "desenvolvedor_nome": "Desenvolvedor", "avaliacao_media": "Avaliação média"},
        title="Top desenvolvedores com jogos em promoção",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    return fig


def grafico_historico(df, nome_jogo):
    if df.empty:
        return None

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["data_ingestao"], y=df["preco_oferta"],
        name="Preço oferta (USD)", mode="lines+markers", line={"color": "#1f77b4"},
    ))
    fig.add_trace(go.Scatter(
        x=df["data_ingestao"], y=df["preco_normal"],
        name="Preço normal (USD)", mode="lines+markers",
        line={"color": "#aec7e8", "dash": "dash"},
    ))

    moeda_steam = df["moeda_steam"].iloc[0] if not df["moeda_steam"].isna().all() else ""
    if not df["preco_steam"].isna().all():
        fig.add_trace(go.Scatter(
            x=df["data_ingestao"], y=df["preco_steam"],
            name=f"Preço Steam ({moeda_steam})", mode="lines+markers",
            line={"color": "#ff7f0e"},
        ))

    fig.update_layout(
        title=f"Histórico de preços — {nome_jogo}",
        xaxis_title="Data", yaxis_title="Preço",
        legend={"orientation": "h"},
    )
    return fig


def grafico_nivel_oferta(df):
    contagem = df["nivel_oferta"].value_counts().reset_index()
    contagem.columns = ["nivel", "quantidade"]
    ordem = ["EXCELENTE", "BOA", "MEDIA", "BAIXA"]
    cores = {"EXCELENTE": "#2ca02c", "BOA": "#1f77b4", "MEDIA": "#ff7f0e", "BAIXA": "#d62728"}
    contagem["nivel"] = pd.Categorical(contagem["nivel"], categories=ordem, ordered=True)
    contagem = contagem.sort_values("nivel")

    fig = px.bar(
        contagem, x="nivel", y="quantidade", color="nivel",
        color_discrete_map=cores,
        labels={"nivel": "Nível da oferta", "quantidade": "Quantidade"},
        title="Distribuição por nível de oferta",
    )
    fig.update_layout(showlegend=False)
    return fig

def grafico_dispersao(df):
    cores = {"EXCELENTE": "#2ca02c", "BOA": "#1f77b4", "MEDIA": "#ff7f0e", "BAIXA": "#d62728"}
    fig = px.scatter(
        df.dropna(subset=["desconto", "avaliacao_percentual"]),
        x="desconto", y="avaliacao_percentual",
        color="nivel_oferta", color_discrete_map=cores,
        hover_data=["nome", "loja"],
        labels={"desconto": "Desconto (%)", "avaliacao_percentual": "Avaliação (%)", "nivel_oferta": "Nível"},
        title="Desconto vs Avaliação dos jogos",
    )
    fig.add_hline(y=70, line_dash="dot", line_color="gray", annotation_text="Avaliação 70%")
    fig.add_vline(x=50, line_dash="dot", line_color="gray", annotation_text="Desconto 50%")
    return fig