# 📊 Dashboard — GamePrice Analytics

Documentação do dashboard Streamlit de análise de preços de jogos.

---

## Como rodar

```bash
cd dashboard
streamlit run app.py
```

---

## Abas disponíveis

### 🏷️ Melhores Ofertas
Lista de jogos em promoção do snapshot mais recente.

- **Filtros:** busca por nome e nível da oferta (EXCELENTE / BOA / MEDIA / BAIXA)
- **Preços:** `Oferta` sempre em USD · `Preço Steam` em BRL ou USD conforme `moeda_steam`
- **`economia`** só é calculada quando ambos os preços estão em USD
- **Download:** exportar resultados filtrados em CSV

<!-- ![Aba Ofertas](images/dash_ofertas.png) -->

### 🎯 Gêneros
Ranking de gêneros por quantidade de jogos em promoção, desconto médio e avaliação.

<!-- ![Aba Gêneros](images/dash_generos.png) -->

### 👨‍💻 Desenvolvedores
Top 20 desenvolvedores com mais jogos em promoção.

<!-- ![Aba Desenvolvedores](images/dash_desenvolvedores.png) -->

### 📈 Histórico
Evolução de preços de um jogo ao longo das datas de ingestão.

- Buscar por nome → selecionar o jogo → ver gráfico de linha temporal
- Linhas: oferta (USD), preço normal (USD), preço Steam (BRL ou USD)

<!-- ![Aba Histórico](images/dash_historico.png) -->

### 🔵 Análise — Desconto × Avaliação
Gráfico de dispersão para identificar os melhores negócios:
jogos com **alto desconto e boa avaliação** ficam no quadrante superior direito.

<!-- ![Aba Análise](images/dash_analise.png) -->

---

## Atualização dos dados

Os dados são cacheados por **1 hora**. Para forçar atualização:
clique em **🔄 Atualizar dados** na sidebar.

Os dados refletem sempre o snapshot mais recente carregado pelo pipeline.

---

## Fontes de dados

| Campo | Fonte | Moeda |
|---|---|---|
| `preco_oferta`, `preco_normal`, `desconto`, `loja` | CheapShark API | USD |
| `preco_steam`, `preco_steam_original` | Steam Store API | BRL ou USD |
| `avaliacao`, `avaliacao_percentual` | CheapShark (via Steam ratings) | — |
| `generos`, `desenvolvedores` | Steam Store API | — |
