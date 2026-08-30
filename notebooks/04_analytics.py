# Databricks notebook source
# MAGIC %md
# MAGIC # Tech Challenge Fase 2
# MAGIC ## Notebook 04 - Análise e modelagem
# MAGIC
# MAGIC Pré-requisito: o notebook 03 precisa ter rodado.
# MAGIC
# MAGIC A Gold existe para ser consumida, e é isso que este notebook faz. São quatro coisas: o
# MAGIC retrato da desigualdade entre municípios, um modelo que prevê a taxa do próximo ciclo, um
# MAGIC agrupamento por perfil de vulnerabilidade e um índice de priorização que volta para a Gold
# MAGIC como tabela.
# MAGIC
# MAGIC A análise usa pandas e scikit-learn em vez de Spark. As tabelas da Gold têm dezenas de
# MAGIC milhares de linhas, e nesse volume o custo de distribuir é maior que o ganho. É também o
# MAGIC motivo de a Gold ser a camada certa para modelagem: o agregado já cabe na memória de um nó.
# MAGIC
# MAGIC Nenhuma célula abaixo faz tratamento de nulo, decodificação de rede, conversão de tipo ou
# MAGIC junção. Isso tudo ficou nas camadas anteriores, e é o que deixa a análise curta.
# MAGIC
# MAGIC Os gráficos são salvos como PNG no volume, para usar no relatório e na apresentação.

# COMMAND ----------

# COMMAND ----------

# ============================================================
# SETUP E CARGA DA CAMADA GOLD
# ============================================================
import os
import sys
import warnings
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore")
plt.rcParams.update({"figure.figsize": (11, 5), "axes.grid": True, "grid.alpha": 0.3,
                     "axes.spines.top": False, "axes.spines.right": False, "font.size": 10})

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("schema_gold", "tc2_gold", "Schema Gold")
dbutils.widgets.text("schema_landing", "tc2_landing", "Schema Landing")
dbutils.widgets.text("volume", "arquivos", "Volume")
dbutils.widgets.text("schema_monitoramento", "tc2_monitoramento", "Schema de monitoramento")

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")
VOLUME = f"/Volumes/{CATALOGO}/{dbutils.widgets.get('schema_landing')}/{dbutils.widgets.get('volume')}"
SCHEMA_MONITORAMENTO = dbutils.widgets.get("schema_monitoramento")
DIR_GRAFICOS = f"{VOLUME}/graficos"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_MONITORAMENTO}")

def localizar_raiz_repositorio():
    """Acha a raiz do repositorio a partir do diretorio de execucao."""
    inicio = Path(os.getcwd()).resolve()
    for candidato in [inicio, *inicio.parents]:
        if (candidato / "src" / "utils" / "monitoramento.py").exists():
            return candidato
    return None


RAIZ = localizar_raiz_repositorio()
if RAIZ is None:
    raise Exception("Nao encontrei src/. Rode a partir do repositorio importado como Git folder.")
sys.path.insert(0, str(RAIZ))

from src.utils.monitoramento import Monitor

monitor = Monitor(spark, CATALOGO, SCHEMA_MONITORAMENTO, "04_analytics")

dbutils.fs.mkdirs(DIR_GRAFICOS)


def salvar_figura(nome):
    """Salva o grafico atual como PNG no volume e mostra no notebook."""
    caminho = f"{DIR_GRAFICOS}/{nome}.png"
    plt.savefig(caminho, dpi=150, bbox_inches="tight")
    print(f"[grafico] {caminho}")
    display(plt.gcf())
    plt.close()


def carregar(tabela):
    """Carrega uma tabela da Gold como DataFrame do pandas.

    Expressao com literal decimal no SQL, do tipo 100.0 * SUM(...), produz DECIMAL
    no Spark, e o toPandas devolve isso como objeto Decimal em coluna do tipo
    object. Nesse formato o matplotlib nao plota e o sklearn nao treina, entao as
    colunas numericas viram float na carga.
    """
    df = spark.table(f"{CATALOGO}.{SCHEMA_GOLD}.{tabela}").toPandas()
    for coluna, tipo in zip(df.columns, df.dtypes):
        if tipo == "object" and isinstance(df[coluna].dropna().head(1).squeeze(), Decimal):
            df[coluna] = df[coluna].astype(float)
    return df


with monitor.etapa("carga_gold", camada="gold") as etapa:
    indicador   = carregar("gold_indicador_municipio")
    consolidado = carregar("gold_meta_vs_realizado")
    evolucao    = carregar("gold_evolucao_temporal")
    ranking_uf  = carregar("gold_ranking_uf")
    features    = carregar("gold_features_ml")
    niveis      = carregar("gold_distribuicao_niveis")
    etapa.saida(sum(len(d) for d in (indicador, consolidado, evolucao,
                                     ranking_uf, features, niveis)))

ANO_REFERENCIA = int(indicador["ano"].max())
REDE_FOCO = "Municipal"   # rede com meta municipal definida no Compromisso Nacional

# Recorte de trabalho: rede municipal no ciclo mais recente
municipal = indicador[(indicador["ano"] == ANO_REFERENCIA)
                      & (indicador["rede_nome"] == REDE_FOCO)].copy()

print(f"Camada Gold carregada — ano de referência: {ANO_REFERENCIA}")
print(f"  gold_indicador_municipio : {len(indicador):>7,} linhas".replace(",", "."))
print(f"  gold_evolucao_temporal   : {len(evolucao):>7,} linhas".replace(",", "."))
print(f"  gold_features_ml         : {len(features):>7,} linhas".replace(",", "."))
print(f"\nRecorte de análise: rede {REDE_FOCO} em {ANO_REFERENCIA} -> "
      f"{len(municipal):,} municípios".replace(",", "."))
display(municipal.head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Etapa 2 - Retrato da desigualdade
# MAGIC
# MAGIC O indicador tem uma média nacional, mas ela esconde o que interessa para a política pública:
# MAGIC a distância entre o município que alfabetiza 9 em cada 10 crianças e o que alfabetiza 2 em
# MAGIC cada 10, no mesmo ano e no mesmo país.
# MAGIC
# MAGIC Por isso as células abaixo olham para a distribuição inteira e para os recortes regionais.

# COMMAND ----------

# ============================================================
# ETAPA 2.1 — DISTRIBUIÇÃO NACIONAL E RECORTE REGIONAL
# ============================================================
META_2030 = 80.0

descritivas = municipal["taxa_alfabetizacao"].describe()
amplitude = descritivas["max"] - descritivas["min"]
razao_p90_p10 = (municipal["taxa_alfabetizacao"].quantile(0.90)
                 / municipal["taxa_alfabetizacao"].quantile(0.10))

print(f"TAXA DE ALFABETIZAÇÃO — REDE {REDE_FOCO.upper()}, {ANO_REFERENCIA}\n")
print(f"  Municípios avaliados ....... {len(municipal):,}".replace(",", "."))
print(f"  Média ...................... {descritivas['mean']:.1f}%")
print(f"  Mediana .................... {descritivas['50%']:.1f}%")
print(f"  Menor / Maior .............. {descritivas['min']:.1f}% / {descritivas['max']:.1f}%")
print(f"  Amplitude .................. {amplitude:.1f} pontos percentuais")
print(f"  Razão P90/P10 .............. {razao_p90_p10:.2f}x")
print(f"  Já na meta de 2030 (80%) ... {(municipal['taxa_alfabetizacao'] >= META_2030).mean() * 100:.1f}%")
print(f"  Em risco crítico (<40%) .... "
      f"{(municipal['risco_alfabetizacao'] == 'Critico').mean() * 100:.1f}%")

fig, eixos = plt.subplots(1, 2, figsize=(14, 5))

eixos[0].hist(municipal["taxa_alfabetizacao"], bins=45, color="#2E5EAA", edgecolor="white")
eixos[0].axvline(META_2030, color="#C1272D", linestyle="--", linewidth=2,
                 label=f"Meta 2030 ({META_2030:.0f}%)")
eixos[0].axvline(descritivas["mean"], color="#F2A104", linestyle="-", linewidth=2,
                 label=f"Média ({descritivas['mean']:.1f}%)")
eixos[0].set_title(f"Distribuição da taxa de alfabetização\nrede {REDE_FOCO}, {ANO_REFERENCIA}")
eixos[0].set_xlabel("Taxa de alfabetização (%)")
eixos[0].set_ylabel("Municípios")
eixos[0].legend()

ordem_regioes = (municipal.groupby("nome_regiao")["taxa_alfabetizacao"].median()
                 .sort_values(ascending=False).index.tolist())
dados_regiao = [municipal.loc[municipal["nome_regiao"] == r, "taxa_alfabetizacao"]
                for r in ordem_regioes]
caixas = eixos[1].boxplot(dados_regiao, labels=ordem_regioes, patch_artist=True, showfliers=False)
for caixa in caixas["boxes"]:
    caixa.set_facecolor("#7FA1D9")
eixos[1].axhline(META_2030, color="#C1272D", linestyle="--", linewidth=2)
eixos[1].set_title("Dispersão por região — a média nacional esconde isto")
eixos[1].set_ylabel("Taxa de alfabetização (%)")
plt.tight_layout()
salvar_figura("distribuicao_e_regioes")

resumo_regiao = (municipal.groupby("nome_regiao")
                 .agg(municipios=("id_municipio", "nunique"),
                      taxa_media=("taxa_alfabetizacao", "mean"),
                      taxa_mediana=("taxa_alfabetizacao", "median"),
                      desvio=("taxa_alfabetizacao", "std"),
                      pct_criticos=("risco_alfabetizacao", lambda s: (s == "Critico").mean() * 100),
                      gap_medio_2030=("gap_meta_2030", "mean"))
                 .round(2).sort_values("taxa_media", ascending=False))
print("\nPANORAMA POR REGIÃO")
display(resumo_regiao)

# COMMAND ----------

# ============================================================
# ETAPA 2.2 — META × REALIZADO POR UF E EVOLUÇÃO ENTRE CICLOS
# ============================================================
# UF sem nenhum municipio com meta definida fica com percentual nulo, por causa
# do NULLIF na consulta. Sai do grafico e e listada a parte, senao apareceria
# como barra zerada.
painel_completo = consolidado[(consolidado["ano"] == ANO_REFERENCIA)
                              & (consolidado["rede_nome"] == REDE_FOCO)]
sem_meta = painel_completo[painel_completo["pct_municipios_na_meta"].isna()]
painel_uf = (painel_completo.dropna(subset=["pct_municipios_na_meta"])
             .sort_values("pct_municipios_na_meta", ascending=False))

if len(sem_meta):
    print(f"{len(sem_meta)} UF(s) sem meta municipal definida, fora do grafico: "
          f"{', '.join(sorted(sem_meta['sigla_uf']))}")
    print()

fig, eixos = plt.subplots(1, 2, figsize=(15, 6))

cores = ["#2E7D32" if v >= 50 else "#C1272D" for v in painel_uf["pct_municipios_na_meta"]]
eixos[0].barh(painel_uf["sigla_uf"], painel_uf["pct_municipios_na_meta"], color=cores)
eixos[0].axvline(50, color="#333", linestyle=":", linewidth=1)
eixos[0].set_title(f"% de municípios dentro da meta do ano — {ANO_REFERENCIA}")
eixos[0].set_xlabel("% dos municípios da UF")
eixos[0].invert_yaxis()

evolucao_municipal = evolucao[evolucao["rede_nome"] == REDE_FOCO].dropna(subset=["variacao_pp"])
contagem_tendencia = evolucao_municipal["tendencia"].value_counts()
paleta = {"Melhora": "#2E7D32", "Estavel": "#F2A104", "Piora": "#C1272D", "Serie unica": "#9E9E9E"}
eixos[1].bar(contagem_tendencia.index, contagem_tendencia.values,
             color=[paleta.get(t, "#607D8B") for t in contagem_tendencia.index])
eixos[1].set_title(f"Tendência entre os ciclos disponíveis — rede {REDE_FOCO}")
eixos[1].set_ylabel("Municípios")
for indice, valor in enumerate(contagem_tendencia.values):
    eixos[1].text(indice, valor, f"{valor:,}".replace(",", "."), ha="center", va="bottom")
plt.tight_layout()
salvar_figura("meta_vs_realizado")

# No ritmo atual, quantos municipios chegam a meta em 2030
com_projecao = evolucao_municipal.dropna(subset=["anos_estimados_para_meta"])
anos_restantes = 2030 - ANO_REFERENCIA
chegam_a_tempo = (com_projecao["anos_estimados_para_meta"] <= anos_restantes).sum()
nao_chegam = len(com_projecao) - chegam_a_tempo
sem_rota = len(evolucao_municipal) - len(com_projecao)

print(f"PROJEÇÃO NO RITMO ATUAL (faltam {anos_restantes} anos até 2030)\n")
print(f"  Municípios com trajetória de melhora ....... {len(com_projecao):,}".replace(",", "."))
print(f"    ... que chegam à meta até 2030 ........... {chegam_a_tempo:,}".replace(",", "."))
print(f"    ... que NÃO chegam no ritmo atual ........ {nao_chegam:,}".replace(",", "."))
print(f"  Municípios estagnados ou em queda .......... {sem_rota:,}".replace(",", "."))
print(f"\n  Sem mudança de política, {(nao_chegam + sem_rota) / max(len(evolucao_municipal), 1) * 100:.0f}% "
      f"dos municípios da rede {REDE_FOCO} ficam fora da meta.")

print("\nOS 10 MUNICÍPIOS COM MAIOR QUEDA ENTRE OS CICLOS")
display(evolucao_municipal.nsmallest(10, "variacao_pp")
        [["nome_municipio", "sigla_uf", "nome_regiao", "taxa_inicial", "taxa_final",
          "variacao_pp", "tendencia"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Etapa 3 - Predição do próximo ciclo
# MAGIC
# MAGIC A ideia é estimar a taxa do próximo ciclo antes da avaliação acontecer, para antecipar apoio
# MAGIC aos municípios com maior risco.
# MAGIC
# MAGIC Duas decisões pesaram mais que a escolha do algoritmo.
# MAGIC
# MAGIC A primeira foi evitar vazamento de informação. A tabela gold_features_ml tem colunas do ciclo
# MAGIC atual, como participação e proficiência média, e todas são medidas junto com o alvo. Se
# MAGIC entrassem no treino, o R² ficaria altíssimo e o modelo seria inútil, porque no momento da
# MAGIC previsão essas informações ainda não existem. Só entram variáveis do ciclo anterior e as
# MAGIC metas.
# MAGIC
# MAGIC A segunda foi comparar com um baseline. Indicador educacional é autocorrelacionado, e
# MAGIC repetir a taxa do ano anterior já acerta bastante. O modelo só se justifica se ganhar dessa
# MAGIC regra.
# MAGIC
# MAGIC Limitação: existem só dois ciclos, 2023 e 2024. Dá para prever um passo à frente, mas não
# MAGIC para capturar efeito de longo prazo.

# COMMAND ----------

# ============================================================
# ETAPA 3.1 — MODELO PREDITIVO DA TAXA DE ALFABETIZAÇÃO
# ============================================================
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# So entram variaveis conhecidas antes do ciclo que esta sendo previsto
FEATURES_SEM_VAZAMENTO = [
    "taxa_ciclo_anterior",
    "media_ciclo_anterior",
    "participacao_ciclo_anterior",
    "taxa_uf_ciclo_anterior",
    "meta_taxa_ano",
    "meta_taxa_2030",
]
ALVO = "alvo_taxa_alfabetizacao"

# Ordena antes de dividir treino e teste, senao o resultado muda a cada execucao
base_modelo = (features.dropna(subset=["taxa_ciclo_anterior", ALVO])
                       .sort_values("id_municipio")
                       .reset_index(drop=True))
descartados = len(features) - len(base_modelo)

# One-hot da região, para entrar como variável
regioes = pd.get_dummies(base_modelo["nome_regiao"], prefix="regiao", dtype=float)
X = pd.concat([base_modelo[FEATURES_SEM_VAZAMENTO], regioes], axis=1)
X = X.fillna(X.median(numeric_only=True))
y = base_modelo[ALVO]

X_treino, X_teste, y_treino, y_teste = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=base_modelo["nome_regiao"])

print(f"Municípios com histórico completo : {len(base_modelo):,}".replace(",", "."))
print(f"Descartados por falta de ciclo anterior: {descartados:,}".replace(",", "."))
print(f"Treino / Teste: {len(X_treino):,} / {len(X_teste):,}".replace(",", "."))
print(f"Variáveis preditoras: {X.shape[1]}\n")


def avaliar(nome, y_real, y_previsto):
    return {
        "modelo": nome,
        "MAE_pp": round(mean_absolute_error(y_real, y_previsto), 2),
        "RMSE_pp": round(float(np.sqrt(mean_squared_error(y_real, y_previsto))), 2),
        "R2": round(r2_score(y_real, y_previsto), 3),
    }


# ── Baseline de comparação: repetir a taxa do ciclo anterior ──
baseline = X_teste["taxa_ciclo_anterior"]
resultados = [avaliar("Baseline (repete ciclo anterior)", y_teste, baseline)]

# ── Modelos ──
modelos = {
    "Ridge (linear regularizado)": Pipeline([("escala", StandardScaler()),
                                             ("estimador", Ridge(alpha=1.0))]),
    "Random Forest": RandomForestRegressor(n_estimators=300, max_depth=12,
                                           min_samples_leaf=5, random_state=42, n_jobs=-1),
}
treinados = {}
for nome, modelo in modelos.items():
    modelo.fit(X_treino, y_treino)
    treinados[nome] = modelo
    resultados.append(avaliar(nome, y_teste, modelo.predict(X_teste)))

comparativo = pd.DataFrame(resultados)
mae_baseline = comparativo.loc[0, "MAE_pp"]
comparativo["ganho_vs_baseline"] = (
    (1 - comparativo["MAE_pp"] / mae_baseline) * 100).round(1).astype(str) + "%"

print("DESEMPENHO NO CONJUNTO DE TESTE")
display(comparativo)

melhor_nome = comparativo.iloc[1:].sort_values("MAE_pp").iloc[0]["modelo"]
melhor = treinados[melhor_nome]
print(f"\nMelhor modelo: {melhor_nome}")
print(f"Erro médio de {comparativo.set_index('modelo').loc[melhor_nome, 'MAE_pp']:.2f} pontos "
      f"percentuais — contra {mae_baseline:.2f} p.p. da regra ingênua.")

# COMMAND ----------

# ============================================================
# ETAPA 3.2 — O QUE O MODELO APRENDEU
# ============================================================
previsoes = melhor.predict(X_teste)
residuos = y_teste - previsoes

fig, eixos = plt.subplots(1, 3, figsize=(16, 4.5))

eixos[0].scatter(y_teste, previsoes, s=12, alpha=0.35, color="#2E5EAA")
limites = [min(y_teste.min(), previsoes.min()), max(y_teste.max(), previsoes.max())]
eixos[0].plot(limites, limites, color="#C1272D", linestyle="--", linewidth=1.5)
eixos[0].set_title(f"Previsto vs. observado\n{melhor_nome}")
eixos[0].set_xlabel("Taxa observada (%)")
eixos[0].set_ylabel("Taxa prevista (%)")

eixos[1].hist(residuos, bins=40, color="#7FA1D9", edgecolor="white")
eixos[1].axvline(0, color="#C1272D", linestyle="--", linewidth=1.5)
eixos[1].set_title(f"Distribuição dos resíduos\nmédia={residuos.mean():.2f} | dp={residuos.std():.2f}")
eixos[1].set_xlabel("Erro (p.p.)")

if hasattr(melhor, "feature_importances_"):
    importancias = pd.Series(melhor.feature_importances_, index=X.columns).nlargest(10)
    titulo = "Importância das variáveis (Random Forest)"
else:
    importancias = pd.Series(np.abs(melhor.named_steps["estimador"].coef_),
                             index=X.columns).nlargest(10)
    titulo = "Coeficientes (valor absoluto)"
eixos[2].barh(importancias.index[::-1], importancias.values[::-1], color="#2E7D32")
eixos[2].set_title(titulo)
plt.tight_layout()
salvar_figura("modelo_preditivo")

print("VARIÁVEIS MAIS IMPORTANTES\n")
display(importancias.round(4).rename("importancia").to_frame())

print("\nLeitura do resultado:")
print(f"  - O erro é aproximadamente simétrico (média {residuos.mean():.2f} p.p.), ou seja,")
print("    o modelo não tende a superestimar nem subestimar sistematicamente.")
print("  - A variável dominante é o desempenho do ciclo anterior — o que é esperado e reforça")
print("    por que a comparação com o baseline de persistência era obrigatória.")
print("  - O ganho sobre o baseline vem do contexto: meta do município, desempenho da UF e região.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Etapa 3.3 - Sanidade da série histórica
# MAGIC
# MAGIC Antes de usar a variação entre ciclos num modelo ou num ranking, vale conferir se essa
# MAGIC variação é real.
# MAGIC
# MAGIC Nas maiores quedas da célula anterior apareceu um padrão estranho: elas se concentram todas
# MAGIC num estado só, com quedas de 80 pontos percentuais e valores de 2023 exatamente iguais a
# MAGIC 100,00%. Indicador educacional não varia assim.
# MAGIC
# MAGIC A célula abaixo compara a variação média de cada UF com a mediana nacional e sinaliza as que
# MAGIC destoam muito. Sem essa conferência, o ranking de priorização apontaria um estado inteiro
# MAGIC como emergência educacional por causa de um problema no dado.

# COMMAND ----------

# ============================================================
# ETAPA 3.3 — DETECÇÃO DE QUEBRA NA SÉRIE HISTÓRICA POR UF
# ============================================================
LIMITE_QUEBRA_PP = 15.0     # desvio da UF em relação à mediana nacional

variacao_nacional = evolucao_municipal["variacao_pp"].median()
variacao_uf = (evolucao_municipal.groupby("sigla_uf")
               .agg(municipios=("id_municipio", "count"),
                    variacao_media=("variacao_pp", "mean"),
                    variacao_mediana=("variacao_pp", "median"))
               .round(2))
variacao_uf["desvio_vs_nacional"] = (variacao_uf["variacao_media"] - variacao_nacional).round(2)
variacao_uf = variacao_uf.sort_values("variacao_media")

ufs_suspeitas = variacao_uf[variacao_uf["desvio_vs_nacional"].abs() > LIMITE_QUEBRA_PP]
UFS_SERIE_SUSPEITA = set(ufs_suspeitas.index)

print(f"Variação mediana nacional entre os ciclos: {variacao_nacional:+.2f} p.p.")
print(f"Limite para sinalizar quebra de série: ±{LIMITE_QUEBRA_PP:.0f} p.p. de desvio\n")
print("VARIAÇÃO POR UF (5 menores e 5 maiores)")
display(pd.concat([variacao_uf.head(5), variacao_uf.tail(5)]))

if len(ufs_suspeitas):
    print(f"\n⚠️  QUEBRA DE SÉRIE DETECTADA: {', '.join(sorted(UFS_SERIE_SUSPEITA))}")
    display(ufs_suspeitas)

    # Taxa de exatamente 100% no ciclo inicial e implausivel numa rede real
    suspeitos = evolucao_municipal[evolucao_municipal["sigla_uf"].isin(UFS_SERIE_SUSPEITA)]
    demais = evolucao_municipal[~evolucao_municipal["sigla_uf"].isin(UFS_SERIE_SUSPEITA)]
    print(f"\nMunicípios com taxa inicial = 100,00%:")
    print(f"  nas UFs sinalizadas : {(suspeitos['taxa_inicial'] == 100).sum():>4} "
          f"({(suspeitos['taxa_inicial'] == 100).mean() * 100:.1f}% da UF)")
    print(f"  no resto do país    : {(demais['taxa_inicial'] == 100).sum():>4} "
          f"({(demais['taxa_inicial'] == 100).mean() * 100:.1f}%)")

    print("\nInterpretação: o padrão é compatível com mudança de metodologia, de instrumento ou de")
    print("cobertura da avaliação entre os dois ciclos — não com uma queda real de aprendizagem.")
    print("\nDecisão adotada nas análises seguintes:")
    print("  1. A variação entre ciclos dessas UFs NÃO alimenta o índice de priorização")
    print("     (usamos a mediana nacional no lugar, neutralizando o efeito).")
    print("  2. Os municípios afetados recebem a marca 'alerta_serie_historica' e continuam")
    print("     visíveis — a decisão de excluí-los é do gestor, não nossa.")
    print("  3. O achado seria reportado ao produtor do dado (Inep / Base dos Dados).")
else:
    print("\nNenhuma quebra de série detectada — todas as UFs variam dentro do esperado.")

# A coluna original e preservada; a ajustada e a que entra nas analises
evolucao_municipal = evolucao_municipal.copy()
evolucao_municipal["alerta_serie_historica"] = evolucao_municipal["sigla_uf"].isin(UFS_SERIE_SUSPEITA)
evolucao_municipal["variacao_pp_ajustada"] = np.where(
    evolucao_municipal["alerta_serie_historica"], variacao_nacional,
    evolucao_municipal["variacao_pp"])

fig, eixo = plt.subplots(figsize=(13, 4.5))
cores = ["#C1272D" if uf in UFS_SERIE_SUSPEITA else "#2E5EAA" for uf in variacao_uf.index]
eixo.bar(variacao_uf.index, variacao_uf["variacao_media"], color=cores)
eixo.axhline(variacao_nacional, color="#F2A104", linestyle="--", linewidth=2,
             label=f"Mediana nacional ({variacao_nacional:+.1f} p.p.)")
eixo.set_title("Variação média do indicador entre ciclos, por UF — vermelho = quebra de série")
eixo.set_ylabel("Variação (p.p.)")
eixo.legend()
plt.tight_layout()
salvar_figura("quebra_serie_uf")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Etapa 4 - Clusters de vulnerabilidade
# MAGIC
# MAGIC A ideia é identificar perfis de município que pedem intervenções diferentes.
# MAGIC
# MAGIC Uma política nacional não consegue tratar 5.400 municípios um a um, e tratar todos igual
# MAGIC também não funciona. A clusterização agrupa municípios parecidos em várias dimensões ao
# MAGIC mesmo tempo: nível atual, distância da meta, trajetória, participação e posição relativa
# MAGIC dentro do próprio estado.
# MAGIC
# MAGIC O número de grupos sai do coeficiente de silhueta, testado para vários valores de k.

# COMMAND ----------

# ============================================================
# ETAPA 4.1 — CLUSTERIZAÇÃO DE VULNERABILIDADE (K-MEANS)
# ============================================================
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# A base junta situacao atual e trajetoria, que ja vem prontas da Gold
base_cluster = (municipal[["id_municipio", "nome_municipio", "sigla_uf", "nome_regiao",
                           "taxa_alfabetizacao", "media_portugues", "gap_meta_2030",
                           "percentual_participacao", "dif_vs_uf"]]
                .merge(evolucao_municipal[["id_municipio", "variacao_pp",
                                           "variacao_pp_ajustada", "alerta_serie_historica"]],
                       on="id_municipio", how="left"))
base_cluster["alerta_serie_historica"] = base_cluster["alerta_serie_historica"].fillna(False)

# Aqui entra a variacao ajustada. Se entrasse a original, as UFs com quebra de
# serie puxariam o agrupamento para um perfil de colapso que nao existe.
VARIAVEIS_CLUSTER = ["taxa_alfabetizacao", "media_portugues", "gap_meta_2030",
                     "percentual_participacao", "dif_vs_uf", "variacao_pp_ajustada"]
base_cluster[VARIAVEIS_CLUSTER] = base_cluster[VARIAVEIS_CLUSTER].fillna(
    base_cluster[VARIAVEIS_CLUSTER].median())

escalador = StandardScaler()
X_cluster = escalador.fit_transform(base_cluster[VARIAVEIS_CLUSTER])

# ── Escolha de k pela silhueta ──
avaliacao_k = []
for k in range(2, 7):
    modelo_k = KMeans(n_clusters=k, random_state=42, n_init=10).fit(X_cluster)
    avaliacao_k.append({"k": k,
                        "silhueta": round(silhouette_score(X_cluster, modelo_k.labels_), 3),
                        "inercia": round(modelo_k.inertia_)})

df_k = pd.DataFrame(avaliacao_k)
print("ESCOLHA DO NÚMERO DE CLUSTERS")
display(df_k)

# A silhueta quase sempre aponta k=2, mas dois grupos ajudam pouco na pratica.
# Entao fica o melhor k entre 3 e 6.
k_silhueta_pura = int(df_k.loc[df_k["silhueta"].idxmax(), "k"])
candidatos = df_k[df_k["k"] >= 3]
melhor_k = int(candidatos.loc[candidatos["silhueta"].idxmax(), "k"])
print(f"Melhor k pela silhueta pura .....: {k_silhueta_pura} "
      f"(segmentação binária, pouco acionável)")
print(f"k adotado (melhor entre 3 e 6) ..: {melhor_k}\n")

kmeans = KMeans(n_clusters=melhor_k, random_state=42, n_init=10).fit(X_cluster)
base_cluster["cluster"] = kmeans.labels_

# ── Perfil de cada cluster ──
perfil = (base_cluster.groupby("cluster")[VARIAVEIS_CLUSTER].mean().round(2))
perfil["municipios"] = base_cluster["cluster"].value_counts().sort_index()

# Os clusters saem numerados. Ordenar pela taxa media permite dar nome a cada um.
ordem = perfil.sort_values("taxa_alfabetizacao").index.tolist()
NOMES_PERFIL = ["Vulnerabilidade crítica", "Atenção prioritária", "Em rota de melhoria",
                "Consolidado", "Referência", "Destaque"]
rotulos = {cluster: NOMES_PERFIL[posicao] for posicao, cluster in enumerate(ordem)}
base_cluster["perfil"] = base_cluster["cluster"].map(rotulos)
perfil["perfil"] = perfil.index.map(rotulos)

print("PERFIL MÉDIO DE CADA CLUSTER")
display(perfil[["perfil", "municipios"] + VARIAVEIS_CLUSTER])

fig, eixos = plt.subplots(1, 2, figsize=(15, 5))

for cluster in ordem:
    recorte = base_cluster[base_cluster["cluster"] == cluster]
    eixos[0].scatter(recorte["taxa_alfabetizacao"], recorte["variacao_pp_ajustada"],
                     s=14, alpha=0.45, label=rotulos[cluster])
eixos[0].axhline(0, color="#333", linewidth=1)
eixos[0].axvline(META_2030, color="#C1272D", linestyle="--", linewidth=1.5)
eixos[0].set_xlabel("Taxa de alfabetização atual (%)")
eixos[0].set_ylabel("Variação entre ciclos (p.p.)")
eixos[0].set_title("Onde está e para onde vai cada município")
eixos[0].legend(fontsize=8)

distribuicao = (pd.crosstab(base_cluster["nome_regiao"], base_cluster["perfil"],
                            normalize="index") * 100)
distribuicao = distribuicao[[rotulos[c] for c in ordem]]
base_acumulada = np.zeros(len(distribuicao))
for coluna in distribuicao.columns:
    eixos[1].barh(distribuicao.index, distribuicao[coluna], left=base_acumulada, label=coluna)
    base_acumulada += distribuicao[coluna].values
eixos[1].set_title("Composição dos perfis por região (%)")
eixos[1].set_xlabel("% dos municípios da região")
eixos[1].legend(fontsize=8, loc="lower right")
plt.tight_layout()
salvar_figura("clusters_municipios")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Etapa 5 - Índice de priorização
# MAGIC
# MAGIC Esta célula transforma a análise numa lista ordenada. O índice combina três dimensões, cada
# MAGIC uma convertida em percentil: distância da meta de 2030 com peso 50%, trajetória entre ciclos
# MAGIC com 30% e posição relativa dentro da própria UF com 20%.
# MAGIC
# MAGIC O índice ainda não leva em conta quantas crianças estão envolvidas. Um município de 800
# MAGIC habitantes e uma capital com a mesma taxa acabam com prioridade parecida, o que não faz
# MAGIC sentido para alocar orçamento. Para corrigir isso seria preciso ingerir as matrículas do
# MAGIC Censo Escolar e ponderar o índice. Fica como próximo passo.

# COMMAND ----------

# ============================================================
# ETAPA 5.1 — ÍNDICE DE PRIORIZAÇÃO DE INVESTIMENTO
# ============================================================
PESOS = {"gap_meta_2030": 0.50, "trajetoria": 0.30, "posicao_relativa": 0.20}

priorizacao = base_cluster.copy()

# Cada dimensão vira percentil: 100 = caso mais urgente
priorizacao["p_gap"] = priorizacao["gap_meta_2030"].rank(pct=True) * 100
priorizacao["p_trajetoria"] = (-priorizacao["variacao_pp_ajustada"]).rank(pct=True) * 100
priorizacao["p_relativa"] = (-priorizacao["dif_vs_uf"]).rank(pct=True) * 100

priorizacao["indice_prioridade"] = (
    PESOS["gap_meta_2030"] * priorizacao["p_gap"]
    + PESOS["trajetoria"] * priorizacao["p_trajetoria"]
    + PESOS["posicao_relativa"] * priorizacao["p_relativa"]
).round(1)

priorizacao["faixa_prioridade"] = pd.cut(
    priorizacao["indice_prioridade"], bins=[0, 50, 70, 85, 100],
    labels=["Monitoramento", "Média", "Alta", "Crítica"], include_lowest=True).astype(str)

print("DISTRIBUIÇÃO POR FAIXA DE PRIORIDADE\n")
faixas = (priorizacao.groupby("faixa_prioridade")
          .agg(municipios=("id_municipio", "count"),
               taxa_media=("taxa_alfabetizacao", "mean"),
               gap_medio_2030=("gap_meta_2030", "mean"),
               variacao_media=("variacao_pp_ajustada", "mean"))
          .round(2).reindex(["Crítica", "Alta", "Média", "Monitoramento"]))
display(faixas)

print("\nOS 20 MUNICÍPIOS DE MAIOR PRIORIDADE")
colunas_saida = ["nome_municipio", "sigla_uf", "nome_regiao", "perfil", "taxa_alfabetizacao",
                 "gap_meta_2030", "variacao_pp_ajustada", "dif_vs_uf", "indice_prioridade",
                 "alerta_serie_historica"]
display(priorizacao.nlargest(20, "indice_prioridade")[colunas_saida]
        .reset_index(drop=True))

print("\nCONCENTRAÇÃO DA PRIORIDADE CRÍTICA POR UF (top 10)")
criticos_uf = (priorizacao[priorizacao["faixa_prioridade"] == "Crítica"]
               .groupby("sigla_uf").size().sort_values(ascending=False).head(10))
display(criticos_uf.rename("municipios_prioridade_critica").to_frame())

# Grava o resultado de volta na Gold
COLUNAS_SAIDA = ["id_municipio", "nome_municipio", "sigla_uf", "nome_regiao", "cluster",
                 "perfil", "taxa_alfabetizacao", "gap_meta_2030", "variacao_pp",
                 "variacao_pp_ajustada", "dif_vs_uf", "indice_prioridade",
                 "faixa_prioridade", "alerta_serie_historica"]
destino = f"{CATALOGO}.{SCHEMA_GOLD}.gold_priorizacao_municipio"
with monitor.etapa("gravacao_priorizacao", camada="gold") as etapa:
    (spark.createDataFrame(priorizacao[COLUNAS_SAIDA])
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino))
    etapa.entrada(len(priorizacao)).saida(len(priorizacao))
print(f"\nTabela de priorizacao gravada na Gold: {destino}")
print("Passa a ser consultavel por SQL como qualquer outra tabela da camada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Etapa 6 - O que faltaria para virar produto
# MAGIC
# MAGIC Num cenário de produção, o treino viraria uma tarefa agendada no Lakeflow, disparada depois
# MAGIC do job da Gold. A previsão seria um job mensal gravando de volta na Gold, e o índice de
# MAGIC priorização seria consumido por uma ferramenta de BI ou por API.
# MAGIC
# MAGIC ### Limitações
# MAGIC
# MAGIC 1. Só dois ciclos de dados. As tendências são de curto prazo, e um município pode variar por
# MAGIC    mudança na composição da rede em vez de qualidade de ensino.
# MAGIC 2. O modelo mostra onde o risco é maior, mas não explica a causa.
# MAGIC 3. Participação afeta o indicador. Município com participação baixa tem taxa menos confiável,
# MAGIC    e a coluna está na Gold justamente para servir de filtro nesse tipo de decisão.
# MAGIC
# MAGIC ### Próximas fontes
# MAGIC
# MAGIC Censo Escolar para matrículas e infraestrutura, IBGE e PNAD para contexto socioeconômico,
# MAGIC Cadastro Único para vulnerabilidade social e FUNDEB para investimento por aluno.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo dos quatro notebooks
# MAGIC
# MAGIC | Notebook | O que faz |
# MAGIC |---|---|
# MAGIC | 01 | ingestão em lote das 6 fontes, Bronze, verificações e Silver integrada |
# MAGIC | 02 | ingestão em streaming, fila de erro, janelas, alertas e upsert idempotente |
# MAGIC | 03 | camada Gold, otimização das tabelas e análise de custo |
# MAGIC | 04 | consumo da Gold: desigualdade, modelo, clusters e priorização |
# MAGIC
# MAGIC O que cada execução deixa gravado:
# MAGIC
# MAGIC | Onde | O quê |
# MAGIC |---|---|
# MAGIC | histórico Delta de cada tabela | quem escreveu, quando e quantas linhas |
# MAGIC | tc2_monitoramento.execucao_etapa | duração, volume e rejeições por etapa |
# MAGIC | gold_priorizacao_municipio | o resultado desta análise, consultável por SQL |
# MAGIC | volume graficos/ | os PNG usados no relatório e na apresentação |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoramento da execução
# MAGIC
# MAGIC As etapas acima ficam gravadas em tc2_monitoramento.execucao_etapa, junto com volume
# MAGIC processado, rejeições e alertas.

# COMMAND ----------

# ============================================================
# PAINEL DE EXECUÇÃO
# ============================================================
monitor.gravar()
monitor.painel()
