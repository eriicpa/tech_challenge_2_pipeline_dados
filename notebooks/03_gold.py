# Databricks notebook source
# MAGIC %md
# MAGIC # Tech Challenge Fase 2
# MAGIC ## Notebook 03 - Camada Gold
# MAGIC
# MAGIC Pré-requisitos: os notebooks 01 e 02 precisam ter rodado.
# MAGIC
# MAGIC Este notebook quase não tem SQL escrito dentro dele. Ele importa src/gold/consultas.py, onde
# MAGIC estão as oito consultas, e executa cada uma aqui.
# MAGIC
# MAGIC Escrevi as consultas em SQL padrão num arquivo separado em vez de usar a DSL do Spark
# MAGIC espalhada pelas células por dois motivos: a regra de negócio fica num lugar só e versionada,
# MAGIC e o mesmo SQL roda no editor do Databricks sem alteração.
# MAGIC
# MAGIC Para o import funcionar, o repositório precisa estar importado como Git folder.

# COMMAND ----------

# ============================================================
# CONFIGURAÇÃO E IMPORT DAS CONSULTAS
# ============================================================
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("schema_bronze", "tc2_bronze", "Schema Bronze")
dbutils.widgets.text("schema_silver", "tc2_silver", "Schema Silver")
dbutils.widgets.text("schema_gold", "tc2_gold", "Schema Gold")
dbutils.widgets.text("schema_landing", "tc2_landing", "Schema Landing")
dbutils.widgets.text("volume", "arquivos", "Volume")
dbutils.widgets.text("schema_monitoramento", "tc2_monitoramento", "Schema de monitoramento")
dbutils.widgets.text("tabelas", "todas", "Tabelas a gerar")

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")
SCHEMA_LANDING = dbutils.widgets.get("schema_landing")
VOLUME = f"/Volumes/{CATALOGO}/{SCHEMA_LANDING}/{dbutils.widgets.get('volume')}"
SCHEMA_MONITORAMENTO = dbutils.widgets.get("schema_monitoramento")
SELECAO_TABELAS = dbutils.widgets.get("tabelas")

for schema in (SCHEMA_GOLD, SCHEMA_MONITORAMENTO):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema}")
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def localizar_raiz_repositorio():
    inicio = Path(os.getcwd()).resolve()
    for candidato in [inicio, *inicio.parents]:
        if (candidato / "src" / "gold" / "consultas.py").exists():
            return candidato
    return None


RAIZ = localizar_raiz_repositorio()
if RAIZ is None:
    raise Exception(
        "Nao encontrei src/gold/consultas.py.\n"
        "Este notebook precisa rodar de dentro do repositorio importado como Git folder:\n"
        "  Workspace > Create > Git folder > URL do repositorio\n"
        "Depois abra notebooks/03_gold a partir da pasta criada."
    )

sys.path.insert(0, str(RAIZ))
from src.gold.consultas import CONSULTAS_GOLD
from src.utils.monitoramento import Monitor

monitor = Monitor(spark, CATALOGO, SCHEMA_MONITORAMENTO, "03_gold")

print(f"Repositorio: {RAIZ}")
print(f"Consultas carregadas: {len(CONSULTAS_GOLD)}")
for nome, spec in CONSULTAS_GOLD.items():
    print(f"  - {nome:32s} {spec['pergunta']}")

# COMMAND ----------

# ============================================================
# VIEWS DA SILVER
# ============================================================
# As consultas usam nomes genericos comecando com silver_. As views abaixo
# ligam esses nomes as tabelas Delta correspondentes.
VIEWS = {
    "silver_alfabetizacao_municipio": f"{CATALOGO}.{SCHEMA_SILVER}.fato_alfabetizacao_municipio",
    "silver_indicador_uf":            f"{CATALOGO}.{SCHEMA_SILVER}.fato_indicador_uf",
    "silver_dim_municipio":           f"{CATALOGO}.{SCHEMA_SILVER}.dim_municipio",
    "silver_dim_uf":                  f"{CATALOGO}.{SCHEMA_SILVER}.dim_uf",
    "silver_meta_municipio":          f"{CATALOGO}.{SCHEMA_SILVER}.dim_meta_municipio",
    "silver_meta_uf":                 f"{CATALOGO}.{SCHEMA_SILVER}.dim_meta_uf",
    "silver_distribuicao_nivel":      f"{CATALOGO}.{SCHEMA_SILVER}.fato_distribuicao_nivel",
    "silver_aluno":                   f"{CATALOGO}.{SCHEMA_SILVER}.fato_aluno",
    "silver_avaliacao_stream":        f"{CATALOGO}.{SCHEMA_SILVER}.fato_avaliacao_stream",
}

registradas = set()
for view, tabela in VIEWS.items():
    if spark.catalog.tableExists(tabela):
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS SELECT * FROM {tabela}")
        registradas.add(view)
        print(f"{view:34s} -> {tabela}")
    else:
        print(f"{view:34s} -- ausente, consultas dependentes serao puladas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Construção da Gold
# MAGIC
# MAGIC Cada consulta vira uma tabela Delta em tc2_gold.
# MAGIC
# MAGIC Não usei particionamento físico. Para tabelas dessa ordem de grandeza, particionar criaria
# MAGIC arquivos pequenos demais e a leitura pioraria. O data skipping por estatísticas de arquivo
# MAGIC do Delta já resolve nesse volume.

# COMMAND ----------

# ============================================================
# EXECUÇÃO DAS CONSULTAS
# ============================================================
import time

resultados = []
alvos = (list(CONSULTAS_GOLD) if SELECAO_TABELAS.strip().lower() == "todas"
         else [t.strip() for t in SELECAO_TABELAS.split(",") if t.strip()])

for nome in alvos:
    spec = CONSULTAS_GOLD[nome]
    dependencias = [v for v in VIEWS if v in spec["sql"]]
    ausentes = [v for v in dependencias if v not in registradas]
    if ausentes:
        print(f"[GOLD] {nome:32s} PULADA (falta {', '.join(ausentes)})")
        continue

    inicio = time.perf_counter()
    try:
        with monitor.etapa(f"gold:{nome}", camada="gold") as etapa:
            df = spark.sql(spec["sql"])
            destino = f"{CATALOGO}.{SCHEMA_GOLD}.{nome}"
            (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino))
            linhas = spark.table(destino).count()
            etapa.saida(linhas)
        duracao = time.perf_counter() - inicio
        resultados.append((nome, linhas, round(duracao, 2), spec["pergunta"]))
        print(f"[GOLD] {nome:32s} {linhas:>7,} linhas | {duracao:5.2f}s".replace(",", "."))
    except Exception as exc:
        if spec.get("opcional"):
            print(f"[GOLD] {nome:32s} FALHOU (opcional): {exc}")
        else:
            raise

display(spark.createDataFrame(resultados, ["tabela", "linhas", "segundos", "pergunta"]))

# COMMAND ----------

# ============================================================
# QUALITY GATE DA GOLD
# ============================================================
# A Gold e a camada que o gestor e o modelo consomem, entao ela tambem passa
# pelas verificacoes.
indicador = spark.table(f"{CATALOGO}.{SCHEMA_GOLD}.gold_indicador_municipio")

verificacoes = []
total = indicador.count()
verificacoes.append(("row_count", total >= 10000, f"{total} linhas"))

dups = total - indicador.select("ano", "id_municipio", "rede").distinct().count()
verificacoes.append(("chave_composta", dups == 0, f"{dups} duplicatas no grao"))

nulos = indicador.filter(F.col("nome_municipio").isNull() | F.col("sigla_uf").isNull()).count()
verificacoes.append(("not_null:territorio", nulos == 0, f"{nulos} linhas sem territorio"))

fora_dominio = indicador.filter(
    ~F.col("risco_alfabetizacao").isin("Critico", "Alto", "Medio", "Baixo")).count()
verificacoes.append(("dominio:risco", fora_dominio == 0, f"{fora_dominio} fora do dominio"))

fora_faixa = indicador.filter(~F.col("taxa_alfabetizacao").between(0, 100)).count()
verificacoes.append(("faixa:taxa", fora_faixa == 0, f"{fora_faixa} fora de [0,100]"))

# Consistencia entre ciclos. Uma UF que varia de forma muito diferente do resto
# do pais costuma indicar mudanca de metodologia na origem.
evolucao = spark.table(f"{CATALOGO}.{SCHEMA_GOLD}.gold_evolucao_temporal").filter(
    (F.col("rede_nome") == "Municipal") & F.col("variacao_pp").isNotNull())
mediana = evolucao.approxQuantile("variacao_pp", [0.5], 0.01)[0]
por_uf = evolucao.groupBy("sigla_uf").agg(F.avg("variacao_pp").alias("media"))
anomalas = por_uf.filter(F.abs(F.col("media") - F.lit(mediana)) > 15).collect()

falhas = [f"{nome}: {detalhe}" for nome, ok, detalhe in verificacoes if not ok]
display(spark.createDataFrame(
    [(n, "PASS" if ok else "FAIL", d) for n, ok, d in verificacoes],
    ["verificacao", "status", "detalhe"]))

if anomalas:
    lista = ", ".join(f"{r['sigla_uf']} ({r['media']:+.1f} p.p.)" for r in anomalas)
    print(f"\n[AVISO] Possivel quebra de serie historica em: {lista}")
    print(f"        Mediana nacional: {mediana:+.2f} p.p.")
    print("        Tratado explicitamente no notebook 04.")

if falhas:
    raise Exception("[QUALITY GATE] Gold reprovada:\n  - " + "\n  - ".join(falhas))
print(f"\nQuality gate da Gold: {len(verificacoes)} verificacoes aprovadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Otimização das tabelas
# MAGIC
# MAGIC O OPTIMIZE junta os arquivos pequenos e o ZORDER reorganiza os dados pelas colunas que mais
# MAGIC aparecem em filtro, o que melhora o data skipping.
# MAGIC
# MAGIC É o que substitui o particionamento físico aqui, com a vantagem de não travar o layout numa
# MAGIC coluna só.

# COMMAND ----------

# ============================================================
# OPTIMIZE + ZORDER
# ============================================================
OTIMIZAR = {
    "gold_indicador_municipio": "ano, sigla_uf",
    "gold_meta_vs_realizado": "ano, sigla_uf",
    "gold_evolucao_temporal": "sigla_uf",
}

for tabela, colunas in OTIMIZAR.items():
    nome = f"{CATALOGO}.{SCHEMA_GOLD}.{tabela}"
    try:
        spark.sql(f"OPTIMIZE {nome} ZORDER BY ({colunas})")
        print(f"OPTIMIZE {tabela:32s} ZORDER BY ({colunas})")
    except Exception as exc:
        print(f"OPTIMIZE {tabela:32s} indisponivel neste workspace: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas de negócio
# MAGIC
# MAGIC Quatro perguntas que a Gold responde direto, sem transformação adicional:
# MAGIC
# MAGIC 1. Onde intervir primeiro, listando os municípios em risco crítico do pior para o melhor
# MAGIC 2. Quais estados estão na rota da meta e quais concentram municípios críticos
# MAGIC 3. Quanto cada região precisa avançar por ano para chegar em 2030
# MAGIC 4. Municípios que atingiram a meta do ano mas estão abaixo da média da própria UF
# MAGIC
# MAGIC As definições vêm de src/gold/consultas.py, então dá para colar o mesmo SQL no editor do
# MAGIC Databricks e montar um dashboard em cima.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Municípios em risco crítico (prioridade de intervenção)
# MAGIC SELECT sigla_uf, nome_municipio, taxa_alfabetizacao, meta_taxa_ano, gap_meta_2030
# MAGIC FROM workspace.tc2_gold.gold_indicador_municipio
# MAGIC WHERE ano = 2024 AND rede_nome = 'Municipal' AND risco_alfabetizacao = 'Critico'
# MAGIC ORDER BY taxa_alfabetizacao ASC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. Ranking de estados por percentual de municípios dentro da meta
# MAGIC SELECT sigla_uf, nome_regiao, municipios, taxa_media,
# MAGIC        pct_municipios_na_meta, municipios_criticos
# MAGIC FROM workspace.tc2_gold.gold_meta_vs_realizado
# MAGIC WHERE ano = 2024 AND rede_nome = 'Municipal'
# MAGIC ORDER BY pct_municipios_na_meta DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Esforço anual necessário até 2030, por região
# MAGIC SELECT nome_regiao,
# MAGIC        COUNT(*) AS municipios,
# MAGIC        ROUND(AVG(taxa_alfabetizacao), 2) AS taxa_media,
# MAGIC        ROUND(AVG(ritmo_anual_necessario_pp), 2) AS ritmo_anual_necessario_pp
# MAGIC FROM workspace.tc2_gold.gold_indicador_municipio
# MAGIC WHERE ano = 2024 AND rede_nome = 'Municipal'
# MAGIC GROUP BY nome_regiao
# MAGIC ORDER BY ritmo_anual_necessario_pp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. Bom no absoluto, ruim no relativo: atingiu a meta do ano mas está abaixo da média da UF
# MAGIC SELECT sigla_uf, nome_municipio, taxa_alfabetizacao, taxa_uf, dif_vs_uf
# MAGIC FROM workspace.tc2_gold.gold_indicador_municipio
# MAGIC WHERE ano = 2024 AND rede_nome = 'Municipal'
# MAGIC   AND situacao_meta = 'Meta atingida' AND dif_vs_uf < 0
# MAGIC ORDER BY dif_vs_uf ASC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## FinOps
# MAGIC
# MAGIC A ideia aqui não é escolher a configuração mais barata. É entender o que a nuvem cobra e
# MAGIC desenhar a pipeline em volta disso.
# MAGIC
# MAGIC | O que é cobrado | Como reduzir |
# MAGIC |---|---|
# MAGIC | GB armazenado por mês | Parquet comprimido e ciclo de vida por camada |
# MAGIC | bytes escaneados na consulta | projeção de colunas, particionamento e data skipping |
# MAGIC | DBU-hora de processamento | menos shuffle, menos releitura, job bem dimensionado |
# MAGIC | hora de execução do streaming | ligar só na janela em que há evento chegando |
# MAGIC
# MAGIC A segunda linha é a que mais pesa, porque a cobrança é por byte lido. A célula abaixo mede
# MAGIC isso nos próprios dados: a mesma pergunta respondida de quatro formas, e quantos bytes cada
# MAGIC uma obriga a ler.

# COMMAND ----------

# ============================================================
# BYTES ESCANEADOS: A MESMA PERGUNTA, QUATRO CUSTOS
# ============================================================
# Pergunta: taxa de alfabetizacao por municipio na rede Municipal em 2024.
# Sao 4 colunas de 1 ano. A medicao abaixo mostra quantos bytes cada estrategia
# obriga a ler, que e o que a cobranca por byte escaneado leva em conta.
import pyarrow.parquet as pq

COLUNAS_NECESSARIAS = {"id_municipio", "nome_municipio", "taxa_alfabetizacao", "rede_nome"}
ANO_ALVO = 2024
DIR_MEDICAO = f"{VOLUME}/_finops/gold_indicador_municipio"


def formatar_bytes(n):
    for unidade in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unidade}"
        n /= 1024
    return f"{n:.1f} TB"


def bytes_das_colunas(caminho, colunas=None):
    """Conta os bytes lidos quando so algumas colunas sao projetadas.

    Le o rodape do Parquet, que guarda o tamanho comprimido de cada coluna em
    cada row group. E o mesmo mecanismo que faz um motor serverless cobrar so pelo lido.
    """
    total = 0
    for arquivo in sorted(Path(caminho).rglob("*.parquet")):
        metadados = pq.ParquetFile(arquivo).metadata
        nomes = [metadados.schema.column(i).name for i in range(metadados.num_columns)]
        for grupo in range(metadados.num_row_groups):
            for indice, nome in enumerate(nomes):
                if colunas is None or nome in colunas:
                    total += metadados.row_group(grupo).column(indice).total_compressed_size
    return total


# Os arquivos de uma tabela Delta gerenciada nao ficam acessiveis pelo sistema de
# arquivos. Para medir os bytes, a tabela e gravada em Parquet particionado no
# volume, que e o mesmo formato fisico que o Delta usa por baixo.
(spark.table(f"{CATALOGO}.{SCHEMA_GOLD}.gold_indicador_municipio")
 .write.mode("overwrite").partitionBy("ano").parquet(DIR_MEDICAO))

csv_origem = f"{VOLUME}/br_inep_avaliacao_alfabetizacao_municipio.csv"
bytes_csv = [a.size for a in dbutils.fs.ls(csv_origem)][0]

bytes_parquet_total = bytes_das_colunas(DIR_MEDICAO)
particao_alvo = f"{DIR_MEDICAO}/ano={ANO_ALVO}"
bytes_particao = bytes_das_colunas(particao_alvo)
bytes_otimizado = bytes_das_colunas(particao_alvo, COLUNAS_NECESSARIAS)

PRECO_POR_TB_ESCANEADO = 5.00
CONSULTAS_POR_MES = 500


def custo_consulta(bytes_lidos, consultas=CONSULTAS_POR_MES):
    return bytes_lidos * consultas / (1024 ** 4) * PRECO_POR_TB_ESCANEADO


estrategias = [
    ("1. CSV, varredura completa", bytes_csv),
    ("2. Parquet sem particionamento", bytes_parquet_total),
    (f"3. Parquet + particao (ano={ANO_ALVO})", bytes_particao),
    ("4. Parquet + particao + projecao de colunas", bytes_otimizado),
]
tabela_estrategias = spark.createDataFrame(
    [(nome, formatar_bytes(b), f"{b / bytes_csv * 100:.1f}%", round(custo_consulta(b), 4))
     for nome, b in estrategias],
    ["estrategia", "lido", "vs_csv", f"custo_{CONSULTAS_POR_MES}_consultas_usd"])

print("BYTES LIDOS PARA RESPONDER A MESMA PERGUNTA")
display(tabela_estrategias)

fator = bytes_csv / max(bytes_otimizado, 1)
print(f"A estrategia 4 le {fator:.0f}x menos bytes que a 1, com o mesmo resultado.")
print("Em cobranca por byte escaneado, essa e a razao entre as duas contas no fim do mes.")

dbutils.fs.rm(f"{VOLUME}/_finops", True)   # a copia so existia para a medicao

# COMMAND ----------

# ============================================================
# ESTIMATIVA DE CUSTO MENSAL
# ============================================================
# O Databricks cobra por DBU consumida e por armazenamento. Como o monitoramento
# ja mediu a duracao de cada etapa, a estimativa usa esses tempos.
#
# Os precos abaixo sao de referencia e precisam ser conferidos na calculadora do
# Databricks antes de virarem orcamento.
PRECOS = {
    "dbu_jobs_serverless_hora": 0.15,   # USD por DBU-hora, Jobs Compute serverless
    "dbu_por_hora_de_job":      4.0,    # DBU consumidas por hora pelo tamanho minimo
    "storage_gb_mes":           0.023,  # object storage, classe padrao
}

# Premissas de operacao. O dado oficial sai uma vez por ano, mas revisoes de meta
# e correcoes da fonte chegam ao longo dele.
PREMISSAS = {
    "execucoes_batch_mes":   4,    # uma por semana, para capturar republicacoes
    "semanas_streaming_ano": 6,    # janela de aplicacao da avaliacao
    "execucoes_stream_dia":  24,   # de hora em hora, durante a janela
}


def bytes_do_schema(schema):
    """Devolve o tamanho fisico somado das tabelas Delta de um schema."""
    total = 0
    for t in spark.sql(f"SHOW TABLES IN {CATALOGO}.{schema}").collect():
        if t.isTemporary:
            continue
        detalhe = spark.sql(f"DESCRIBE DETAIL {CATALOGO}.{schema}.{t.tableName}").collect()[0]
        total += detalhe["sizeInBytes"] or 0
    return total


def horas_medidas(pipeline):
    """Le do monitoramento quanto tempo a ultima execucao do notebook levou."""
    linha = spark.sql(f"""
        SELECT COALESCE(SUM(duracao_s), 0) / 3600.0 AS horas
        FROM   {CATALOGO}.{SCHEMA_MONITORAMENTO}.execucao_etapa
        WHERE  pipeline = '{pipeline}'
          AND  run_id = (SELECT MAX(run_id)
                         FROM {CATALOGO}.{SCHEMA_MONITORAMENTO}.execucao_etapa
                         WHERE pipeline = '{pipeline}')
    """).collect()
    return float(linha[0]["horas"]) if linha else 0.0


def custo_por_hora(horas):
    return horas * PRECOS["dbu_por_hora_de_job"] * PRECOS["dbu_jobs_serverless_hora"]


horas_batch = horas_medidas("01_bronze_silver") + horas_medidas("03_gold")
horas_stream = horas_medidas("02_streaming")

gb_total = sum(bytes_do_schema(s) for s in (SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD)) / (1024 ** 3)

custo_batch = custo_por_hora(horas_batch) * PREMISSAS["execucoes_batch_mes"]
execucoes_stream_mes = (PREMISSAS["semanas_streaming_ano"] * 7
                        * PREMISSAS["execucoes_stream_dia"] / 12)
custo_stream = custo_por_hora(horas_stream) * execucoes_stream_mes
custo_storage = gb_total * PRECOS["storage_gb_mes"]

componentes = [
    ("Processamento batch (Bronze, Silver e Gold)",
     f"{horas_batch * 60:.1f} min x {PREMISSAS['execucoes_batch_mes']}/mes", custo_batch),
    ("Processamento streaming (janela de aplicacao)",
     f"{horas_stream * 60:.1f} min x {execucoes_stream_mes:.0f}/mes", custo_stream),
    ("Armazenamento das tres camadas", f"{gb_total:.2f} GB", custo_storage),
]
total_mes = sum(c[2] for c in componentes)

print("ESTIMATIVA DE CUSTO MENSAL")
display(spark.createDataFrame([(n, d, round(c, 4)) for n, d, c in componentes],
                              ["componente", "dimensionamento", "custo_mes_usd"]))
print(f"TOTAL ESTIMADO: US$ {total_mes:.2f}/mes  (~US$ {total_mes * 12:.2f}/ano)")
print("A execucao atual roda no Databricks Free Edition e nao gera cobranca.")

# Comparacao com o cenario de streaming ligado o tempo todo
custo_stream_continuo = custo_por_hora(horas_stream) * 730
economia_anual = (custo_stream_continuo - custo_stream) * 12
print()
print(f"Streaming ligado o ano inteiro custaria US$ {custo_stream_continuo:.2f}/mes.")
print(f"Restringir a janela de aplicacao economiza US$ {economia_anual:,.2f}/ano"
      .replace(",", "."))
print("E a maior decisao de FinOps do projeto, e ela e arquitetural, nao de configuracao.")

# COMMAND ----------

# ============================================================
# INVENTÁRIO FINAL
# ============================================================
linhas = []
for schema in (SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_GOLD):
    # O isTemporary tira as views de sessao criadas la em cima. Elas aparecem no
    # SHOW TABLES de qualquer schema, mas nao pertencem a nenhum.
    for t in spark.sql(f"SHOW TABLES IN {CATALOGO}.{schema}").collect():
        if t.isTemporary:
            continue
        nome = f"{CATALOGO}.{schema}.{t.tableName}"
        linhas.append((schema, t.tableName, spark.table(nome).count()))

inventario = spark.createDataFrame(linhas, ["camada", "tabela", "linhas"])
print(f"{inventario.count()} tabelas | "
      f"{inventario.agg(F.sum('linhas')).collect()[0][0]:,} linhas no total".replace(",", "."))
display(inventario.orderBy("camada", "tabela"))

print("\nCamadas:")
print(f"  {SCHEMA_BRONZE}: dados como vieram da fonte, com metadados de linhagem")
print(f"  {SCHEMA_SILVER}: limpos, padronizados e integrados, no grao de municipio e de aluno")
print(f"  {SCHEMA_GOLD}: agregados por pergunta de negocio, prontos para BI e modelagem")

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
