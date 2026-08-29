# Databricks notebook source
# MAGIC %md
# MAGIC # Tech Challenge Fase 2
# MAGIC ## Notebook 01 - Bronze e Silver
# MAGIC
# MAGIC Primeiro dos quatro notebooks. Ele lê os arquivos de origem, grava a camada Bronze sem
# MAGIC mexer no conteúdo, roda as verificações de qualidade e monta a Silver já com as seis
# MAGIC fontes integradas.
# MAGIC
# MAGIC Tudo fica em tabelas Delta no Unity Catalog. A escolha do Delta foi por causa do MERGE,
# MAGIC que o notebook 02 precisa, e do histórico de versões, que dá para consultar com
# MAGIC DESCRIBE HISTORY depois.
# MAGIC
# MAGIC ### Antes de rodar
# MAGIC
# MAGIC 1. O volume precisa existir e ter os 8 arquivos: 7 CSVs e o Parquet dos microdados de
# MAGIC    aluno. A primeira célula cria o volume e a segunda diz o que está faltando.
# MAGIC 2. O repositório precisa estar importado como Git folder, senão o import de src/ falha.

# COMMAND ----------

# ============================================================
# PARÂMETROS DO JOB
# ============================================================
# Os parametros ficam em widgets. Aparecem como campos editaveis no topo do
# notebook e a tarefa do Lakeflow consegue sobrescrever cada um sem editar o
# codigo, o que permite reprocessar num schema diferente.
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.window import Window

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("schema_landing", "tc2_landing", "Schema de landing")
dbutils.widgets.text("schema_bronze", "tc2_bronze", "Schema Bronze")
dbutils.widgets.text("schema_silver", "tc2_silver", "Schema Silver")
dbutils.widgets.text("schema_monitoramento", "tc2_monitoramento", "Schema de monitoramento")
dbutils.widgets.text("volume", "arquivos", "Volume dos arquivos")

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA_LANDING = dbutils.widgets.get("schema_landing")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
SCHEMA_MONITORAMENTO = dbutils.widgets.get("schema_monitoramento")
VOLUME_NOME = dbutils.widgets.get("volume")

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

PONTO_CORTE_SAEB = 743
CICLO_ESPERADO = 2024   # ultimo ciclo publicado do indicador
META_NACIONAL_2030 = 80.0
DIM_REDE = {0: "Total", 1: "Federal", 2: "Estadual", 3: "Municipal", 4: "Privada", 5: "Publica"}
REDE_TEXTO = {"municipal": 3, "publica": 5, "pública": 5, "estadual": 2, "federal": 1, "privada": 4}

for schema in (SCHEMA_LANDING, SCHEMA_BRONZE, SCHEMA_SILVER, SCHEMA_MONITORAMENTO):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA_LANDING}.{VOLUME_NOME}")

VOLUME = f"/Volumes/{CATALOGO}/{SCHEMA_LANDING}/{VOLUME_NOME}"


def localizar_raiz_repositorio():
    """Acha a raiz do repositorio a partir do diretorio de execucao."""
    inicio = Path(os.getcwd()).resolve()
    for candidato in [inicio, *inicio.parents]:
        if (candidato / "src" / "quality" / "verificacoes.py").exists():
            return candidato
    return None


RAIZ = localizar_raiz_repositorio()
if RAIZ is None:
    raise Exception(
        "Nao encontrei src/quality/verificacoes.py.\n"
        "Este notebook precisa rodar de dentro do repositorio importado como Git folder: "
        "Workspace > Create > Git folder."
    )
sys.path.insert(0, str(RAIZ))

from src.quality.verificacoes import Verificador, consolidar, resumo
from src.utils.monitoramento import Monitor

monitor = Monitor(spark, CATALOGO, SCHEMA_MONITORAMENTO, "01_bronze_silver", run_id=RUN_TS)

print(f"Repositorio       : {RAIZ}")
print(f"Volume de landing : {VOLUME}")
print(f"Run               : {RUN_TS}")

# COMMAND ----------

# ============================================================
# CONFERÊNCIA DOS ARQUIVOS ENVIADOS
# ============================================================
ARQUIVOS = {
    "indicador_municipio": "br_inep_avaliacao_alfabetizacao_municipio.csv",
    "indicador_uf":        "br_inep_avaliacao_alfabetizacao_uf.csv",
    "meta_brasil":         "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv",
    "meta_uf":             "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
    "meta_municipio":      "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
}

# Microdados de aluno: 3,87 milhões de linhas, em Parquet
ARQUIVO_ALUNOS = "alunos.parquet"

presentes = {f.name for f in dbutils.fs.ls(VOLUME)}
faltando = [a for a in list(ARQUIVOS.values()) + [ARQUIVO_ALUNOS] if a not in presentes]

if faltando:
    raise Exception(
        f"Faltam {len(faltando)} arquivo(s) no volume {VOLUME}:\n  - "
        + "\n  - ".join(faltando)
        + f"\n\nEnvie-os pela UI: Catalog > {SCHEMA_LANDING} > {VOLUME_NOME} > "
          "Upload to this volume."
    )

print(f"Os {len(ARQUIVOS) + 1} arquivos estão no volume.")
display(dbutils.fs.ls(VOLUME))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada Bronze
# MAGIC
# MAGIC Três formatos de origem entram em lote: os cinco arquivos de indicador e de metas são CSV,
# MAGIC os microdados de aluno vêm em Parquet e o território vem em JSON, pela API do IBGE.
# MAGIC
# MAGIC Em número de arquivos o CSV é a maioria, mas em volume não. O Parquet dos microdados tem
# MAGIC 3,87 milhões de linhas contra 35 mil de todos os CSVs somados, então quase tudo que entra
# MAGIC em lote está em Parquet. A célula abaixo imprime essa divisão.
# MAGIC
# MAGIC Aqui o dado entra como veio, sem dropna e sem rename. As únicas colunas acrescentadas
# MAGIC começam com underline e servem para saber depois de onde cada linha veio: quando foi
# MAGIC ingerida, qual arquivo a originou, qual execução a gerou e um hash do conteúdo.
# MAGIC
# MAGIC O hash serve para perceber se a fonte mudou sem avisar. Se o arquivo for republicado com
# MAGIC correção, as linhas alteradas passam a ter hash diferente.

# COMMAND ----------

# ============================================================
# INGESTÃO PARA A BRONZE
# ============================================================
def com_linhagem(df, entidade, origem):
    return (df
            .withColumn("_ingestion_timestamp", F.lit(RUN_TS))
            .withColumn("_ingestion_date", F.lit(INGESTION_DATE))
            .withColumn("_source", F.lit(origem))
            .withColumn("_source_format", F.lit("csv"))
            .withColumn("_entity", F.lit(entidade))
            .withColumn("_record_hash", F.sha2(F.concat_ws("||", *df.columns), 256)))


def gravar(df, schema, tabela):
    nome = f"{CATALOGO}.{schema}.{tabela}"
    (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nome))
    return nome


resumo_bronze = []
with monitor.etapa("ingestao_arquivos", camada="bronze") as etapa:
    etapa.entrada(len(ARQUIVOS) + 1)

    # Os cinco arquivos de indicador e de metas sao CSV. Sao pequenos, entao a
    # inferencia de schema nao pesa.
    for entidade, arquivo in ARQUIVOS.items():
        bruto = (spark.read
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .csv(f"{VOLUME}/{arquivo}"))
        nome = gravar(com_linhagem(bruto, entidade, arquivo), SCHEMA_BRONZE, entidade)
        resumo_bronze.append((entidade, "csv", spark.table(nome).count(),
                              len(bruto.columns), nome))
        print(f"[BRONZE] {entidade:22s} {'csv':8s} {resumo_bronze[-1][2]:>9,} linhas"
              .replace(",", "."))

    # Os microdados de aluno vem em Parquet. Sao 3,87 milhoes de linhas, e o
    # Parquet ja traz o schema, entao entram sem inferencia.
    alunos_bruto = spark.read.parquet(f"{VOLUME}/{ARQUIVO_ALUNOS}")
    nome = gravar(com_linhagem(alunos_bruto, "aluno", ARQUIVO_ALUNOS)
                  .withColumn("_source_format", F.lit("parquet")), SCHEMA_BRONZE, "aluno")
    resumo_bronze.append(("aluno", "parquet", spark.table(nome).count(),
                          len(alunos_bruto.columns), nome))
    print(f"[BRONZE] {'aluno':22s} {'parquet':8s} {resumo_bronze[-1][2]:>9,} linhas"
          .replace(",", "."))

    etapa.saida(sum(linhas for _, _, linhas, _, _ in resumo_bronze))

display(spark.createDataFrame(
    resumo_bronze, ["entidade", "formato", "linhas", "colunas", "tabela"]))

# Peso de cada formato no que entra em lote. O arquivo de aluno sozinho responde
# por quase todo o volume, entao a ingestao em lote e majoritariamente Parquet,
# mesmo tendo mais arquivos CSV do que Parquet.
por_formato = {}
for _, formato, linhas, _, _ in resumo_bronze:
    por_formato[formato] = por_formato.get(formato, 0) + linhas
total_lote = sum(por_formato.values())

print()
print("Volume por formato de origem:")
for formato, linhas in sorted(por_formato.items(), key=lambda x: -x[1]):
    print(f"  {formato:8s} {linhas:>9,} linhas  {linhas / total_lote * 100:5.1f}%"
          .replace(",", "."))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Território pela API do IBGE
# MAGIC
# MAGIC As tabelas do Inep só trazem o código IBGE de sete dígitos. Sem nome de município, UF e
# MAGIC região não dá para fazer nenhum recorte territorial, então foi preciso buscar isso fora.
# MAGIC
# MAGIC A fonte é a API pública de Localidades do IBGE. É a integração com fonte externa da
# MAGIC pipeline, e a única origem que não vem do Inep.
# MAGIC
# MAGIC O serverless do Databricks tem restrição de saída para internet, então a chamada pode não
# MAGIC completar. Se isso acontecer, a célula usa o CSV que está no volume, que é o mesmo payload
# MAGIC baixado antes, e grava um alerta dizendo qual origem foi usada. A coluna _source_format
# MAGIC registra se veio da API ou do arquivo.

# COMMAND ----------

# ============================================================
# INGESTÃO DO IBGE: API COM QUEDA PARA O ARQUIVO
# ============================================================
import pandas as pd

API_IBGE = "https://servicodados.ibge.gov.br/api/v1/localidades"
IBGE = {
    "dim_uf_ibge":        (f"{API_IBGE}/estados",    "ibge_estados.csv",    27),
    "dim_municipio_ibge": (f"{API_IBGE}/municipios", "ibge_municipios.csv", 5500),
}


def buscar_na_api(url, tentativas=3, espera=2):
    """Chama a API e devolve o JSON achatado, ou None se a rede nao permitir."""
    import time

    import requests

    for tentativa in range(1, tentativas + 1):
        try:
            resposta = requests.get(url, timeout=20)
            resposta.raise_for_status()
            return pd.json_normalize(resposta.json(), sep="_")
        except Exception as erro:
            if tentativa == tentativas:
                print(f"  API indisponivel apos {tentativas} tentativas: "
                      f"{type(erro).__name__}: {erro}")
                return None
            time.sleep(espera * tentativa)   # espera crescente entre tentativas


with monitor.etapa("ingestao_ibge", camada="bronze") as etapa:
    origens, total_territorios = [], 0
    for entidade, (url, arquivo, minimo) in IBGE.items():
        achatado = buscar_na_api(url)

        if achatado is not None and len(achatado) >= minimo:
            df = spark.createDataFrame(achatado.astype(str))
            origem, formato = url, "json_api"
        else:
            df = (spark.read.option("header", "true").option("inferSchema", "true")
                  .csv(f"{VOLUME}/{arquivo}"))
            origem, formato = arquivo, "csv"
            monitor.alerta("BAIXA", f"{entidade} veio do volume: API do IBGE inacessivel")

        nome = gravar(com_linhagem(df, entidade, origem).withColumn(
            "_source_format", F.lit(formato)), SCHEMA_BRONZE, entidade)
        linhas = spark.table(nome).count()
        total_territorios += linhas
        origens.append((entidade, formato, linhas, origem))
        print(f"[BRONZE] {entidade:22s} {linhas:>9,} linhas  via {formato}".replace(",", "."))

    etapa.saida(total_territorios)

display(spark.createDataFrame(origens, ["entidade", "origem", "linhas", "endereco"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificações de qualidade da Bronze
# MAGIC
# MAGIC As regras estão em src/quality/verificacoes.py, escritas em Spark. Cobrem duplicidade,
# MAGIC valores ausentes, chaves de relacionamento e consistência entre tabelas. Ficam fora do
# MAGIC notebook porque os quatro usam as mesmas.
# MAGIC
# MAGIC Cada verificação tem uma severidade. As de nível ERROR param a execução, o que faz a
# MAGIC tarefa do Lakeflow parar também. As de nível WARNING só registram e a pipeline continua.
# MAGIC
# MAGIC A separação existe porque nem todo problema justifica parar tudo. Os níveis de proficiência
# MAGIC que faltam em 2023, por exemplo, são característica da fonte.

# COMMAND ----------

# ============================================================
# VERIFICAÇÕES DE QUALIDADE DA BRONZE
# ============================================================
def tabela_bronze(entidade):
    return spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{entidade}")


with monitor.etapa("quality_gate_bronze", camada="bronze") as etapa:
    municipios_ibge = tabela_bronze("dim_municipio_ibge")
    ufs_ibge = tabela_bronze("dim_uf_ibge")

    gates = []

    gates.append(
        Verificador("indicador_municipio", tabela_bronze("indicador_municipio"), "bronze")
        .linhas_minimas(10000)
        .sem_nulos("id_municipio", "taxa_alfabetizacao")
        .grao_unico("ano", "id_municipio", "serie", "rede")
        .dentro_da_faixa("taxa_alfabetizacao", 0, 100)
        .dentro_do_dominio("rede", DIM_REDE.keys())
        # Integridade referencial: confere se todo municipio do Inep existe no
        # cadastro do IBGE. Se nao existisse, o join adiante descartaria essas
        # linhas em silencio.
        .chave_existe_em("id_municipio", municipios_ibge, "id")
        .contem_ciclo("ano", CICLO_ESPERADO))

    gates.append(
        Verificador("indicador_uf", tabela_bronze("indicador_uf"), "bronze")
        .linhas_minimas(100)
        .sem_nulos("sigla_uf")
        .dentro_da_faixa("taxa_alfabetizacao", 0, 100)
        .chave_existe_em("sigla_uf", ufs_ibge, "sigla"))

    gates.append(
        Verificador("meta_brasil", tabela_bronze("meta_brasil"), "bronze")
        .linhas_minimas(1)
        .sem_nulos("ano"))

    gates.append(
        Verificador("meta_uf", tabela_bronze("meta_uf"), "bronze")
        .linhas_minimas(20)
        .sem_nulos("sigla_uf")
        .chave_existe_em("sigla_uf", ufs_ibge, "sigla"))

    gates.append(
        Verificador("meta_municipio", tabela_bronze("meta_municipio"), "bronze")
        .linhas_minimas(10000)
        .sem_nulos("id_municipio")
        .chave_existe_em("id_municipio", municipios_ibge, "id"))

    gates.append(
        Verificador("dim_uf_ibge", ufs_ibge, "bronze")
        .linhas_minimas(27)
        .valor_unico("id")
        .sem_nulos("sigla", "nome"))

    gates.append(
        Verificador("dim_municipio_ibge", municipios_ibge, "bronze")
        .linhas_minimas(5500)
        .valor_unico("id")
        .sem_nulos("nome"))

    # O grao do aluno e (ano, id_aluno): o mesmo id reaparece no ciclo seguinte,
    # entao unicidade isolada por id_aluno acusaria falso positivo.
    gates.append(
        Verificador("aluno", tabela_bronze("aluno"), "bronze")
        .linhas_minimas(1000)
        .sem_nulos("id_aluno", "id_municipio")
        .grao_unico("ano", "id_aluno")
        .chave_existe_em("id_municipio", municipios_ibge, "id"))

    resultados_bronze = consolidar(gates)
    contagem = resumo(resultados_bronze)
    etapa.entrada(len(resultados_bronze)).saida(contagem["PASS"]).rejeitadas(contagem["FAIL"])

    display(spark.createDataFrame(resultados_bronze))

    for aviso in [r for r in resultados_bronze if r["status"] == "WARN"]:
        monitor.alerta("MEDIA", f"{aviso['tabela']}.{aviso['verificacao']} -> {aviso['detalhe']}")

    falhas = [r for r in resultados_bronze if r["status"] == "FAIL"]
    if falhas:
        raise Exception("[QUALITY GATE] Bronze reprovada:\n  - " + "\n  - ".join(
            f"{f['tabela']}.{f['verificacao']}:{f['coluna']} -> {f['detalhe']}" for f in falhas))

print(f"Quality gate da Bronze: {contagem['PASS']} aprovadas, "
      f"{contagem['WARN']} avisos, {contagem['FAIL']} reprovadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada Silver
# MAGIC
# MAGIC O que acontece aqui: padronização de tipos, decodificação da coluna rede, normalização de
# MAGIC chaves, deduplicação pelo grão de cada tabela e a integração das bases.
# MAGIC
# MAGIC A Silver nunca escreve por cima da Bronze. Se alguma regra estiver errada, dá para corrigir
# MAGIC e reprocessar a partir da Bronze sem precisar reingerir nada.

# COMMAND ----------

# ============================================================
# DIMENSÕES TERRITORIAIS
# ============================================================
def ler_bronze(entidade):
    return spark.table(f"{CATALOGO}.{SCHEMA_BRONZE}.{entidade}")


def mapear_rede(coluna):
    expressao = F.lit(None).cast("string")
    for codigo, nome in DIM_REDE.items():
        expressao = F.when(coluna == codigo, F.lit(nome)).otherwise(expressao)
    return expressao


dim_uf = (ler_bronze("dim_uf_ibge")
          .select(F.col("id").cast("int").alias("id_uf"),
                  F.upper(F.trim(F.col("sigla"))).alias("sigla_uf"),
                  F.trim(F.col("nome")).alias("nome_uf"),
                  F.col("regiao_sigla").alias("sigla_regiao"),
                  F.col("regiao_nome").alias("nome_regiao"))
          .dropDuplicates(["id_uf"]))
gravar(dim_uf, SCHEMA_SILVER, "dim_uf")

# O codigo IBGE tem 7 digitos e os 2 primeiros identificam a UF. Derivar daqui
# funciona ate para municipio que veio sem microrregiao no JSON.
dim_municipio = (ler_bronze("dim_municipio_ibge")
                 .select(F.col("id").cast("int").alias("id_municipio"),
                         F.trim(F.col("nome")).alias("nome_municipio"),
                         F.col("microrregiao_nome").alias("nome_microrregiao"),
                         F.col("microrregiao_mesorregiao_nome").alias("nome_mesorregiao"))
                 .withColumn("id_uf", (F.col("id_municipio") / 100000).cast("int"))
                 .join(broadcast(dim_uf), on="id_uf", how="left")
                 .filter(F.col("sigla_uf").isNotNull()))
gravar(dim_municipio, SCHEMA_SILVER, "dim_municipio")

print(f"dim_uf: {dim_uf.count()} | dim_municipio: {dim_municipio.count()}")

# COMMAND ----------

# ============================================================
# FATOS DO INDICADOR
# ============================================================
COLUNAS_NIVEL = [f"proporcao_aluno_nivel_{i}" for i in range(9)]

indicador_municipio = (ler_bronze("indicador_municipio")
    .withColumn("ano", F.col("ano").cast("int"))
    .withColumn("id_municipio", F.col("id_municipio").cast("int"))
    .withColumn("serie", F.col("serie").cast("int"))
    .withColumn("rede", F.col("rede").cast("int"))
    .withColumn("taxa_alfabetizacao", F.round(F.col("taxa_alfabetizacao").cast("double"), 2))
    .withColumn("media_portugues", F.round(F.col("media_portugues").cast("double"), 2))
    .filter(F.col("ano").isNotNull()
            & F.col("id_municipio").between(1000000, 9999999)
            & F.col("taxa_alfabetizacao").between(0, 100))
    .withColumn("rede_nome", mapear_rede(F.col("rede")))
    .dropDuplicates(["ano", "id_municipio", "serie", "rede"]))

fato_indicador_municipio = (indicador_municipio
    .select("ano", "id_municipio", "serie", "rede", "rede_nome",
            "taxa_alfabetizacao", "media_portugues")
    .withColumn("id_uf", (F.col("id_municipio") / 100000).cast("int"))
    .withColumn("alcancou_meta_2030", F.col("taxa_alfabetizacao") >= F.lit(META_NACIONAL_2030)))
gravar(fato_indicador_municipio, SCHEMA_SILVER, "fato_indicador_municipio")

fato_indicador_uf = (ler_bronze("indicador_uf")
    .withColumn("ano", F.col("ano").cast("int"))
    .withColumn("rede", F.col("rede").cast("int"))
    .withColumn("serie", F.col("serie").cast("int"))
    .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
    .withColumn("taxa_alfabetizacao", F.round(F.col("taxa_alfabetizacao").cast("double"), 2))
    .withColumn("media_portugues", F.round(F.col("media_portugues").cast("double"), 2))
    .withColumn("rede_nome", mapear_rede(F.col("rede")))
    .dropDuplicates(["ano", "sigla_uf", "serie", "rede"])
    .select("ano", "sigla_uf", "serie", "rede", "rede_nome",
            "taxa_alfabetizacao", "media_portugues")
    .join(broadcast(dim_uf.select("sigla_uf", "id_uf", "nome_uf", "nome_regiao")),
          on="sigla_uf", how="left"))
gravar(fato_indicador_uf, SCHEMA_SILVER, "fato_indicador_uf")

# As 9 colunas de proporção viram uma tabela longa (ano, município, rede, nível, proporção)
distribuicao = None
for nivel, coluna in enumerate(COLUNAS_NIVEL):
    parcial = (indicador_municipio
               .select("ano", "id_municipio", "rede", "rede_nome",
                       F.col(coluna).cast("double").alias("proporcao_alunos"))
               .withColumn("nivel", F.lit(nivel))
               .filter(F.col("proporcao_alunos").isNotNull()))
    distribuicao = parcial if distribuicao is None else distribuicao.unionByName(parcial)
gravar(distribuicao, SCHEMA_SILVER, "fato_distribuicao_nivel")

print(f"indicador municipio: {fato_indicador_municipio.count()} | "
      f"indicador uf: {fato_indicador_uf.count()} | "
      f"distribuicao por nivel: {distribuicao.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Microdados de aluno
# MAGIC
# MAGIC São 3,87 milhões de linhas no grão de aluno. É a base que sustenta o indicador publicado.
# MAGIC Ela não cabe no download gratuito do portal da Base dos Dados, então foi extraída do
# MAGIC BigQuery público e enviada ao volume em Parquet. O passo a passo está em
# MAGIC docs/guia_extracao_bigquery.md.
# MAGIC
# MAGIC Três colunas mudam o cálculo do indicador:
# MAGIC
# MAGIC | Coluna | Por que importa |
# MAGIC |---|---|
# MAGIC | presenca e preenchimento_caderno | nem todo matriculado fez a prova, e só quem fez entra na conta |
# MAGIC | peso_aluno | é peso amostral, então a taxa oficial é média ponderada |
# MAGIC | alfabetizado | já vem da fonte; é mantida e conferida contra a regra dos 743 pontos |
# MAGIC
# MAGIC A célula seguinte refaz o indicador municipal partindo do grão de aluno e compara com o
# MAGIC número que o Inep publica. Se a regra estivesse errada, a diferença apareceria ali.

# COMMAND ----------

# ============================================================
# SILVER DE MICRODADOS DE ALUNO
# ============================================================
aluno_bronze = ler_bronze("aluno")

# Na fonte a coluna chama proficiencia. O nome muda aqui, na Silver.
if "proficiencia" in aluno_bronze.columns:
    aluno_bronze = aluno_bronze.withColumnRenamed("proficiencia", "proficiencia_portugues")

fato_aluno = (aluno_bronze
    .withColumn("ano", F.col("ano").cast("int"))
    .withColumn("id_municipio", F.col("id_municipio").cast("int"))
    .withColumn("rede", F.col("rede").cast("int"))
    .withColumn("serie", F.col("serie").cast("int"))
    .withColumn("proficiencia_portugues", F.col("proficiencia_portugues").cast("double"))
    .withColumn("peso_aluno", F.coalesce(F.col("peso_aluno").cast("double"), F.lit(1.0)))
    # Aluno sem prova continua na tabela: ele entra no denominador da participacao.
    .filter(F.col("id_aluno").isNotNull()
            & F.col("id_municipio").between(1000000, 9999999)
            & (F.col("proficiencia_portugues").isNull()
               | F.col("proficiencia_portugues").between(200, 1000)))
    # O grão é (ano, aluno): o mesmo id reaparece no ciclo seguinte.
    .dropDuplicates(["ano", "id_aluno"])
    .withColumn("avaliado", F.col("proficiencia_portugues").isNotNull())
    .withColumn("alfabetizado", F.col("alfabetizado").cast("double") == 1.0)
    .withColumn("rede_nome", mapear_rede(F.col("rede")))
    .withColumn("faixa_proficiencia",
                F.when(F.col("proficiencia_portugues") < 650, "Muito baixa")
                 .when(F.col("proficiencia_portugues") < 700, "Baixa")
                 .when(F.col("proficiencia_portugues") < PONTO_CORTE_SAEB, "Abaixo do corte")
                 .when(F.col("proficiencia_portugues") < 800, "Adequada")
                 .otherwise("Avancada"))
    .select("id_aluno", "ano", "id_municipio", "id_escola", "rede", "rede_nome", "serie",
            "proficiencia_portugues", "alfabetizado", "avaliado", "faixa_proficiencia",
            "presenca", "preenchimento_caderno", "peso_aluno"))

gravar(fato_aluno, SCHEMA_SILVER, "fato_aluno")

total = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_aluno").count()
avaliados = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_aluno").filter("avaliado").count()
print(f"Alunos no cadastro     : {total:,}".replace(",", "."))
print(f"Efetivamente avaliados : {avaliados:,} ({avaliados/total*100:.1f}%)".replace(",", "."))

# COMMAND ----------

# ============================================================
# RECONSTRUÇÃO DO INDICADOR A PARTIR DO GRÃO DE ALUNO
# ============================================================
# Regra oficial: media ponderada por peso_aluno, contando so quem foi avaliado.
# O resultado tem que bater com o numero publicado pelo Inep.
avaliados_df = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_aluno").filter("avaliado")

reconstruido = (avaliados_df
    .groupBy("ano", "id_municipio", "rede")
    .agg((F.sum(F.col("alfabetizado").cast("double") * F.col("peso_aluno"))
          / F.sum("peso_aluno") * 100).alias("taxa_reconstruida"),
         (F.sum(F.col("proficiencia_portugues") * F.col("peso_aluno"))
          / F.sum("peso_aluno")).alias("media_reconstruida"),
         F.count("*").alias("alunos")))

comparacao = (reconstruido
    .join(fato_indicador_municipio.select("ano", "id_municipio", "rede",
                                          "taxa_alfabetizacao", "media_portugues"),
          on=["ano", "id_municipio", "rede"], how="inner")
    .withColumn("erro_taxa", F.abs(F.col("taxa_reconstruida") - F.col("taxa_alfabetizacao")))
    .withColumn("erro_media", F.abs(F.col("media_reconstruida") - F.col("media_portugues"))))

resumo = comparacao.agg(
    F.count("*").alias("combinacoes"),
    F.round(F.avg("erro_taxa"), 4).alias("erro_medio_taxa_pp"),
    F.round(F.avg("erro_media"), 4).alias("erro_medio_proficiencia"),
    F.round(F.avg((F.col("erro_taxa") < 0.05).cast("double")) * 100, 1).alias("pct_dentro_005"),
    F.round(F.abs(
        F.sum(F.col("taxa_reconstruida") * F.col("alunos")) / F.sum("alunos")
        - F.sum(F.col("taxa_alfabetizacao") * F.col("alunos")) / F.sum("alunos")), 4)
     .alias("erro_nacional_pp"),
).collect()[0]

print(f"Combinacoes comparadas (ano x municipio x rede): {resumo['combinacoes']:,}"
      .replace(",", "."))
print(f"Erro medio na taxa        : {resumo['erro_medio_taxa_pp']} p.p.")
print(f"Erro medio na proficiencia: {resumo['erro_medio_proficiencia']} pontos")
print(f"Dentro de 0,05 p.p.       : {resumo['pct_dentro_005']}% dos casos")
print(f"Erro no agregado nacional : {resumo['erro_nacional_pp']} p.p.")

if resumo["erro_nacional_pp"] > 0.5 or resumo["erro_medio_taxa_pp"] > 0.5:
    raise Exception("[QUALITY GATE] A reconstrucao do indicador nao bate com o publicado")
print("\nReconstrucao aprovada: os microdados reproduzem o indicador oficial.")

display(comparacao.orderBy(F.desc("erro_taxa")).limit(10))

# COMMAND ----------

# ============================================================
# TRAJETÓRIAS DE META (colunas por ano -> linhas)
# ============================================================
def normalizar_rede_texto(df):
    expressao = F.lit(None).cast("int")
    for texto, codigo in REDE_TEXTO.items():
        expressao = F.when(F.lower(F.trim(F.col("rede"))) == texto,
                           F.lit(codigo)).otherwise(expressao)
    return df.withColumn("rede", expressao)


def trajetoria_metas(df, chaves):
    colunas_meta = [c for c in df.columns if c.startswith("meta_alfabetizacao_")]
    empilhados = None
    for coluna in colunas_meta:
        ano_meta = int(coluna.split("_")[-1])
        parcial = (df.select(*chaves, "rede", "ano",
                             F.col(coluna).cast("double").alias("meta_taxa"))
                     .withColumn("ano_meta", F.lit(ano_meta))
                     .filter(F.col("meta_taxa").isNotNull()))
        empilhados = parcial if empilhados is None else empilhados.unionByName(parcial)

    # A mesma meta aparece em 2023 e em 2024, as vezes com revisao. Fica valendo
    # a versao declarada no ano mais recente.
    janela = Window.partitionBy(*chaves, "rede", "ano_meta").orderBy(F.col("ano").desc())
    return (empilhados
            .withColumn("_ordem", F.row_number().over(janela))
            .filter(F.col("_ordem") == 1)
            .drop("_ordem", "ano")
            .withColumn("rede_nome", mapear_rede(F.col("rede"))))


meta_municipio = normalizar_rede_texto(
    ler_bronze("meta_municipio").withColumn("id_municipio", F.col("id_municipio").cast("int")))
gravar(trajetoria_metas(meta_municipio, ["id_municipio"]), SCHEMA_SILVER, "dim_meta_municipio")

meta_uf = normalizar_rede_texto(
    ler_bronze("meta_uf").withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf")))))
gravar(trajetoria_metas(meta_uf, ["sigla_uf"]), SCHEMA_SILVER, "dim_meta_uf")

# Participação e nível agregado só existem na tabela de metas municipais
participacao = (normalizar_rede_texto(
        ler_bronze("meta_municipio").withColumn("id_municipio", F.col("id_municipio").cast("int")))
    .select(F.col("ano").cast("int").alias("ano"), "id_municipio", "rede",
            F.col("percentual_participacao").cast("double").alias("percentual_participacao"),
            F.col("nivel_alfabetizacao").cast("double").alias("nivel_alfabetizacao"))
    .filter(F.col("percentual_participacao").isNotNull())
    .dropDuplicates(["ano", "id_municipio", "rede"]))
gravar(participacao, SCHEMA_SILVER, "fato_participacao_municipio")

print("Trajetorias de meta e participacao gravadas.")

# COMMAND ----------

# ============================================================
# TABELA INTEGRADA
# ============================================================
metas_municipio = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.dim_meta_municipio")

meta_2030 = (metas_municipio.filter(F.col("ano_meta") == 2030)
             .select("id_municipio", "rede", F.col("meta_taxa").alias("meta_taxa_2030")))

contexto_uf = (fato_indicador_uf
               .select("ano", "sigla_uf", "rede", F.col("taxa_alfabetizacao").alias("taxa_uf")))

contexto_br = (ler_bronze("meta_brasil")
               .select(F.col("ano").cast("int").alias("ano"),
                       F.col("taxa_alfabetizacao").cast("double").alias("taxa_brasil_publica")))

integrada = (fato_indicador_municipio
    .join(broadcast(dim_municipio.select("id_municipio", "nome_municipio", "sigla_uf",
                                         "nome_uf", "nome_regiao", "nome_mesorregiao")),
          on="id_municipio", how="left")
    .join(metas_municipio.select("id_municipio", "rede",
                                 F.col("ano_meta").alias("ano"),
                                 F.col("meta_taxa").alias("meta_taxa_ano")),
          on=["id_municipio", "rede", "ano"], how="left")
    .join(broadcast(meta_2030), on=["id_municipio", "rede"], how="left")
    .join(participacao, on=["ano", "id_municipio", "rede"], how="left")
    .join(contexto_uf, on=["ano", "sigla_uf", "rede"], how="left")
    .join(broadcast(contexto_br), on="ano", how="left")
    .withColumn("meta_taxa_2030", F.coalesce(F.col("meta_taxa_2030"), F.lit(META_NACIONAL_2030)))
    .withColumn("gap_meta_ano", F.round(F.col("taxa_alfabetizacao") - F.col("meta_taxa_ano"), 2))
    .withColumn("atingiu_meta_ano", F.col("gap_meta_ano") >= 0)
    .withColumn("gap_meta_2030", F.round(F.col("meta_taxa_2030") - F.col("taxa_alfabetizacao"), 2))
    .withColumn("dif_vs_uf", F.round(F.col("taxa_alfabetizacao") - F.col("taxa_uf"), 2))
    .withColumn("dif_vs_brasil",
                F.round(F.col("taxa_alfabetizacao") - F.col("taxa_brasil_publica"), 2)))

# Gate da Silver: o grão precisa ser único e todo município precisa ter território
total = integrada.count()
duplicadas = total - integrada.dropDuplicates(["ano", "id_municipio", "rede"]).count()
sem_territorio = integrada.filter(F.col("sigla_uf").isNull()).count()
print(f"[DQ:SILVER] linhas={total} duplicadas={duplicadas} sem_territorio={sem_territorio}")
if duplicadas or sem_territorio:
    raise Exception(f"[QUALITY GATE] Silver integrada reprovada: {duplicadas} duplicatas, "
                    f"{sem_territorio} linhas sem territorio")

gravar(integrada, SCHEMA_SILVER, "fato_alfabetizacao_municipio")
display(integrada.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consistência entre fontes
# MAGIC
# MAGIC Esta célula confere o mapeamento da coluna rede. A taxa da rede Municipal no indicador tem
# MAGIC que bater com a taxa da tabela de metas municipais.
# MAGIC
# MAGIC O de-para entre o código numérico e o texto não está publicado em nenhum dos arquivos, então
# MAGIC foi deduzido a partir dos dados. Esta comparação é o que dá segurança de que a dedução está
# MAGIC certa: se estivesse errada, daria divergência em quase todas as linhas.
# MAGIC
# MAGIC A tolerância de 0,1 ponto percentual existe porque as duas fontes arredondam de forma
# MAGIC diferente em 2023.

# COMMAND ----------

# ============================================================
# INDICADOR (rede Municipal) x TABELA DE METAS
# ============================================================
esquerda = (integrada.filter(F.col("rede") == 3)
            .select("ano", "id_municipio", "taxa_alfabetizacao"))
direita = (ler_bronze("meta_municipio")
           .select(F.col("ano").cast("int").alias("ano"),
                   F.col("id_municipio").cast("int").alias("id_municipio"),
                   F.col("taxa_alfabetizacao").cast("double").alias("taxa_fonte_meta")))

cruzamento = esquerda.join(direita, on=["ano", "id_municipio"], how="inner").dropna()
divergentes = cruzamento.filter(
    F.abs(F.col("taxa_alfabetizacao") - F.col("taxa_fonte_meta")) > 0.1)

comparados = cruzamento.count()
n_divergentes = divergentes.count()
pct = n_divergentes / comparados * 100 if comparados else 0

print(f"Comparados: {comparados} | divergentes: {n_divergentes} ({pct:.3f}%)")
if pct >= 1.0:
    raise Exception(f"[QUALITY GATE] Mapeamento de rede suspeito: {pct:.2f}% de divergencia")

print("Mapeamento rede=3 -> Municipal confirmado pelos dados.")
if n_divergentes:
    print(f"\nAs {n_divergentes} divergencias abaixo sao inconsistencias da propria fonte "
          f"e seriam reportadas ao produtor do dado:")
    display(divergentes)

# COMMAND ----------

# ============================================================
# RESUMO DA SILVER
# ============================================================
with monitor.etapa("resumo_silver", camada="silver") as etapa:
    tabelas = [t for t in spark.sql(f"SHOW TABLES IN {CATALOGO}.{SCHEMA_SILVER}").collect()
               if not t.isTemporary]
    linhas = [(t.tableName, spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.{t.tableName}").count())
              for t in tabelas]
    etapa.saida(sum(n for _, n in linhas)).nota(f"{len(linhas)} tabelas na Silver")

print(f"SILVER: {len(linhas)} tabelas")
display(spark.createDataFrame(linhas, ["tabela", "linhas"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoramento da execução
# MAGIC
# MAGIC Todas as etapas acima foram cronometradas, com volume de entrada, saída e rejeição
# MAGIC registrados. O painel abaixo mostra a execução atual, e as linhas ficam gravadas em
# MAGIC tc2_monitoramento.execucao_etapa.
# MAGIC
# MAGIC Como fica em tabela, dá para comparar duas execuções por SQL e olhar uma rodada antiga sem
# MAGIC reprocessar nada.
# MAGIC
# MAGIC Se uma etapa levantar exceção, ela é gravada com status FALHA e gera um alerta de
# MAGIC severidade alta. Só depois disso a exceção sobe e interrompe o notebook.

# COMMAND ----------

# ============================================================
# PAINEL DE EXECUÇÃO
# ============================================================
monitor.gravar()
monitor.painel()

print()
print("Historico das ultimas execucoes:")
display(monitor.historico())

print("Proximo: 02_streaming (ingestao de eventos) e 03_gold (camada analitica).")
