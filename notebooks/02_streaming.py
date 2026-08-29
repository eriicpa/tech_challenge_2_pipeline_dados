# Databricks notebook source
# MAGIC %md
# MAGIC # Tech Challenge Fase 2
# MAGIC ## Notebook 02 - Ingestão em streaming
# MAGIC
# MAGIC Pré-requisito: o notebook 01 precisa ter rodado.
# MAGIC
# MAGIC A Free Edition só tem compute serverless, então não há onde subir um broker Kafka. A
# MAGIC ingestão em tempo quase real usa Structured Streaming lendo arquivos de um volume. Continua
# MAGIC sendo streaming de verdade: fonte incremental, checkpoint, escrita exactly-once no Delta e
# MAGIC possibilidade de reprocessar a partir do offset.
# MAGIC
# MAGIC | Conceito | Com Kafka | Aqui |
# MAGIC |---|---|---|
# MAGIC | Fonte de eventos | tópico avaliacoes.raw | arquivos JSON num volume |
# MAGIC | Controle de posição | offset por partição | checkpoint do Structured Streaming |
# MAGIC | Consumo idempotente | commit manual e dedup por id_evento | checkpoint e escrita Delta |
# MAGIC | Eventos inválidos | tópico de DLQ | tabela tc2_bronze.avaliacao_dlq |
# MAGIC
# MAGIC Com um broker disponível, seria o mesmo código trocando format("json") por format("kafka").
# MAGIC A validação, a fila de erro e as agregações continuariam iguais.

# COMMAND ----------

# ============================================================
# PARÂMETROS DO JOB
# ============================================================
import random
from datetime import datetime, timedelta, timezone

import os
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType)

dbutils.widgets.text("catalogo", "workspace", "Catálogo")
dbutils.widgets.text("schema_landing", "tc2_landing", "Schema de landing")
dbutils.widgets.text("schema_bronze", "tc2_bronze", "Schema Bronze")
dbutils.widgets.text("schema_silver", "tc2_silver", "Schema Silver")
dbutils.widgets.text("volume", "arquivos", "Volume dos arquivos")
dbutils.widgets.text("schema_monitoramento", "tc2_monitoramento", "Schema de monitoramento")
dbutils.widgets.text("total_eventos", "1200", "Eventos a publicar")

CATALOGO = dbutils.widgets.get("catalogo")
SCHEMA_LANDING = dbutils.widgets.get("schema_landing")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
VOLUME = f"/Volumes/{CATALOGO}/{SCHEMA_LANDING}/{dbutils.widgets.get('volume')}"

DIR_EVENTOS = f"{VOLUME}/eventos"
DIR_CHECKPOINT = f"{VOLUME}/_checkpoints/avaliacoes"

SCHEMA_MONITORAMENTO = dbutils.widgets.get("schema_monitoramento")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_MONITORAMENTO}")
PONTO_CORTE_SAEB = 743


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

monitor = Monitor(spark, CATALOGO, SCHEMA_MONITORAMENTO, "02_streaming")

DIM_REDE = {0: "Total", 1: "Federal", 2: "Estadual", 3: "Municipal", 4: "Privada", 5: "Publica"}


def mapear_rede(coluna):
    """Traduz o codigo da rede para o nome, como a Silver do lote faz."""
    expressao = F.lit(None).cast("string")
    for codigo, nome in DIM_REDE.items():
        expressao = F.when(coluna == codigo, F.lit(nome)).otherwise(expressao)
    return expressao


TOTAL_EVENTOS = int(dbutils.widgets.get("total_eventos"))
PCT_DEFEITUOSOS = 0.05
MUNICIPIOS_SIMULADOS = 40
MUNICIPIOS_EM_QUEDA = 5
QUEDA_SIMULADA_PONTOS = 70

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

# Reexecutar o notebook do zero: limpa eventos e checkpoint anteriores
dbutils.fs.rm(DIR_EVENTOS, True)
dbutils.fs.rm(DIR_CHECKPOINT, True)
dbutils.fs.mkdirs(DIR_EVENTOS)

print(f"Eventos em     : {DIR_EVENTOS}")
print(f"Checkpoint em  : {DIR_CHECKPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Produtor de eventos
# MAGIC
# MAGIC Os eventos são registros reais de aluno lidos da Silver. O que está sendo simulado é só o
# MAGIC transporte, já que na prática eles chegariam das escolas aos poucos.
# MAGIC
# MAGIC Dois problemas entram de propósito: 5% de eventos com defeito, para a fila de erro ter o que
# MAGIC capturar, e uma queda de desempenho em 5 municípios, para as regras de alerta terem o que
# MAGIC detectar. Os eventos saem em 3 arquivos, para o streaming enxergar 3 lotes chegando em
# MAGIC momentos diferentes.

# COMMAND ----------

# ============================================================
# GERAÇÃO DOS EVENTOS
# ============================================================
random.seed(7)

# Os eventos sao registros reais de aluno lidos da Silver. So o transporte e
# simulado: em producao eles chegariam das escolas ao longo das semanas de
# aplicacao, e nao consolidados num arquivo anual.
fato_aluno = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_aluno")
ANO_RECENTE = fato_aluno.agg(F.max("ano")).collect()[0][0]

avaliados = fato_aluno.filter(
    (F.col("ano") == ANO_RECENTE) & (F.col("rede") == 3) & F.col("avaliado"))

municipios = [r["id_municipio"] for r in
              avaliados.select("id_municipio").distinct().limit(MUNICIPIOS_SIMULADOS).collect()]
em_queda = set(random.sample(municipios, MUNICIPIOS_EM_QUEDA))

registros = (avaliados.filter(F.col("id_municipio").isin(municipios))
             .select("id_aluno", "id_municipio", "id_escola", "rede", "serie",
                     "proficiencia_portugues", "peso_aluno")
             .limit(TOTAL_EVENTOS).collect())

print(f"Registros reais no fluxo: {len(registros)} | municipios: {len(municipios)} | "
      f"com queda injetada: {len(em_queda)}")


def gerar_evento(registro):
    """Monta um evento a partir de um registro de aluno da Silver."""
    defeituoso = random.random() < PCT_DEFEITUOSOS
    proficiencia = float(registro["proficiencia_portugues"])
    if registro["id_municipio"] in em_queda:
        proficiencia = round(proficiencia - QUEDA_SIMULADA_PONTOS, 2)

    evento = {
        "id_evento": f"EV{RUN_TS}{random.randint(0, 10**9):010d}",
        "tipo_evento": "AVALIACAO_ALUNO",
        "ts_evento": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "id_municipio": int(registro["id_municipio"]),
        "id_escola": str(registro["id_escola"]),
        "rede": int(registro["rede"]),
        "serie": int(registro["serie"]) if registro["serie"] is not None else 2,
        "id_aluno": str(registro["id_aluno"]),
        "proficiencia_portugues": proficiencia,
        "peso_aluno": float(registro["peso_aluno"] or 1.0),
        "origem": "sistema_aplicacao_prova",
    }
    if defeituoso:
        problema = random.choice(["campo_faltante", "fora_de_faixa",
                                  "municipio_inexistente", "timestamp_invalido"])
        if problema == "campo_faltante":
            evento["proficiencia_portugues"] = None
        elif problema == "fora_de_faixa":
            evento["proficiencia_portugues"] = 2500.0
        elif problema == "municipio_inexistente":
            evento["id_municipio"] = 9999999
        else:
            evento["ts_evento"] = "ontem de manha"
        evento["_defeito_injetado"] = problema
    else:
        evento["_defeito_injetado"] = None
    return evento


ESQUEMA_EVENTO = StructType([
    StructField("id_evento", StringType()),
    StructField("tipo_evento", StringType()),
    StructField("ts_evento", StringType()),
    StructField("id_municipio", IntegerType()),
    StructField("id_escola", StringType()),
    StructField("rede", IntegerType()),
    StructField("serie", IntegerType()),
    StructField("id_aluno", StringType()),
    StructField("proficiencia_portugues", DoubleType()),
    StructField("peso_aluno", DoubleType()),
    StructField("origem", StringType()),
    StructField("_defeito_injetado", StringType()),
])

por_lote = max(1, len(registros) // 3)
for lote in range(3):
    fatia = registros[lote * por_lote:(lote + 1) * por_lote]
    eventos = [gerar_evento(r) for r in fatia]
    (spark.createDataFrame(eventos, schema=ESQUEMA_EVENTO)
        .coalesce(1).write.mode("append").json(DIR_EVENTOS))
    print(f"Lote {lote + 1}: {len(eventos)} eventos publicados")

arquivos = [f.name for f in dbutils.fs.ls(DIR_EVENTOS) if f.name.endswith(".json")]
print(f"\n{len(arquivos)} arquivos de evento no volume")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento do stream
# MAGIC
# MAGIC O foreachBatch faz quatro coisas em cada micro-lote: valida contra o contrato, manda o que
# MAGIC falhou para a fila de erro com o motivo, enriquece o que passou com a dimensão territorial e
# MAGIC a regra dos 743 pontos, e grava as duas tabelas.
# MAGIC
# MAGIC O trigger availableNow processa tudo que está disponível e encerra. É o único gatilho que o
# MAGIC serverless aceita, e para um job agendado ele serve bem: não deixa compute ligado esperando
# MAGIC evento chegar.

# COMMAND ----------

# ============================================================
# VALIDAÇÃO, DLQ E ENRIQUECIMENTO
# ============================================================
dim_municipio = spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.dim_municipio").select(
    "id_municipio", "nome_municipio", "sigla_uf", "nome_regiao")

TABELA_STREAM = f"{CATALOGO}.{SCHEMA_BRONZE}.avaliacao_stream"
TABELA_DLQ = f"{CATALOGO}.{SCHEMA_BRONZE}.avaliacao_dlq"

for tabela in (TABELA_STREAM, TABELA_DLQ):
    spark.sql(f"DROP TABLE IF EXISTS {tabela}")


def motivo_rejeicao(df):
    """Devolve o primeiro motivo de rejeicao que se aplica ao evento."""
    return (F.when(F.col("id_evento").isNull() | F.col("ts_evento").isNull()
                   | F.col("id_municipio").isNull() | F.col("rede").isNull()
                   | F.col("proficiencia_portugues").isNull(),
                   F.lit("campos_obrigatorios_ausentes"))
             .when(~F.col("proficiencia_portugues").between(200, 1000),
                   F.lit("proficiencia_fora_de_faixa"))
             .when(F.col("ts_convertido").isNull(), F.lit("timestamp_invalido"))
             .when(F.col("nome_municipio").isNull(), F.lit("municipio_inexistente"))
             .otherwise(F.lit(None)))


def processar_lote(lote, epoca):
    # Em modo ANSI, que e o padrao do Databricks, to_timestamp levanta excecao
    # quando o valor esta malformado. O try_to_timestamp devolve NULL, que e o
    # que a regra timestamp_invalido procura para mandar o evento a fila de erro.
    marcado = (lote
               .withColumn("ts_convertido", F.expr("try_to_timestamp(ts_evento)"))
               .join(F.broadcast(dim_municipio), on="id_municipio", how="left"))
    marcado = marcado.withColumn("motivo_rejeicao", motivo_rejeicao(marcado))

    invalidos = marcado.filter(F.col("motivo_rejeicao").isNotNull())
    if invalidos.head(1):
        (invalidos
         .select("id_evento", "id_municipio", "proficiencia_portugues", "ts_evento",
                 "_defeito_injetado", "motivo_rejeicao",
                 F.lit(epoca).alias("_epoca"),
                 F.current_timestamp().alias("_ts_rejeicao"))
         .write.mode("append").option("mergeSchema", "true").saveAsTable(TABELA_DLQ))

    validos = (marcado.filter(F.col("motivo_rejeicao").isNull())
               .withColumn("alfabetizado",
                           F.col("proficiencia_portugues") >= F.lit(PONTO_CORTE_SAEB))
               .withColumn("peso_aluno", F.coalesce(F.col("peso_aluno"), F.lit(1.0)))
               .withColumn("rede_nome", mapear_rede(F.col("rede")))
               .withColumn("ts_processamento", F.current_timestamp())
               .withColumn("latencia_ms",
                           F.round((F.unix_timestamp("ts_processamento")
                                    - F.unix_timestamp("ts_convertido")) * 1000.0, 1))
               .withColumn("data_evento", F.to_date("ts_convertido"))
               .withColumn("_epoca", F.lit(epoca))
               .drop("motivo_rejeicao", "_defeito_injetado"))
    if validos.head(1):
        (validos.write.mode("append").option("mergeSchema", "true").saveAsTable(TABELA_STREAM))



consulta = (spark.readStream
            .schema(ESQUEMA_EVENTO)
            .option("maxFilesPerTrigger", 1)     # um arquivo por micro-lote, para ver os 3 lotes
            .json(DIR_EVENTOS)
            .writeStream
            .foreachBatch(processar_lote)
            .option("checkpointLocation", DIR_CHECKPOINT)
            .trigger(availableNow=True)
            .start())

with monitor.etapa("stream_avaliacoes", camada="bronze") as etapa:
    consulta.awaitTermination()
    etapa.entrada(TOTAL_EVENTOS)
    etapa.saida(spark.table(TABELA_STREAM).count())
    etapa.rejeitadas(spark.table(TABELA_DLQ).count())
print("Stream encerrado.")

# COMMAND ----------

# ============================================================
# RESULTADO DA INGESTÃO
# ============================================================
validos = spark.table(TABELA_STREAM).count()
rejeitados = spark.table(TABELA_DLQ).count() if spark.catalog.tableExists(TABELA_DLQ) else 0
total = validos + rejeitados

print(f"Eventos processados : {total}")
print(f"  validos           : {validos} ({validos / total * 100:.1f}%)")
print(f"  na DLQ            : {rejeitados} ({rejeitados / total * 100:.1f}%)")
print(f"  micro-lotes       : {spark.table(TABELA_STREAM).select('_epoca').distinct().count()}")

if rejeitados:
    print("\nMotivos de rejeicao — o detectado bate com o defeito injetado?")
    display(spark.table(TABELA_DLQ)
            .groupBy("_defeito_injetado", "motivo_rejeicao").count()
            .orderBy(F.desc("count")))
else:
    print("\nNenhum evento rejeitado nesta execucao.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Janelas e alertas
# MAGIC
# MAGIC Duas agregações diferentes. A janela temporal por tempo de evento dá throughput e latência.
# MAGIC O acumulado por município alimenta as regras de alerta.
# MAGIC
# MAGIC A regra de queda compara o que está chegando agora com o indicador consolidado da Silver.
# MAGIC É o ponto em que o lote e o streaming se encontram.

# COMMAND ----------

# ============================================================
# AGREGAÇÕES E REGRAS DE ALERTA
# ============================================================
MINIMO_AVALIACOES = 20
QUEDA_ALERTA_PP = 10.0

stream = spark.table(TABELA_STREAM)

janelas = (stream
           .groupBy(F.window(F.expr("try_to_timestamp(ts_evento)"), "10 seconds"), "sigla_uf")
           .agg(F.count("*").alias("eventos"),
                F.round(F.sum(F.col("alfabetizado").cast("double") * F.col("peso_aluno"))
                        / F.sum("peso_aluno") * 100, 2).alias("taxa_pct"),
                F.round(F.expr("percentile(latencia_ms, 0.95)"), 1).alias("latencia_p95_ms"))
           .select(F.col("window.start").alias("janela_inicio"), "sigla_uf",
                   "eventos", "taxa_pct", "latencia_p95_ms")
           .orderBy("janela_inicio", F.desc("eventos")))

print("Janelas de 10 segundos por UF:")
display(janelas.limit(15))

baseline = (spark.table(f"{CATALOGO}.{SCHEMA_SILVER}.fato_alfabetizacao_municipio")
            .filter((F.col("ano") == ANO_RECENTE) & (F.col("rede") == 3))
            .select("id_municipio", F.col("taxa_alfabetizacao").alias("taxa_oficial")))

acumulado = (stream.groupBy("id_municipio", "nome_municipio", "sigla_uf")
             .agg(F.count("*").alias("avaliados"),
                  F.round(F.sum(F.col("alfabetizado").cast("double") * F.col("peso_aluno"))
                          / F.sum("peso_aluno") * 100, 2).alias("taxa_stream"))
             .filter(F.col("avaliados") >= MINIMO_AVALIACOES)
             .join(baseline, on="id_municipio", how="left"))

alertas = (acumulado
           .withColumn("delta_pp", F.round(F.col("taxa_stream") - F.col("taxa_oficial"), 2))
           .withColumn("tipo",
                       F.when(F.col("delta_pp") < -QUEDA_ALERTA_PP, F.lit("QUEDA_DE_DESEMPENHO"))
                        .when(F.col("taxa_stream") < 50, F.lit("ABAIXO_DA_META"))
                        .otherwise(F.lit(None)))
           .filter(F.col("tipo").isNotNull())
           .withColumn("severidade",
                       F.when(F.col("tipo") == "QUEDA_DE_DESEMPENHO", F.lit("ALTA"))
                        .otherwise(F.lit("MEDIA")))
           .withColumn("_ts_alerta", F.current_timestamp())
           .orderBy("delta_pp"))

(alertas.write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOGO}.{SCHEMA_SILVER}.alerta_stream"))

print(f"\n{alertas.count()} alertas gerados -> {CATALOGO}.{SCHEMA_SILVER}.alerta_stream")
display(alertas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert na Silver
# MAGIC
# MAGIC O MERGE INTO do Delta deixa o reprocessamento seguro. Se o mesmo evento chegar duas vezes,
# MAGIC seja por replay do checkpoint ou por reenvio da origem, a linha é atualizada em vez de
# MAGIC duplicada.
# MAGIC
# MAGIC A célula roda o merge duas vezes com o mesmo lote de propósito, para mostrar que a segunda
# MAGIC não altera nada.

# COMMAND ----------

# ============================================================
# MERGE IDEMPOTENTE
# ============================================================
TABELA_SILVER_STREAM = f"{CATALOGO}.{SCHEMA_SILVER}.fato_avaliacao_stream"

# Recriada a cada execucao para o teste de idempotencia partir sempre do mesmo estado.
spark.sql(f"DROP TABLE IF EXISTS {TABELA_SILVER_STREAM}")
spark.sql(f"""
    CREATE TABLE {TABELA_SILVER_STREAM} (
        id_evento              STRING,
        id_aluno               STRING,
        id_municipio           INT,
        nome_municipio         STRING,
        sigla_uf               STRING,
        nome_regiao            STRING,
        id_escola              STRING,
        rede                   INT,
        rede_nome              STRING,
        serie                  INT,
        proficiencia_portugues DOUBLE,
        peso_aluno             DOUBLE,
        alfabetizado           BOOLEAN,
        latencia_ms            DOUBLE,
        data_evento            DATE,
        ts_evento              STRING
    ) USING DELTA
""")

spark.table(TABELA_STREAM).select(
    "id_evento", "id_aluno", "id_municipio", "nome_municipio", "sigla_uf", "nome_regiao",
    "id_escola", "rede", "rede_nome", "serie", "proficiencia_portugues", "peso_aluno",
    "alfabetizado",
    "latencia_ms", "data_evento", "ts_evento",
).createOrReplaceTempView("novos_eventos")

antes = spark.table(TABELA_SILVER_STREAM).count()

spark.sql(f"""
    MERGE INTO {TABELA_SILVER_STREAM} AS destino
    USING novos_eventos AS origem
      ON destino.id_evento = origem.id_evento
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

depois = spark.table(TABELA_SILVER_STREAM).count()
print(f"Linhas antes do merge : {antes}")
print(f"Linhas depois do merge: {depois}  (inseridas: {depois - antes})")

# Rodar o mesmo MERGE de novo nao pode alterar nenhuma linha
spark.sql(f"""
    MERGE INTO {TABELA_SILVER_STREAM} AS destino
    USING novos_eventos AS origem
      ON destino.id_evento = origem.id_evento
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
print(f"Apos reaplicar o mesmo lote: {spark.table(TABELA_SILVER_STREAM).count()} linhas "
      f"(esperado: {depois}, inalterado)")

# COMMAND ----------

# ============================================================
# OBSERVABILIDADE
# ============================================================
metricas = spark.table(TABELA_STREAM).agg(
    F.count("*").alias("eventos_validos"),
    F.round(F.avg("latencia_ms"), 1).alias("latencia_media_ms"),
    F.round(F.expr("percentile(latencia_ms, 0.95)"), 1).alias("latencia_p95_ms"),
    F.min("ts_evento").alias("primeiro_evento"),
    F.max("ts_evento").alias("ultimo_evento"),
)
display(metricas)

taxa_dlq = rejeitados / total * 100
if taxa_dlq > 10:
    monitor.alerta("ALTA", f"taxa de DLQ em {taxa_dlq:.2f}%, acima do limite de 10%")
print(f"Taxa de DLQ: {taxa_dlq:.2f}%  (limite operacional: 10%)")
print(f"Alertas ativos: {alertas.count()}")
print("\nHistorico da tabela de streaming (auditoria via Delta time travel):")
display(spark.sql(f"DESCRIBE HISTORY {TABELA_SILVER_STREAM}").select(
    "version", "timestamp", "operation", "operationMetrics").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoramento da execução
# MAGIC
# MAGIC Como no notebook 01, as etapas cronometradas ficam gravadas em
# MAGIC tc2_monitoramento.execucao_etapa, junto com volume processado, rejeições e alertas.

# COMMAND ----------

# ============================================================
# PAINEL DE EXECUÇÃO
# ============================================================
monitor.gravar()
monitor.painel()
