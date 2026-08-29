# -*- coding: utf-8 -*-
"""Instrumentação das etapas da pipeline, gravada em tabelas Delta.

Cada etapa cronometrada vira uma linha com duração, volume de entrada e saída,
rejeições e status. Como fica em tabela, dá para comparar duas execuções por SQL
e olhar uma rodada antiga sem reprocessar.

Etapa que levanta exceção é gravada com status FALHA e gera um alerta de
severidade alta. A exceção continua subindo depois disso.
"""
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import types as T

ESQUEMA_EXECUCAO = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("pipeline", T.StringType()),
    T.StructField("etapa", T.StringType()),
    T.StructField("camada", T.StringType()),
    T.StructField("inicio", T.TimestampType()),
    T.StructField("duracao_s", T.DoubleType()),
    T.StructField("linhas_entrada", T.LongType()),
    T.StructField("linhas_saida", T.LongType()),
    T.StructField("linhas_rejeitadas", T.LongType()),
    T.StructField("status", T.StringType()),
    T.StructField("detalhe", T.StringType()),
])

ESQUEMA_ALERTA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("pipeline", T.StringType()),
    T.StructField("momento", T.TimestampType()),
    T.StructField("severidade", T.StringType()),
    T.StructField("mensagem", T.StringType()),
])


class _Etapa:
    """Guarda os números de uma etapa enquanto ela roda."""

    def __init__(self, nome, camada):
        self.nome = nome
        self.camada = camada
        self.linhas_entrada = None
        self.linhas_saida = None
        self.linhas_rejeitadas = None
        self.detalhe = None

    def entrada(self, n):
        self.linhas_entrada = int(n)
        return self

    def saida(self, n):
        self.linhas_saida = int(n)
        return self

    def rejeitadas(self, n):
        self.linhas_rejeitadas = int(n)
        return self

    def nota(self, texto):
        self.detalhe = texto
        return self


class Monitor:
    """Coleta as métricas das etapas de um notebook e grava em Delta."""

    def __init__(self, spark, catalogo, schema, pipeline, run_id=None):
        self.spark = spark
        self.catalogo = catalogo
        self.schema = schema
        self.pipeline = pipeline
        self.run_id = run_id or f"{datetime.now(timezone.utc):%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:6]}"
        self.execucoes = []
        self.alertas = []

    # ── coleta ────────────────────────────────────────────────────────
    @contextmanager
    def etapa(self, nome, camada="-"):
        """Cronometra um bloco e registra o resultado, inclusive quando ele falha."""
        coletor = _Etapa(nome, camada)
        inicio = datetime.now(timezone.utc)
        relogio = time.perf_counter()
        status = "OK"
        try:
            yield coletor
        except Exception as erro:
            status = "FALHA"
            coletor.detalhe = f"{type(erro).__name__}: {erro}"[:500]
            self.alerta("ALTA", f"etapa {nome} falhou: {coletor.detalhe}")
            raise
        finally:
            self.execucoes.append((
                self.run_id, self.pipeline, nome, camada, inicio,
                round(time.perf_counter() - relogio, 3),
                coletor.linhas_entrada, coletor.linhas_saida,
                coletor.linhas_rejeitadas, status, coletor.detalhe,
            ))

    def alerta(self, severidade, mensagem):
        """Guarda um alerta. Não interrompe a execução."""
        self.alertas.append((self.run_id, self.pipeline,
                             datetime.now(timezone.utc), severidade, mensagem[:500]))
        return self

    # ── persistência ──────────────────────────────────────────────────
    def gravar(self):
        """Grava as etapas e os alertas desta execução."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalogo}.{self.schema}")
        if self.execucoes:
            (self.spark.createDataFrame(self.execucoes, ESQUEMA_EXECUCAO)
             .write.mode("append").option("mergeSchema", "true")
             .saveAsTable(f"{self.catalogo}.{self.schema}.execucao_etapa"))
        if self.alertas:
            (self.spark.createDataFrame(self.alertas, ESQUEMA_ALERTA)
             .write.mode("append").option("mergeSchema", "true")
             .saveAsTable(f"{self.catalogo}.{self.schema}.execucao_alerta"))
        return self

    # ── leitura ───────────────────────────────────────────────────────
    def painel(self):
        """Imprime o resumo do que foi medido nesta execução."""
        if not self.execucoes:
            print("Nenhuma etapa instrumentada.")
            return

        duracao = sum(e[5] for e in self.execucoes)
        saida = sum(e[7] or 0 for e in self.execucoes)
        falhas = [e for e in self.execucoes if e[9] == "FALHA"]

        # So entram as etapas que declararam entrada e rejeitadas. Somar todas
        # misturaria arquivos lidos com linhas processadas.
        com_rejeicao = [e for e in self.execucoes if e[8] is not None and e[6]]
        entrada_r = sum(e[6] for e in com_rejeicao)
        rejeitadas = sum(e[8] for e in com_rejeicao)

        print(f"EXECUCAO {self.run_id}  |  {self.pipeline}")
        print(f"  etapas             : {len(self.execucoes)}"
              f" ({len(falhas)} com falha)")
        print(f"  duracao total      : {duracao:.1f}s")
        print(f"  linhas gravadas    : {saida:,}".replace(",", "."))
        if entrada_r:
            print(f"  rejeitadas         : {rejeitadas:,} de {entrada_r:,}"
                  f" ({rejeitadas / entrada_r * 100:.2f}%)".replace(",", "."))
        print(f"  alertas            : {len(self.alertas)}")
        print()
        print(f"  {'etapa':32s} {'camada':9s} {'seg':>7s} {'entrada':>12s} {'saida':>12s}  status")
        for e in self.execucoes:
            print(f"  {e[2][:32]:32s} {e[3]:9s} {e[5]:7.2f}"
                  f" {('' if e[6] is None else f'{e[6]:,}'.replace(',', '.')):>12s}"
                  f" {('' if e[7] is None else f'{e[7]:,}'.replace(',', '.')):>12s}  {e[9]}")
        for a in self.alertas:
            print(f"  [{a[3]}] {a[4]}")

    def historico(self, limite=20):
        """Devolve as últimas execuções gravadas, para comparar rodadas."""
        return self.spark.sql(f"""
            SELECT run_id, pipeline,
                   MIN(inicio)                                   AS inicio,
                   ROUND(SUM(duracao_s), 1)                      AS duracao_s,
                   SUM(linhas_saida)                             AS linhas_saida,
                   SUM(linhas_rejeitadas)                        AS rejeitadas,
                   SUM(CASE WHEN status = 'FALHA' THEN 1 ELSE 0 END) AS etapas_com_falha
            FROM   {self.catalogo}.{self.schema}.execucao_etapa
            GROUP  BY run_id, pipeline
            ORDER  BY inicio DESC
            LIMIT  {limite}
        """)
