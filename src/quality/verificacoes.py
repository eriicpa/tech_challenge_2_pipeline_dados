# -*- coding: utf-8 -*-
"""Verificações de qualidade sobre DataFrames do Spark.

Cobrem seis dimensões: completude, unicidade, validade, chaves de relacionamento,
consistência entre tabelas e atualidade.

As verificações devolvem o próprio objeto, então dá para encadear várias. Elas não
interrompem na hora em que falham; o relatório é montado inteiro e a execução só
para no aprovar_ou_falhar. Assim uma rodada mostra todos os problemas de uma vez.
"""
from pyspark.sql import functions as F

ERRO = "ERROR"
AVISO = "WARNING"


class FalhaDeQualidade(Exception):
    """Erro levantado quando uma verificação de severidade ERROR reprova."""


class Verificador:
    """Roda verificações sobre um DataFrame e guarda o resultado de cada uma."""

    def __init__(self, tabela, df, camada="-"):
        self.tabela = tabela
        self.df = df
        self.camada = camada
        self.resultados = []
        self._total = None

    # ── infraestrutura ────────────────────────────────────────────────
    @property
    def total(self):
        """Total de linhas. Fica em cache porque quase toda verificação usa."""
        if self._total is None:
            self._total = self.df.count()
        return self._total

    def _registrar(self, verificacao, coluna, passou, detalhe, severidade):
        self.resultados.append({
            "tabela": self.tabela,
            "camada": self.camada,
            "verificacao": verificacao,
            "coluna": coluna or "-",
            "status": "PASS" if passou else ("FAIL" if severidade == ERRO else "WARN"),
            "severidade": severidade,
            "detalhe": detalhe,
        })
        return self

    # ── completude ────────────────────────────────────────────────────
    def linhas_minimas(self, minimo, severidade=ERRO):
        """Reprova se a tabela veio com menos linhas que o esperado."""
        return self._registrar("contagem_linhas", None, self.total >= minimo,
                               f"{self.total} linhas (minimo {minimo})", severidade)

    def sem_nulos(self, *colunas, severidade=ERRO):
        """Reprova se alguma das colunas tiver valor nulo."""
        for coluna in colunas:
            nulos = self.df.filter(F.col(coluna).isNull()).count()
            self._registrar("sem_nulos", coluna, nulos == 0,
                            f"{nulos} nulos", severidade)
        return self

    # ── unicidade ─────────────────────────────────────────────────────
    def valor_unico(self, coluna, severidade=ERRO):
        """Reprova se a coluna, que deveria ser identificador, tiver repetição."""
        distintos = self.df.select(coluna).distinct().count()
        duplicatas = self.total - distintos
        return self._registrar("valor_unico", coluna, duplicatas == 0,
                               f"{duplicatas} duplicatas", severidade)

    def grao_unico(self, *colunas, severidade=ERRO):
        """Reprova se a combinação de colunas que define o grão se repetir."""
        distintos = self.df.select(*colunas).distinct().count()
        duplicatas = self.total - distintos
        return self._registrar("grao_unico", ", ".join(colunas), duplicatas == 0,
                               f"{duplicatas} duplicatas no grao", severidade)

    # ── validade ──────────────────────────────────────────────────────
    def dentro_da_faixa(self, coluna, minimo, maximo, severidade=ERRO):
        """Reprova valores numéricos fora do intervalo informado."""
        fora = self.df.filter(F.col(coluna).isNotNull()
                              & ~F.col(coluna).between(minimo, maximo)).count()
        return self._registrar("faixa", coluna, fora == 0,
                               f"{fora} fora de [{minimo}, {maximo}]", severidade)

    def dentro_do_dominio(self, coluna, valores, severidade=ERRO):
        """Reprova valores categóricos que não estejam na lista informada."""
        fora = self.df.filter(F.col(coluna).isNotNull()
                              & ~F.col(coluna).isin(list(valores))).count()
        return self._registrar("dominio", coluna, fora == 0,
                               f"{fora} fora de {sorted(valores)}", severidade)

    # ── chaves de relacionamento ──────────────────────────────────────
    def chave_existe_em(self, coluna, df_referencia, coluna_referencia=None,
                        severidade=ERRO):
        """Integridade referencial: procura chaves que não existem na tabela de referência.

        Serve para pegar o caso em que um join descarta linhas silenciosamente e o
        total final sai menor.
        """
        coluna_referencia = coluna_referencia or coluna
        # A mesma chave chega como inteiro no CSV do Inep e como string no JSON da
        # API do IBGE. Join entre tipos diferentes nao casa, entao os dois lados sao
        # convertidos para texto antes da comparacao.
        referencia = df_referencia.select(
            F.col(coluna_referencia).cast("string").alias("_chave")).distinct()
        orfas = (self.df.filter(F.col(coluna).isNotNull())
                 .select(F.col(coluna).cast("string").alias("_valor"))
                 .join(referencia, F.col("_valor") == F.col("_chave"), "left_anti")
                 .count())
        return self._registrar("integridade_referencial", coluna, orfas == 0,
                               f"{orfas} chaves sem correspondencia", severidade)

    # ── consistência entre tabelas ────────────────────────────────────
    def concorda_com(self, outro_df, chaves, coluna, coluna_outro=None,
                     tolerancia=0.01, limite_divergencia=0.01, severidade=ERRO):
        """Compara a mesma medida em duas tabelas.

        A tolerancia é a diferença aceita em cada linha. O limite_divergencia é a
        fração de linhas que pode divergir antes de a verificação reprovar.
        """
        coluna_outro = coluna_outro or coluna
        esquerda = self.df.select(*chaves, F.col(coluna).alias("_a"))
        direita = outro_df.select(*chaves, F.col(coluna_outro).alias("_b"))
        par = esquerda.join(direita, on=list(chaves), how="inner")
        comparadas = par.count()
        if comparadas == 0:
            return self._registrar("consistencia", coluna, False,
                                   "nenhuma linha em comum para comparar", severidade)
        divergentes = par.filter(F.abs(F.col("_a") - F.col("_b")) > tolerancia).count()
        fracao = divergentes / comparadas
        return self._registrar(
            "consistencia", coluna, fracao <= limite_divergencia,
            f"{divergentes} de {comparadas} divergem ({fracao * 100:.3f}%)", severidade)

    # ── atualidade ────────────────────────────────────────────────────
    def contem_ciclo(self, coluna, ciclo, severidade=AVISO):
        """Confere se o ciclo esperado aparece na tabela."""
        presente = self.df.filter(F.col(coluna) == ciclo).limit(1).count() > 0
        return self._registrar("atualidade", coluna, presente,
                               f"ciclo {ciclo} {'presente' if presente else 'ausente'}",
                               severidade)

    # ── resultado ─────────────────────────────────────────────────────
    def falhas(self):
        """Lista as verificações de severidade ERROR que reprovaram."""
        return [r for r in self.resultados if r["status"] == "FAIL"]

    def avisos(self):
        return [r for r in self.resultados if r["status"] == "WARN"]

    def aprovar_ou_falhar(self):
        """Levanta exceção se houver alguma reprovação de severidade ERROR."""
        problemas = self.falhas()
        if problemas:
            descricao = "; ".join(
                f"{p['tabela']}.{p['verificacao']}:{p['coluna']} -> {p['detalhe']}"
                for p in problemas)
            raise FalhaDeQualidade(
                f"[QUALITY GATE] {len(problemas)} verificacao(oes) reprovada(s): {descricao}")
        return self


def consolidar(verificadores):
    """Junta os resultados de varios verificadores numa lista."""
    return [r for v in verificadores for r in v.resultados]


def resumo(resultados):
    """Conta quantas verificações passaram, avisaram e reprovaram."""
    contagem = {"PASS": 0, "WARN": 0, "FAIL": 0}
    for r in resultados:
        contagem[r["status"]] = contagem.get(r["status"], 0) + 1
    return contagem
