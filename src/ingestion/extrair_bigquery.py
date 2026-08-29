"""Extração das tabelas que não cabem no download gratuito da Base dos Dados.

Duas tabelas do dataset br_inep_avaliacao_alfabetizacao ficam de fora do portal:

    alunos       3.867.999 linhas | 256 MB  -> microdados no grão de aluno
    dicionario   pequena            -> o significado oficial dos códigos (rede, serie...)

As duas estão no BigQuery público e cabem folgadamente na cota gratuita do
BigQuery Sandbox (1 TB de consulta por mês, sem cartão de crédito).

Uso:

    python -m src.ingestion.extrair_bigquery --projeto meu-projeto-sandbox
    python -m src.ingestion.extrair_bigquery --projeto meu-projeto --amostra 60
    python -m src.ingestion.extrair_bigquery --projeto meu-projeto --so-dicionario

Antes de rodar, autentique uma vez:

    gcloud auth application-default login

Três coisas reduzem o custo da consulta:

1. Projeção de colunas, em vez de SELECT *. O BigQuery é colunar e cobra pelas
   colunas lidas, então pedir 10 das 12 já corta parte do custo.
2. Filtro no ano, que reduz as partições varridas.
3. Dry run antes de executar. A estimativa não custa nada e o script mostra quanto
   a consulta vai processar antes de rodar de verdade.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

TABELA_ALUNOS = "basedosdados.br_inep_avaliacao_alfabetizacao.alunos"
TABELA_DICIONARIO = "basedosdados.br_inep_avaliacao_alfabetizacao.dicionario"

COTA_MENSAL_GRATUITA = 1024 ** 4          # 1 TiB
PONTO_CORTE_SAEB = 743

REDE_TEXTO_PARA_CODIGO = {
    "federal": 1, "estadual": 2, "municipal": 3, "privada": 4,
    "publica": 5, "pública": 5, "total": 0,
}


# ---------------------------------------------------------------------------
# Consultas
# ---------------------------------------------------------------------------
def sql_dicionario():
    return f"""
        SELECT id_tabela, nome_coluna, chave, valor, cobertura_temporal
        FROM `{TABELA_DICIONARIO}`
        ORDER BY id_tabela, nome_coluna, chave
    """


def sql_alunos(anos, amostra_por_municipio=None):
    """Monta a consulta dos microdados de aluno.

    O parametro amostra_por_municipio limita a quantidade de alunos por ano,
    municipio e rede, usando QUALIFY. Serve para gerar um arquivo menor em teste.
    """
    filtro_ano = ", ".join(str(a) for a in anos)
    amostragem = ""
    if amostra_por_municipio:
        amostragem = f"""
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ano, id_municipio, rede ORDER BY RAND()
        ) <= {int(amostra_por_municipio)}"""

    return f"""
        SELECT
            ano,
            id_municipio,
            id_escola,
            id_aluno,
            rede,
            serie,
            proficiencia,
            alfabetizado,
            presenca,
            peso_aluno
        FROM `{TABELA_ALUNOS}`
        WHERE ano IN ({filtro_ano}){amostragem}
    """


# ---------------------------------------------------------------------------
# Execução
# ---------------------------------------------------------------------------
def formatar_bytes(n):
    n = float(n)
    for unidade in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unidade == "TB":
            return f"{n:.1f} {unidade}"
        n /= 1024


def criar_cliente(projeto):
    try:
        from google.cloud import bigquery
    except ImportError:
        sys.exit(
            "Biblioteca ausente. Instale com:\n"
            "    pip install google-cloud-bigquery db-dtypes pyarrow"
        )
    return bigquery.Client(project=projeto)


def estimar(cliente, sql):
    """Roda a consulta em dry run e devolve quantos bytes ela processaria."""
    from google.cloud import bigquery

    config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = cliente.query(sql, job_config=config)
    return job.total_bytes_processed


def executar(cliente, sql, rotulo):
    bytes_estimados = estimar(cliente, sql)
    pct = bytes_estimados / COTA_MENSAL_GRATUITA * 100
    print(f"\n[{rotulo}] estimativa: {formatar_bytes(bytes_estimados)} "
          f"({pct:.4f}% da cota mensal gratuita de 1 TiB)")

    job = cliente.query(sql)
    df = job.result().to_dataframe()
    print(f"[{rotulo}] processado: {formatar_bytes(job.total_bytes_processed)} | "
          f"faturado: {formatar_bytes(job.total_bytes_billed)} | "
          f"{len(df):,} linhas".replace(",", "."))
    return df


# ---------------------------------------------------------------------------
# Normalização
# ---------------------------------------------------------------------------
def normalizar_alunos(df):
    """Ajusta os microdados para o formato que a pipeline espera.

    No BigQuery, id_municipio e rede vêm como texto e a proficiência está numa
    coluna chamada proficiencia. A conversão fica aqui para os notebooks não
    precisarem tratar isso.
    """
    import numpy as np
    import pandas as pd

    dados = df.copy()
    dados["id_municipio"] = pd.to_numeric(dados["id_municipio"], errors="coerce").astype("Int32")
    dados["ano"] = pd.to_numeric(dados["ano"], errors="coerce").astype("Int16")
    dados["serie"] = pd.to_numeric(dados["serie"], errors="coerce").astype("Int8")
    dados["proficiencia_portugues"] = pd.to_numeric(dados["proficiencia"], errors="coerce")

    print("\nValores encontrados nas colunas categóricas (conferência):")
    for coluna in ("rede", "alfabetizado", "presenca"):
        if coluna in dados.columns:
            print(f"  {coluna:14s} {dict(dados[coluna].value_counts(dropna=False).head(6))}")

    dados["rede_texto"] = dados["rede"]
    dados["rede"] = (dados["rede"].astype(str).str.strip().str.lower()
                     .map(REDE_TEXTO_PARA_CODIGO).astype("Int8"))

    # A coluna alfabetizado vem como texto. Vira booleano aqui e, se o valor nao
    # for reconhecido, entra a regra oficial dos 743 pontos.
    mapa = {"1": True, "0": False, "sim": True, "nao": False, "não": False,
            "true": True, "false": False}
    normalizado = dados["alfabetizado"].astype(str).str.strip().str.lower().map(mapa)
    dados["alfabetizado"] = normalizado.fillna(
        dados["proficiencia_portugues"] >= PONTO_CORTE_SAEB).astype(bool)

    colunas = ["id_aluno", "ano", "id_municipio", "id_escola", "rede", "rede_texto", "serie",
               "proficiencia_portugues", "alfabetizado", "presenca", "peso_aluno"]
    return dados[[c for c in colunas if c in dados.columns]]


def localizar_landing():
    for candidato in [Path.cwd(), *Path.cwd().parents]:
        if (candidato / "data" / "landing").is_dir():
            return candidato / "data" / "landing"
    sys.exit("Rode o script de dentro do repositório (a pasta data/landing precisa existir).")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--projeto", required=True,
                        help="ID do projeto GCP (o do BigQuery Sandbox serve)")
    parser.add_argument("--anos", nargs="+", type=int, default=[2023, 2024],
                        help="Anos a extrair (padrão: 2023 2024)")
    parser.add_argument("--amostra", type=int, default=None,
                        help="Máximo de alunos por município/rede/ano. "
                             "Sem isso, extrai a tabela completa (~3,9 milhões de linhas)")
    parser.add_argument("--so-dicionario", action="store_true",
                        help="Extrai apenas o dicionário de códigos")
    args = parser.parse_args()

    landing = localizar_landing()
    cliente = criar_cliente(args.projeto)
    print(f"Projeto GCP : {args.projeto}")
    print(f"Destino     : {landing}")

    # ── Dicionário oficial dos códigos ──
    dicionario = executar(cliente, sql_dicionario(), "dicionario")
    destino = landing / "dicionario.csv"
    dicionario.to_csv(destino, index=False, encoding="utf-8")
    print(f"[dicionario] salvo em {destino}")

    rede = dicionario[dicionario["nome_coluna"] == "rede"]
    if not rede.empty:
        print("\nMapeamento OFICIAL da coluna 'rede':")
        for _, linha in rede.drop_duplicates(subset=["chave", "valor"]).iterrows():
            print(f"   {linha['chave']:>3}  ->  {linha['valor']}   (tabela: {linha['id_tabela']})")

    if args.so_dicionario:
        return

    # ── Microdados de aluno ──
    sql = sql_alunos(args.anos, args.amostra)
    alunos = executar(cliente, sql, "alunos")
    alunos = normalizar_alunos(alunos)

    destino = landing / "alunos.parquet"
    alunos.to_parquet(destino, engine="pyarrow", compression="snappy", index=False)
    print(f"\n[alunos] {len(alunos):,} linhas salvas em {destino} "
          f"({destino.stat().st_size / 1024 / 1024:.1f} MB)".replace(",", "."))

    print("\nTaxa de alfabetização reconstruída dos microdados reais: "
          f"{alunos['alfabetizado'].mean() * 100:.2f}%")
    print("\nPróximo passo: reexecute o Notebook 01. Ele detecta o arquivo e passa a usar "
          "os microdados reais no lugar da simulação.")


if __name__ == "__main__":
    main()
