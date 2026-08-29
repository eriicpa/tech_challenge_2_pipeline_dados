"""Consultas que constroem a camada Gold.

Estão em SQL num arquivo separado para ficarem num lugar só e para o mesmo texto
poder ser colado no editor SQL do Databricks sem adaptação.

Cada entrada tem a consulta, as colunas de partição e a pergunta que ela responde.
"""

CONSULTAS_GOLD = {

    # ------------------------------------------------------------------
    "gold_indicador_municipio": {
        "descricao": "Indicador por município, ano e rede, com meta, risco e posição relativa.",
        "pergunta": "Como está cada município e quão longe ele está da própria meta?",
        "particoes": ["ano"],
        "sql": """
            SELECT
                ano,
                id_municipio,
                nome_municipio,
                sigla_uf,
                nome_uf,
                nome_regiao,
                nome_mesorregiao,
                rede,
                rede_nome,
                serie,
                taxa_alfabetizacao,
                media_portugues,
                percentual_participacao,
                meta_taxa_ano,
                meta_taxa_2030,
                gap_meta_ano,
                gap_meta_2030,
                taxa_uf,
                taxa_brasil_publica,
                dif_vs_uf,
                dif_vs_brasil,
                CASE
                    WHEN taxa_alfabetizacao < 40 THEN 'Critico'
                    WHEN taxa_alfabetizacao < 55 THEN 'Alto'
                    WHEN taxa_alfabetizacao < 70 THEN 'Medio'
                    ELSE 'Baixo'
                END AS risco_alfabetizacao,
                CASE
                    WHEN meta_taxa_ano IS NULL THEN 'Sem meta definida'
                    WHEN atingiu_meta_ano THEN 'Meta atingida'
                    ELSE 'Abaixo da meta'
                END AS situacao_meta,
                ROUND(gap_meta_2030 / NULLIF(2030 - ano, 0), 2) AS ritmo_anual_necessario_pp,
                RANK() OVER (
                    PARTITION BY ano, rede, sigla_uf ORDER BY taxa_alfabetizacao DESC
                ) AS posicao_na_uf,
                NTILE(4) OVER (
                    PARTITION BY ano, rede ORDER BY taxa_alfabetizacao
                ) AS quartil_nacional
            FROM silver_alfabetizacao_municipio
        """,
    },

    # ------------------------------------------------------------------
    "gold_meta_vs_realizado": {
        "descricao": "Consolidado meta × realizado por ano, rede e UF.",
        "pergunta": "Quais estados estão na rota da meta e quais concentram municípios críticos?",
        "particoes": ["ano"],
        "sql": """
            SELECT
                ano,
                rede,
                rede_nome,
                sigla_uf,
                nome_regiao,
                COUNT(DISTINCT id_municipio) AS municipios,
                ROUND(AVG(taxa_alfabetizacao), 2) AS taxa_media,
                ROUND(AVG(meta_taxa_ano), 2) AS meta_media,
                ROUND(AVG(gap_meta_ano), 2) AS gap_medio_pp,
                ROUND(AVG(gap_meta_2030), 2) AS gap_medio_2030_pp,
                ROUND(
                    100.0 * SUM(CASE WHEN atingiu_meta_ano THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN meta_taxa_ano IS NOT NULL THEN 1 ELSE 0 END), 0), 1
                ) AS pct_municipios_na_meta,
                SUM(CASE WHEN taxa_alfabetizacao < 40 THEN 1 ELSE 0 END) AS municipios_criticos,
                ROUND(AVG(percentual_participacao), 1) AS participacao_media
            FROM silver_alfabetizacao_municipio
            GROUP BY ano, rede, rede_nome, sigla_uf, nome_regiao
        """,
    },

    # ------------------------------------------------------------------
    "gold_evolucao_temporal": {
        "descricao": "Variação do indicador entre o primeiro e o último ciclo disponível.",
        "pergunta": "Quem melhorou, quem piorou e em quantos anos cada município chega à meta?",
        "particoes": None,
        "sql": """
            WITH marcos AS (
                SELECT id_municipio, rede,
                       MIN(ano) AS ano_inicial,
                       MAX(ano) AS ano_final
                FROM silver_alfabetizacao_municipio
                GROUP BY id_municipio, rede
            ),
            consolidado AS (
                SELECT
                    s.id_municipio,
                    s.rede,
                    s.rede_nome,
                    m.ano_inicial,
                    m.ano_final,
                    MAX(CASE WHEN s.ano = m.ano_inicial THEN s.taxa_alfabetizacao END) AS taxa_inicial,
                    MAX(CASE WHEN s.ano = m.ano_final   THEN s.taxa_alfabetizacao END) AS taxa_final,
                    MAX(CASE WHEN s.ano = m.ano_final   THEN s.meta_taxa_2030 END)     AS meta_2030,
                    MAX(CASE WHEN s.ano = m.ano_final   THEN s.nome_municipio END)     AS nome_municipio,
                    MAX(CASE WHEN s.ano = m.ano_final   THEN s.sigla_uf END)           AS sigla_uf,
                    MAX(CASE WHEN s.ano = m.ano_final   THEN s.nome_regiao END)        AS nome_regiao
                FROM silver_alfabetizacao_municipio s
                JOIN marcos m
                  ON s.id_municipio = m.id_municipio AND s.rede = m.rede
                GROUP BY s.id_municipio, s.rede, s.rede_nome, m.ano_inicial, m.ano_final
            )
            SELECT
                id_municipio,
                nome_municipio,
                sigla_uf,
                nome_regiao,
                rede,
                rede_nome,
                ano_inicial,
                ano_final,
                taxa_inicial,
                taxa_final,
                meta_2030,
                ROUND(taxa_final - taxa_inicial, 2) AS variacao_pp,
                ROUND((taxa_final - taxa_inicial) / NULLIF(ano_final - ano_inicial, 0), 2)
                    AS variacao_media_anual_pp,
                CASE
                    WHEN ano_inicial = ano_final THEN 'Serie unica'
                    WHEN taxa_final - taxa_inicial > 1 THEN 'Melhora'
                    WHEN taxa_final - taxa_inicial < -1 THEN 'Piora'
                    ELSE 'Estavel'
                END AS tendencia,
                CASE
                    WHEN taxa_final >= meta_2030 THEN 0
                    WHEN taxa_final > taxa_inicial AND ano_final > ano_inicial
                        THEN ROUND(
                            (meta_2030 - taxa_final)
                            / ((taxa_final - taxa_inicial) / (ano_final - ano_inicial)), 1)
                    ELSE NULL
                END AS anos_estimados_para_meta
            FROM consolidado
        """,
    },

    # ------------------------------------------------------------------
    "gold_ranking_uf": {
        "descricao": "Indicador estadual com posição nacional e distância da meta da UF.",
        "pergunta": "Qual a fotografia por unidade da federação?",
        "particoes": ["ano"],
        "sql": """
            SELECT
                u.ano,
                u.sigla_uf,
                u.nome_uf,
                u.nome_regiao,
                u.rede,
                u.rede_nome,
                u.taxa_alfabetizacao AS taxa_uf,
                u.media_portugues,
                mu.meta_taxa AS meta_taxa_ano,
                ROUND(u.taxa_alfabetizacao - mu.meta_taxa, 2) AS gap_meta_ano,
                RANK() OVER (
                    PARTITION BY u.ano, u.rede ORDER BY u.taxa_alfabetizacao DESC
                ) AS posicao_nacional
            FROM silver_indicador_uf u
            LEFT JOIN silver_meta_uf mu
              ON mu.sigla_uf = u.sigla_uf
             AND mu.rede = u.rede
             AND mu.ano_meta = u.ano
        """,
    },

    # ------------------------------------------------------------------
    "gold_distribuicao_niveis": {
        "descricao": "Distribuição média dos alunos por nível de proficiência, por UF.",
        "pergunta": "A dificuldade está concentrada nos níveis mais baixos ou é difusa?",
        "particoes": ["ano"],
        "sql": """
            SELECT
                n.ano,
                m.sigla_uf,
                m.nome_regiao,
                n.rede,
                n.rede_nome,
                n.nivel,
                ROUND(AVG(n.proporcao_alunos), 2) AS proporcao_media,
                COUNT(DISTINCT n.id_municipio) AS municipios
            FROM silver_distribuicao_nivel n
            JOIN silver_dim_municipio m
              ON n.id_municipio = m.id_municipio
            GROUP BY n.ano, m.sigla_uf, m.nome_regiao, n.rede, n.rede_nome, n.nivel
        """,
    },

    # ------------------------------------------------------------------
    "gold_perfil_aluno_municipio": {
        "descricao": "Agregação ponderada dos microdados de aluno por município e rede.",
        "pergunta": "Qual a dispersão de proficiência e a participação dentro de cada município?",
        "particoes": ["ano"],
        "sql": """
            SELECT
                ano,
                id_municipio,
                rede,
                rede_nome,
                COUNT(*) AS alunos_cadastrados,
                SUM(CASE WHEN avaliado THEN 1 ELSE 0 END) AS alunos_avaliados,
                ROUND(100.0 * SUM(CASE WHEN avaliado THEN 1 ELSE 0 END) / COUNT(*), 2)
                    AS taxa_participacao,
                ROUND(SUM(CASE WHEN avaliado THEN proficiencia_portugues * peso_aluno END)
                      / NULLIF(SUM(CASE WHEN avaliado THEN peso_aluno END), 0), 2)
                    AS proficiencia_media,
                ROUND(STDDEV(CASE WHEN avaliado THEN proficiencia_portugues END), 2)
                    AS proficiencia_desvio,
                ROUND(MIN(proficiencia_portugues), 2) AS proficiencia_minima,
                ROUND(MAX(proficiencia_portugues), 2) AS proficiencia_maxima,
                ROUND(100.0 * SUM(CASE WHEN avaliado AND alfabetizado THEN peso_aluno END)
                      / NULLIF(SUM(CASE WHEN avaliado THEN peso_aluno END), 0), 2)
                    AS taxa_alfabetizacao_microdados
            FROM silver_aluno
            GROUP BY ano, id_municipio, rede, rede_nome
        """,
    },

    # ------------------------------------------------------------------
    "gold_features_ml": {
        "descricao": "Tabela larga por município: features do ciclo anterior + alvo do ciclo atual.",
        "pergunta": "Dá para prever a taxa do próximo ciclo e priorizar municípios em risco?",
        "particoes": None,
        "sql": """
            WITH marcos AS (
                SELECT MAX(ano) AS ano_alvo FROM silver_alfabetizacao_municipio
            ),
            atual AS (
                SELECT *
                FROM silver_alfabetizacao_municipio
                WHERE ano = (SELECT ano_alvo FROM marcos) AND rede = 3
            ),
            anterior AS (
                SELECT
                    id_municipio,
                    taxa_alfabetizacao      AS taxa_ciclo_anterior,
                    media_portugues         AS media_ciclo_anterior,
                    percentual_participacao AS participacao_ciclo_anterior,
                    taxa_uf                 AS taxa_uf_ciclo_anterior
                FROM silver_alfabetizacao_municipio
                WHERE ano = (SELECT ano_alvo - 1 FROM marcos) AND rede = 3
            ),
            microdados AS (
                SELECT
                    id_municipio,
                    COUNT(*) AS alunos_cadastrados,
                    SUM(CASE WHEN avaliado THEN 1 ELSE 0 END) AS alunos_avaliados,
                    ROUND(SUM(CASE WHEN avaliado THEN proficiencia_portugues * peso_aluno END)
                          / NULLIF(SUM(CASE WHEN avaliado THEN peso_aluno END), 0), 2)
                        AS proficiencia_media,
                    ROUND(STDDEV(CASE WHEN avaliado THEN proficiencia_portugues END), 2)
                        AS proficiencia_desvio
                FROM silver_aluno
                WHERE ano = (SELECT ano_alvo FROM marcos) AND rede = 3
                GROUP BY id_municipio
            )
            SELECT
                a.ano,
                a.id_municipio,
                a.nome_municipio,
                a.sigla_uf,
                a.nome_regiao,
                p.taxa_ciclo_anterior,
                p.media_ciclo_anterior,
                p.participacao_ciclo_anterior,
                p.taxa_uf_ciclo_anterior,
                a.percentual_participacao,
                a.taxa_uf,
                a.taxa_brasil_publica,
                a.meta_taxa_ano,
                a.meta_taxa_2030,
                a.gap_meta_2030,
                d.alunos_cadastrados,
                d.alunos_avaliados,
                d.proficiencia_media,
                d.proficiencia_desvio,
                a.taxa_alfabetizacao AS alvo_taxa_alfabetizacao,
                CASE WHEN a.atingiu_meta_ano THEN 1 ELSE 0 END AS alvo_atingiu_meta
            FROM atual a
            LEFT JOIN anterior p ON a.id_municipio = p.id_municipio
            LEFT JOIN microdados d ON a.id_municipio = d.id_municipio
        """,
    },

    # ------------------------------------------------------------------
    "gold_avaliacao_streaming": {
        "descricao": "Consolidação near-real-time das avaliações recebidas por streaming.",
        "pergunta": "O que está chegando agora e como se compara ao consolidado oficial?",
        "particoes": None,
        "opcional": True,
        "sql": """
            SELECT
                s.id_municipio,
                s.nome_municipio,
                s.sigla_uf,
                s.nome_regiao,
                s.rede_nome,
                COUNT(*) AS avaliacoes_recebidas,
                ROUND(100.0 * SUM(CASE WHEN s.alfabetizado THEN 1 ELSE 0 END) / COUNT(*), 2)
                    AS taxa_stream,
                ROUND(AVG(s.proficiencia_portugues), 2) AS proficiencia_media_stream,
                ROUND(AVG(s.latencia_ms), 1) AS latencia_media_ms,
                MIN(s.ts_evento) AS primeiro_evento,
                MAX(s.ts_evento) AS ultimo_evento
            FROM silver_avaliacao_stream s
            GROUP BY s.id_municipio, s.nome_municipio, s.sigla_uf, s.nome_regiao, s.rede_nome
        """,
    },
}