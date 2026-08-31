# Pipeline de dados do Indicador Criança Alfabetizada

Tech Challenge Fase 2 — Pós Tech AI Scientist

Este projeto junta as seis fontes do Indicador Criança Alfabetizada do Inep numa camada analítica
que dá para consultar por SQL e usar para treinar modelo. Tem ingestão em lote e em streaming,
arquitetura medalhão em Delta Lake, e roda no Databricks.

---

## Sumário

- [Estrutura do repositório](#estrutura-do-repositório)
- [O problema](#o-problema)
- [Arquitetura](#arquitetura)
- [Fluxo de dados](#fluxo-de-dados)
- [Tecnologias](#tecnologias)
- [Decisões e trade-offs](#decisões-e-trade-offs)
- [Qualidade de dados](#qualidade-de-dados)
- [Monitoramento](#monitoramento)
- [FinOps](#finops)
- [Uso em IA](#uso-em-ia)
- [Como rodar](#como-rodar)
- [Resultados da última execução](#resultados-da-última-execução)
- [Limitações](#limitações)

---

## Estrutura do repositório

```
tech_challenge_2_pipeline_dados/
├── apresentacao/
│   ├── PPT_TECH_CHALLENGE.pdf   slides da apresentação
│   └── tech_challenge.mp4       vídeo da apresentação
├── notebooks/
│   ├── 01_bronze_silver.py      ingestão em lote, Bronze, verificações e Silver
│   ├── 02_streaming.py          eventos, fila de erro, janelas e upsert idempotente
│   ├── 03_gold.py               camada analítica, otimização e FinOps
│   └── 04_analytics.py          desigualdade, predição, clusters e priorização
├── src/
│   ├── quality/verificacoes.py  verificações de qualidade em Spark
│   ├── utils/monitoramento.py   instrumentação gravada em Delta
│   ├── gold/consultas.py        as oito consultas da Gold, em SQL
│   └── ingestion/extrair_bigquery.py   extração dos microdados de aluno
├── data/landing/                arquivos de origem, que vão para o volume
├── docs/guia_extracao_bigquery.md
└── requirements.txt
```

Os notebooks estão salvos como arquivo-fonte do Databricks, que é .py com marcador de célula.
O formato foi escolhido porque o diff fica legível no Git. Com .ipynb qualquer execução muda o arquivo inteiro
e o histórico fica inútil.

---

## O problema

O Compromisso Nacional Criança Alfabetizada junta União, estados e municípios em torno de uma coisa:
toda criança alfabetizada até o fim do 2º ano.

Em 2023 o Inep fez a Pesquisa Alfabetiza Brasil e fixou o corte em 743 pontos na escala de leitura do
Saeb. Quem chega lá é considerado alfabetizado, e o percentual de crianças nessa situação virou o
Indicador Criança Alfabetizada.

O objetivo é alfabetizar todo mundo, mas a meta que está publicada tem trajetória ano a ano: 80% até
2030, saindo de 59,9% em 2024. É contra essa trajetória que cada município é comparado.

### Por que não era simples

Para agir, um gestor precisa saber quais municípios estão fora da rota, quanto falta para cada um e
quem está piorando. Nenhuma dessas respostas estava pronta, e o motivo não era volume de dado.

**A rede de ensino não batia entre as bases.** As tabelas de indicador usam código numérico e as
tabelas de meta usam texto. O de-para entre os dois não está publicado em nenhum dos arquivos nem na
documentação. Enquanto isso não fosse resolvido, meta e resultado não se encontravam. Acabei
deduzindo o mapeamento a partir dos próprios dados e conferindo depois.

**O território só vinha como código.** As tabelas do Inep têm o código IBGE de sete dígitos e mais
nada. Sem nome de município, UF e região, não dá para fazer nenhum corte regional, então precisei
buscar isso fora.

**Os grãos são diferentes.** Aluno, município, UF e Brasil estão em arquivos separados. E as metas
vêm com uma coluna por ano enquanto o realizado vem com uma linha por ano, então nem o formato ajuda.

### As seis entidades

| Entidade | Grão | Linhas | Origem |
|---|---|---|---|
| Dados de alunos | 1 linha por aluno | 3.867.999 | BigQuery, via Base dos Dados |
| Indicador por município | ano × município × rede × série | 23.995 | Base dos Dados |
| Meta por município | ano × município × rede | 10.704 | Base dos Dados |
| Município | 1 linha por município | 5.571 | API de Localidades do IBGE |
| Indicador e meta por UF | ano × UF × rede | 199 | Base dos Dados |
| Meta Brasil | ano × rede | 3 | Base dos Dados |
| UF | 1 linha por UF | 27 | API de Localidades do IBGE |

O IBGE é a fonte externa. É ela que torna possível qualquer corte regional.

---

## Arquitetura

Três camadas, com duas rotas de entrada que se encontram no meio.

```
  ORIGEM                    INGESTÃO              ARMAZENAMENTO                 CONSUMO

  Base dos Dados ──┐
  API do IBGE ─────┼──►  lote (PySpark) ───►  BRONZE ──► SILVER ──► GOLD ──►  SQL e BI
  BigQuery ────────┘                          bruto     integrado  analítico   modelos
                                                 │          │          │       priorização
  Eventos de           Structured Streaming       │          │          │
  avaliação      ──►   (micro-lotes)  ───────────►┘          │          │
                              │                   ▼          ▼          ▼
                              └──► fila de erro  gate       gate       gate

  ─────────────────────────────────────────────────────────────────────────────────────
  GOVERNANÇA    Unity Catalog · linhagem por registro · histórico Delta · monitoramento
```

### Bronze

O dado entra como veio, sem descarte e sem renomear coluna. As únicas colunas que acrescentei
começam com underline:

| Coluna | Para quê |
|---|---|
| _ingestion_timestamp e _ingestion_date | quando o dado entrou |
| _source e _source_format | de onde veio e em que formato |
| _entity | qual entidade originou a linha |
| _record_hash | hash do registro, para perceber se a fonte mudou sem avisar |

Se alguma regra mudar depois, dá para reprocessar a partir daqui sem precisar baixar tudo de novo.

### Silver

É onde acontece o trabalho: padronização de tipos, normalização de chaves, decodificação da coluna
rede, tratamento de nulos, deduplicação pelo grão e a junção das seis fontes.

A tabela principal é a fato_alfabetizacao_municipio, no grão ano × município × rede. Ela tem
identificação territorial, o realizado, a meta do ano, a meta de 2030 e a posição relativa dentro da
UF e do país.

A Silver nunca escreve por cima da Bronze.

### Gold

Nove tabelas, uma para cada pergunta. As consultas estão em src/gold/consultas.py, em SQL, então dá
para colar o mesmo texto no editor do Databricks e montar um dashboard.

| Tabela | Pergunta |
|---|---|
| gold_indicador_municipio | Como está cada município e quão longe está da própria meta? |
| gold_meta_vs_realizado | Quais estados estão na rota e quais concentram casos críticos? |
| gold_evolucao_temporal | Quem melhorou, quem piorou, e em quantos anos chega à meta? |
| gold_ranking_uf | Qual a fotografia por unidade da federação? |
| gold_distribuicao_niveis | A dificuldade está nos níveis iniciais ou é difusa? |
| gold_perfil_aluno_municipio | Qual a dispersão de proficiência dentro de cada município? |
| gold_features_ml | Dá para prever a taxa do próximo ciclo? |
| gold_avaliacao_streaming | O que está chegando agora e como se compara ao consolidado? |
| gold_priorizacao_municipio | Em que ordem intervir? |

---

## Fluxo de dados

**Lote.** Entram três formatos. Os cinco arquivos de indicador e de metas são CSV, os microdados de
aluno vêm em Parquet e o território vem em JSON pela API do IBGE.

Em quantidade de arquivo o CSV é maioria, mas em volume não chega perto: o Parquet sozinho tem
3.867.999 linhas contra 34.901 de todos os CSVs juntos. Ou seja, 99% do que entra em lote está em
Parquet.

A chamada da API tem três tentativas com espera crescente. Se a rede do serverless bloquear, a
ingestão usa o CSV que está no volume e grava um alerta. A coluna _source_format diz qual origem foi
usada de fato naquela execução.

**Streaming.** Os eventos chegam em micro-lotes. Cada lote é validado contra um contrato, o que falha
vai para a fila de erro com o motivo, e o que passa é enriquecido com território e gravado na Bronze.
O MERGE INTO do Delta deixa o reprocessamento seguro: se o mesmo lote for reaplicado, nenhuma linha
muda.

**Entre as camadas.** Cada passagem tem um portão de qualidade. Se uma verificação de severidade
ERROR reprovar, a execução para ali e a tarefa do Lakeflow para junto, sem passar dado ruim adiante.

---

## Tecnologias

| Escolha | Por quê |
|---|---|
| Databricks | Lakehouse gerenciado com camada gratuita, o que resolveu a parte de nuvem sem custo |
| Delta Lake | precisava do MERGE para o streaming e do histórico de versões para auditoria |
| Unity Catalog | catálogo, permissões e linhagem no mesmo lugar do dado |
| Structured Streaming | o mesmo motor roda o lote e o streaming, então não vira código duplicado |
| PySpark | são 3,87 milhões de linhas na ingestão |
| pandas e scikit-learn | na análise, onde as tabelas têm milhares de linhas e cabem num nó só |
| API do IBGE | única fonte de território disponível |

---

## Decisões e trade-offs

| Decisão | O que descartei | Ganho | Custo |
|---|---|---|---|
| Lote para o histórico, streaming só na janela | tudo em streaming | computação proporcional ao uso | sem atualização contínua fora da janela |
| Lakehouse com Delta | data lake em Parquet puro | ACID, MERGE e histórico | uma camada a mais para operar |
| Gold sem particionamento físico | partição por ano | arquivos maiores, leitura melhor | sem partition pruning explícito |
| pandas na análise, Spark no processamento | Spark em tudo | modelagem mais direta | dois motores no projeto |

**Lote ou streaming.** O dado oficial sai uma vez por ano, então o histórico não ganha nada com
processamento contínuo. Durante as semanas de aplicação da prova a história muda, porque saber hoje
influencia o que se faz amanhã. Por isso os dois caminhos coexistem.

**Data lake ou data warehouse.** O Lakehouse dá os dois lados: dado em formato aberto e barato, com
as garantias de transação que um warehouse tem. O que pesou na decisão foi precisar do MERGE
idempotente no streaming, que Parquet puro não faz.

**Custo ou performance.** A Gold é reconstruída inteira a cada execução em vez de atualizada aos
poucos. Para 60 mil linhas, rastrear o que mudou daria mais trabalho que recalcular tudo.

---

## Qualidade de dados

As verificações estão em src/quality/verificacoes.py e cobrem seis dimensões:

| Dimensão | Verificação | O que checa |
|---|---|---|
| Completude | linhas_minimas, sem_nulos | campo obrigatório vazio |
| Unicidade | valor_unico, grao_unico | duplicata no grão declarado |
| Validade | dentro_da_faixa, dentro_do_dominio | valor fora da faixa ou do domínio |
| Relacionamento | chave_existe_em | chave que não existe na tabela de referência |
| Consistência | concorda_com | duas tabelas que deveriam bater e não batem |
| Atualidade | contem_ciclo | ciclo esperado ausente |

São 37 verificações por execução, 32 na Bronze e 5 na Gold. Cada uma tem uma severidade: ERROR para
a pipeline, WARNING só registra. A separação existe porque nem todo problema justifica parar tudo.
Os níveis de proficiência que faltam em 2023, por exemplo, são característica da fonte.

### Como a integração foi conferida

Três conferências.

**O de-para da rede.** Como o mapeamento não existe em arquivo nenhum, foi deduzido dos dados e
comparado contra a tabela de metas: 10.584 comparações e 3 divergências, que são inconsistências da
própria fonte.

**Chaves órfãs.** A verificação de integridade referencial cruza os 3.867.999 microdados de aluno e
as quatro tabelas do Inep contra o cadastro do IBGE. Não achou nenhum registro sem correspondência.

**Reconstrução do indicador.** Essa é a conferência mais forte. Aplicando a regra oficial em cima dos
microdados, que é média ponderada por peso amostral contando só quem foi avaliado, a pipeline chega
no mesmo número que o Inep publica:

```
Combinacoes comparadas (ano x municipio x rede) : 12.408
Erro medio na taxa                              : 0,0376 p.p.
Dentro de 0,05 p.p.                             : 95,5% dos casos
Erro no agregado nacional                       : 0,0062 p.p.
```

Qualquer erro na integração apareceria aqui.

### Governança

Cada registro carrega quando entrou, de onde veio, qual execução o gerou e um hash do conteúdo. O
Unity Catalog cuida de catálogo, permissões e linhagem, e o DESCRIBE HISTORY do Delta deixa olhar
qualquer execução passada sem precisar reprocessar.

---

## Monitoramento

O módulo src/utils/monitoramento.py cronometra cada etapa e grava em duas tabelas Delta:

- tc2_monitoramento.execucao_etapa, com etapa, camada, início, duração, linhas de entrada, saída e
  rejeitadas, status e o identificador da execução
- tc2_monitoramento.execucao_alerta, com severidade e mensagem

Isso cobre as quatro coisas que interessam acompanhar: falha de ingestão, latência, volume processado
e alertas.

Se uma etapa levantar exceção, ela é gravada com status FALHA e gera um alerta de severidade alta
antes de a exceção subir. Como fica tudo em tabela, comparar duas execuções é só uma consulta:

```sql
SELECT run_id, pipeline, ROUND(SUM(duracao_s), 1) AS duracao_s, SUM(linhas_saida) AS linhas
FROM   workspace.tc2_monitoramento.execucao_etapa
GROUP  BY run_id, pipeline
ORDER  BY MIN(inicio) DESC
```

---

## FinOps

A nuvem cobra por três coisas, e cada uma tem um jeito de reduzir:

| O que é cobrado | Medida adotada |
|---|---|
| GB armazenado | Parquet comprimido; 8 milhões de linhas dão menos de 250 MB |
| bytes escaneados na consulta | projeção de colunas, OPTIMIZE com ZORDER, data skipping |
| tempo de computação | streaming em micro-lotes agendados, sem processo ligado direto |

### Bytes escaneados

Peguei a mesma pergunta, taxa de alfabetização por município na rede Municipal em 2024, e respondi de
quatro formas diferentes. Os bytes foram medidos no rodapé do Parquet:

| Estratégia | Lido | Contra o CSV |
|---|---|---|
| CSV, varredura completa | 1,3 MB | 100% |
| Parquet sem particionamento | 777 KB | 57% |
| Parquet com partição por ano | 415 KB | 30% |
| Parquet com partição e projeção de colunas | 150 KB | 11% |

Nove vezes menos bytes para chegar no mesmo resultado.

### Quanto custaria

A estimativa usa as durações reais que o monitoramento gravou, em vez de número chutado. Com quatro
execuções em lote por mês e o streaming só nas seis semanas de aplicação da prova, dá algo em torno
de US$ 0,43 por mês. A execução atual roda na Free Edition e não cobra nada.

O valor é pequeno, mas o que importa mais aqui é que a arquitetura não deixa nada ligado o tempo
todo. Não tem broker esperando mensagem nem cluster parado, e o custo acompanha o tempo de execução,
que é de minutos. Isso já evita o desperdício mais comum em pipeline de dados.

---

## Uso em IA

A Gold já serve como base de treino, por três motivos.

**O grão está certo.** Uma linha por município e ciclo, que é a unidade em que a política pública é
decidida. Então é também a unidade em que o modelo deve prever.

**Não tem vazamento temporal.** A gold_features_ml traz as variáveis do ciclo anterior e o alvo do
ciclo atual em colunas separadas. Nada do futuro entra no treino. Esse foi o cuidado que mais deu
trabalho, porque a tabela tem colunas do ciclo atual que seriam tentadoras de usar e inflariam o R².

**O contexto já vem junto.** Meta do município, desempenho da UF, região e participação estão na
mesma linha, e é com isso que o modelo consegue ir além de repetir o valor do ano anterior.

### O que dá para fazer com isso

**Prever.** Estimar a taxa do próximo ciclo antes da avaliação acontecer, para agir no ano corrente
em vez de reagir dois anos depois. Treinei um modelo só para conferir se a base servia mesmo:

| Abordagem | Erro médio | R² | Ganho |
|---|---|---|---|
| Repetir o ciclo anterior | 12,13 p.p. | 0,246 | — |
| Ridge | 9,80 p.p. | 0,560 | 19,2% |
| Random Forest | 8,86 p.p. | 0,606 | 27,0% |

**Agrupar.** Juntar municípios por perfil de vulnerabilidade em vez de ordenar num ranking. Sem
supervisão aparecem três perfis: vulnerabilidade crítica com 1.630 municípios e taxa média de 39,6%,
atenção prioritária com 2.395 e 64,9%, e em rota de melhoria com 1.423 e 85,8%. Isso importa porque
dificuldade difusa pede intervenção diferente de dificuldade concentrada nos níveis iniciais.

**Priorizar.** Um índice que junta distância da meta, trajetória e posição relativa dentro da UF,
gravado de volta na Gold como gold_priorizacao_municipio. Dá 401 municípios em prioridade crítica,
cada um com o quanto falta e o esforço anual necessário. O resultado não fica preso no notebook, vira
tabela que dá para consultar.

### O que ainda daria para fazer sem mudar nada de esquema

- Detecção de anomalia sobre a gold_avaliacao_streaming, para sinalizar queda durante a aplicação da
  prova em vez de meses depois
- Previsão de trajetória em vez de um ponto só, quando tiver mais ciclos publicados
- Juntar Censo Escolar e FUNDEB na gold_features_ml, que é o caminho para sair de onde está o
  problema e chegar em por quê

---

## Como rodar

Precisa de um workspace Databricks com Unity Catalog. A Free Edition dá conta.

### 1. Subir os arquivos

Criar o volume e mandar os oito arquivos de data/landing:

```
Catalog > workspace > tc2_landing > arquivos
```

O Parquet dos microdados não está no Git porque tem 53 MB. O passo a passo para gerar está em
docs/guia_extracao_bigquery.md, ou direto:

```bash
python src/ingestion/extrair_bigquery.py --projeto SEU_PROJETO_GCP
```

### 2. Importar o repositório

```
Workspace > Create > Git folder > https://github.com/eriicpa/repo_alfabetizacao.git
```

Precisa ser Git folder, senão os notebooks não acham o src/.

### 3. Rodar na ordem

01_bronze_silver, 02_streaming, 03_gold e 04_analytics.

Dá uns três minutos no total. Catálogo, schemas e volume são widgets no topo de cada notebook, então
uma tarefa do Lakeflow consegue trocar qualquer um deles sem mexer no código.

---

## Resultados da última execução

| | |
|---|---|
| Tabelas nas quatro camadas | 31 |
| Linhas processadas | 8.048.264 |
| Microdados de aluno | 3.867.999 |
| Verificações de qualidade | 37, todas aprovadas |
| Chaves órfãs | 0 |
| Etapas monitoradas | 15, nenhuma com falha |
| Duração total | 160 s |
| Erro na reconstrução do indicador | 0,0062 p.p. |
| Eventos em streaming | 1.133 válidos, 67 na fila de erro (5,6%) |
| Bytes lidos por consulta otimizada | 11% do CSV equivalente |

---

## Limitações

**Só dois ciclos de dados.** O indicador tem poucas edições publicadas, então qualquer projeção
temporal é frágil. As estimativas de anos até a meta são extrapolação linear do ritmo entre dois
pontos, e servem para sinalizar, não para prever.

**Tem uma quebra de série na origem.** A verificação de sanidade achou uma UF com variação de
−18,42 p.p. em 480 municípios enquanto a mediana nacional subia 2,19 p.p. O padrão tem cara de
mudança de metodologia, não de queda real de aprendizagem. O efeito foi neutralizado no índice de
priorização e os municípios ficaram marcados, mas o dado em si não dá para corrigir daqui.

**O modelo mostra associação.** Ele acha municípios com perfil parecido com os que pioraram, mas não
diz o que causou a piora.

**Nem todo município tem meta.** Alguns não aparecem no arquivo de metas do Inep. Esses ficam de fora
das comparações e são listados na saída.

**Os eventos de streaming são simulados.** A lógica é real, com validação, fila de erro, janelas e
MERGE idempotente, mas os eventos são gerados para demonstração e a origem é um diretório de arquivos
em vez de um broker. Trocar para Kafka seria mudança de configuração.

---

## Fontes

- [Indicador Criança Alfabetizada](https://basedosdados.org/dataset/br-inep-avaliacao-alfabetizacao) — Base dos Dados / Inep
- [API de Localidades](https://servicodados.ibge.gov.br/api/docs/localidades) — IBGE
