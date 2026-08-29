# Guia de extração — BigQuery Sandbox

Como obter as duas tabelas do dataset `br_inep_avaliacao_alfabetizacao` que **não cabem no download
gratuito** do portal da Base dos Dados. Custo: zero. Cartão de crédito: não precisa.

| Tabela | Tamanho | Por que precisamos |
|---|---|---|
| `alunos` | 3.867.999 linhas · 256 MB | microdados no grão de aluno — a sexta fonte integrada na Silver |
| `dicionario` | pequena | significado **oficial** dos códigos (`rede`, `serie`) |

O BigQuery Sandbox dá **1 TiB de consulta por mês**, sem conta de faturamento. Ler a tabela inteira
de alunos consome cerca de 0,02% dessa cota.

---

## 1. Criar o projeto no BigQuery Sandbox

1. Acesse <https://console.cloud.google.com/bigquery> com uma conta Google comum.
2. Aceite os termos. Quando aparecer a opção, escolha o **Sandbox** — não ative faturamento.
3. Crie um projeto (ou use o que o console sugerir) e **anote o ID do projeto**. É algo como
   `meu-projeto-123456` — não é o nome de exibição.

> **Limitações do sandbox que valem saber:** tabelas que você criar expiram em 60 dias, não há
> exportação para Cloud Storage e não há streaming. Nada disso nos afeta: só vamos **ler** tabelas
> públicas e trazer o resultado para o disco local.

---

## 2. Autenticar a máquina

Instale o [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) e rode uma vez:

```bash
gcloud auth application-default login
```

O navegador abre, você escolhe a mesma conta Google e pronto. A credencial fica salva na máquina.

Depois instale as bibliotecas de acesso:

```bash
pip install google-cloud-bigquery db-dtypes pyarrow
```

---

## 3. Rodar a extração

Da raiz do repositório:

```bash
python -m src.ingestion.extrair_bigquery --projeto SEU-PROJETO-AQUI
```

O script faz, nesta ordem:

1. **Dry run** de cada consulta — estima quantos bytes serão processados **antes** de executar, sem
   custo nenhum. É projeção de colunas e filtro de partição aplicados antes de executar.
2. Extrai a tabela `dicionario` para `data/landing/dicionario.csv` e **imprime o mapeamento oficial
   da coluna `rede`**.
3. Extrai os microdados de aluno com projeção de colunas (10 das 12) e filtro de ano.
4. Normaliza os tipos e grava em `data/landing/alunos.parquet`.
5. Mostra a taxa de alfabetização reconstruída a partir dos microdados reais.

### Variações

```bash
# Só o dicionário (rápido, resolve a questão do mapeamento de 'rede')
python -m src.ingestion.extrair_bigquery --projeto SEU-PROJETO --so-dicionario

# Amostra de até 60 alunos por município/rede/ano — arquivo bem menor
python -m src.ingestion.extrair_bigquery --projeto SEU-PROJETO --amostra 60

# Outro recorte de anos
python -m src.ingestion.extrair_bigquery --projeto SEU-PROJETO --anos 2024
```

**Qual usar?** Se a máquina tiver RAM sobrando, extraia completo — são microdados reais e o argumento
de "pipeline que aguenta volume" fica concreto. Se for rodar tudo no Colab gratuito, use
`--amostra 60`: dá cerca de 300 mil linhas, roda leve e continua sendo dado real.

O arquivo `alunos.parquet` está no `.gitignore` por causa do tamanho. O `dicionario.csv` é pequeno e
**deve ser versionado**.

---

## 4. Reexecutar a pipeline

Só rodar o Notebook 01 de novo. Ele detecta o arquivo sozinho — a ingestão tenta, nesta ordem:

1. `data/landing/alunos.parquet` → marca a origem como **real**
2. BigQuery direto, se `GCP_BILLING_PROJECT` estiver definido → **real**
3. simulação calibrada → **simulado**

O mesmo vale para o dicionário: se `data/landing/dicionario.csv` existir, o notebook usa o mapeamento
oficial de `rede` e imprime isso na saída. Senão, usa o mapeamento do Inep e valida contra os dados.

Depois é só seguir com os notebooks 02, 03 e 04 normalmente.

---

## 5. Conferir o que mudou

Rode `notebooks/00_explorar_lake.ipynb` e compare:

| Antes | Depois |
|---|---|
| `_source = simulado::calibrado_por_indicador_municipal` | `_source = bigquery::basedosdados...alunos` |
| ~228 mil alunos gerados | microdados reais |
| Alerta de simulação no painel de monitoramento | sem alerta |

E no Notebook 01, a saída da Etapa 1.4 passa a dizer
`Dicionário de rede: dicionario oficial do dataset`.

---

## Ponto de atenção nos dados reais

A tabela de alunos tem duas colunas que a versão simulada não tem, e que merecem decisão consciente:

- **`presenca`** — aluno ausente na aplicação não deveria contar no indicador. O script imprime os
  valores encontrados nessa coluna; confira e, se necessário, filtre na camada Silver.
- **`peso_aluno`** — peso amostral. Para reproduzir a taxa oficial com precisão, a agregação deveria
  ser **ponderada** por essa coluna, não uma média simples.

Os dois pontos não estavam na versão simulada porque lá não existiam. Depois de extrair, vale
verificar os valores impressos e ajustar a agregação — é exatamente o tipo de detalhe que separa
"reproduzi o número" de "calculei um número parecido".
