# First-Spark-SQL-Project

Este projeto realiza análises sobre um conjunto de dados de transações comerciais utilizando PySpark. A coleção de dados utilizada não está disponível publicamente, mas o código pode ser adaptado para conjuntos de dados similares.

## Utilização do Código
O script em Python usa o PySpark para realizar diversas análises sobre as transações comerciais. Abaixo estão as principais análises realizadas:

### 1. Número de Transações Envolvendo o Brasil
Conta o número de transações envolvendo o Brasil e salva o resultado em transações_brasil.txt.

### 2. Número de Transações por Ano
Calcula o número de transações por ano e salva os resultados em transacoes_por_ano.txt.

### 3. Número de Transações por Tipo de Fluxo e Ano
Analisa o número de transações por tipo de fluxo e ano, salvando os resultados em transacoes_por_fluxo_e_ano.txt.

### 4. Média dos Valores das Commodities por Ano
Calcula a média dos valores das commodities por ano e salva os resultados em valor_medio_por_ano.txt.

### 5. Preço Médio das Commodities por Tipo de Unidade, Ano e Categoria no Fluxo de Exportação no Brasil
Determina o preço médio das commodities por tipo de unidade, ano e categoria no fluxo de exportação no Brasil, salvando os resultados em preco_medio_exportacao_brasil.txt.

### 6. Preço de Transação Máximo, Mínimo e Médio por Tipo de Unidade e Ano
Analisa o preço de transação máximo, mínimo e médio por tipo de unidade e ano, salvando os resultados em preco_por_unidade_e_ano.txt.

### 7. Commodities Mais Comercializadas (somando as quantidades) em 2016, por Tipo de Fluxo
Identifica as commodities mais comercializadas (somando as quantidades) em 2016, por tipo de fluxo, e salva os resultados em mais_comercializados_2016.txt.

## Instalação de Dependências

Certifique-se de ter o PySpark instalado executando o seguinte comando:

```bash
!pip install pyspark
