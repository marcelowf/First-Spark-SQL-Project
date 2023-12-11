# !pip install pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("TDE4").getOrCreate()
schema = "country_or_area string, year int, comm_code int, commodity string, flow string, trade_usd double, weight_kg double, quantity_name string, quantity double, category string"
df = spark.read.option("sep", ";").option("header", "true").schema(schema).csv("transactions_amostra.csv")

# Número de transações envolvendo o Brasil
transacoes_brasil = df.filter(df.country_or_area == "Brazil").count()
contador_df = spark.createDataFrame([(str(transacoes_brasil),)], ["count"])
contador_df.write.text("transações_brasil.txt")

# Número de transações por ano
transacoes_por_ano = df.groupBy("year").count()
transacoes_por_ano_df = spark.createDataFrame([(str(row["year"]) + "," + str(row["count"]),) for row in transacoes_por_ano.limit(5).collect()], ["result"])
transacoes_por_ano_df.write.text("transacoes_por_ano.txt")

# Número de transações por tipo de fluxo e ano
transacoes_por_fluxo_e_ano = df.groupBy("flow", "year").count()
transacoes_por_fluxo_e_ano_df = spark.createDataFrame([(str(row["flow"]) + "," + str(row["year"]) + "," + str(row["count"]),) for row in transacoes_por_fluxo_e_ano.limit(5).collect()], ["result"])
transacoes_por_fluxo_e_ano_df.write.text("transacoes_por_fluxo_e_ano.txt")

# Média dos valores das commodities por ano
valor_medio_por_ano = df.groupBy("year").agg({"trade_usd": "avg"})
valor_medio_por_ano_df = spark.createDataFrame([(str(row["year"]) + "," + str(row["avg(trade_usd)"]),) for row in valor_medio_por_ano.limit(5).collect()], ["result"])
valor_medio_por_ano_df.write.text("valor_medio_por_ano.txt")

# Preço médio das commodities por tipo de unidade, ano e categoria no fluxo de exportação no Brasil
preco_medio_exportacao_brasil = df.filter((df.country_or_area == "Brazil") & (df.flow == "Export")) \
    .groupBy("year", "quantity_name", "category") \
    .agg({"trade_usd": "avg"})
preco_medio_exportacao_brasil_df = spark.createDataFrame([(str(row["year"]) + "," + str(row["quantity_name"]) + "," + str(row["category"]) + "," + str(row["avg(trade_usd)"]),) for row in preco_medio_exportacao_brasil.limit(5).collect()], ["result"])
preco_medio_exportacao_brasil_df.write.text("preco_medio_exportacao_brasil.txt")

# Preço de transação máximo, mínimo e médio por tipo de unidade e ano
preco_por_unidade_e_ano = df.groupBy("year", "quantity_name") \
    .agg(F.max("trade_usd").alias("max_trade_usd"),
         F.min("trade_usd").alias("min_trade_usd"),
         F.avg("trade_usd").alias("avg_trade_usd"))
preco_por_unidade_e_ano_df = spark.createDataFrame([(str(row["year"]) + "," + str(row["quantity_name"]) + "," + str(row["max_trade_usd"]) + "," + str(row["min_trade_usd"]) + "," + str(row["avg_trade_usd"]),) for row in preco_por_unidade_e_ano.limit(5).collect()], ["result"])
preco_por_unidade_e_ano_df.write.text("preco_por_unidade_e_ano.txt")

# Commodity mais comercializada (somando as quantidades) em 2016, por tipo de fluxo
mais_comercializados_2016 = df.filter(df.year == 2016) \
    .groupBy("flow", "commodity") \
    .agg(F.sum("quantity").alias("sum_quantity")) \
    .orderBy("sum_quantity", ascending=False)
mais_comercializados_2016_df = spark.createDataFrame([(str(row["flow"]) + "," + str(row["commodity"]) + "," + str(row["sum_quantity"]),) for row in mais_comercializados_2016.limit(5).collect()], ["result"])
mais_comercializados_2016_df.write.text("mais_comercializados_2016.txt")

# Stop the Spark session
spark.stop()