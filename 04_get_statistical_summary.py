# Databricks notebook source
from pyspark.sql.functions import *
data = spark.sql("select * from df_concat where persona_fis_giur  = 'F'").orderBy(['codice_cliente','new_date2'], ascending=[0, 0])
df_clients = data.select(col('codice_cliente').cast('int')).orderBy(['codice_cliente'], ascending=[1])

client_list = df_clients.distinct()

# COMMAND ----------

df2 = data.dropDuplicates(['codice_cliente'])

# COMMAND ----------

display(df2)

# COMMAND ----------

totalClient = client_list.count()

# COMMAND ----------

totalClient

# COMMAND ----------

df2.select(col("classe_patrimonio_estesa")).show(n=2)

# COMMAND ----------

df2.filter(col("classe_patrimonio_estesa").contains('12:')).count() / totalClient

# COMMAND ----------

df2.filter((col("flag_bb2_medbond") == 1) | (col("flag_bb2_pct") == 1) | (col("flag_medplus") == 1)).count() / totalClient

# COMMAND ----------

df2.filter((col("flag_vita") == 1) | (col("flag_ul") == 1) | (col("flag_ul_europension")==1) | (col("flag_ul_mypension") == 1) | (col("flag_ul_tbold") == 1) | (col("flag_ul_tbnew") == 1) | (col("flag_ul_mylife") == 1) | (col("flag_fondi")==1) | (col("flag_fondi_terzi") == 1) | (col("flag_gestioni") == 1)).count() / totalClient


# COMMAND ----------

df2.select("classe_impiego_fido_mutuo_prest").distinct().show()

# COMMAND ----------

df2.columns

# COMMAND ----------

df2.filter(col("classe_impiego_fido_mutuo_prest").contains("9)")).count() / totalClient

# COMMAND ----------

  
