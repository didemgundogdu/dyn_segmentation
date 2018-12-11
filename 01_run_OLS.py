# Databricks notebook source
from pyspark.sql.functions import *
cust_query = spark.sql("select int(codice_cliente), TO_DATE(CAST(UNIX_TIMESTAMP(data_rif, 'dd/MM/yyyy') AS TIMESTAMP)) as new_date, persona_fis_giur, \
                        descrizione_stato_civile, flag_email, int(eta), classe_eta, classe_anzianita, sesso, area_geografica, \
                        classe_professione, classe_tit_studio, classe_patrimonio_estesa, prima_banca, num_riass_36mesi, flag_dossier, flag_gestioni,\
                        classi_probab_abbandono_12m, flag_protezione, flag_carta_debito, flag_carta_credito, \
                        flag_bb2_medbond, flag_bb2_pct,  flag_medplus,flag_vita,flag_ul, flag_ul_europension, flag_ul_mypension,\
                        flag_fondi, flag_fondi_terzi, flag_ul_tbold, flag_ul_tbnew, flag_ul_mylife, \
                        int(patrimonio_totale), int(patrimonio_gestito), int(patrimonio_amministrato), int(patr_cc), \
                        int(patr_time_deposit), int(patr_obbl_strutt), int(patr_bb2_medbond), int(patr_bb2_pct), int(patr_fondi), \
                        int(patr_fondi_terzi), int(patr_vita), int(patr_medplus), int(patr_ul),\
                        int(patr_ul_europension), int(patr_ul_mypension), int(patr_ul_tbold), int(patr_ul_tbnew), int(patr_ul_mylife), \
                        int(patr_vita_trad), int(impieghi_mutuo),  int(impieghi_prestito),int(impieghi_fido), int(impieghi_fido_mutuo_prestito),\
                        connessioni_totali, num_linee_prodotto, c_mobile, c_internet, c_operatore, c_ivr_nooperatore ,\
                        classe_impieghi_mutuo, classe_impieghi_prestito, classe_impieghi_fido, classe_impiego_fido_mutuo_prest \
               from 201808cleaneddata_parquet where flag_primo_intestatario = 1 order by codice_cliente , new_date ASC")
# df = cust_query.select (year("new_date").alias('year'), month("new_date").alias('month'),"new_date","patrimonio_amministrato")
cust_query = cust_query.withColumn('new_date2', regexp_replace('new_date', '-', ''))
# df = cust_query.select("codice_cliente", col("new_date2").cast('int'), "patrimonio_amministrato","patrimonio_gestito").dropna()
# display(df)

# COMMAND ----------

from pyspark.sql.functions import *
cust_query2017 = spark.sql("select int(codice_cliente), TO_DATE(CAST(UNIX_TIMESTAMP(data_rif, 'dd/MM/yyyy') AS TIMESTAMP)) as new_date, persona_fis_giur, \
                        descrizione_stato_civile, flag_email, int(eta), classe_eta, classe_anzianita, sesso, area_geografica, \
                        classe_professione, classe_tit_studio, classe_patrimonio_estesa, prima_banca, num_riass_36mesi, flag_dossier, flag_gestioni,\
                        classi_probab_abbandono_12m, flag_protezione, flag_carta_debito, flag_carta_credito, \
                        flag_bb2_medbond, flag_bb2_pct,  flag_medplus,flag_vita,flag_ul, flag_ul_europension, flag_ul_mypension,\
                        flag_fondi, flag_fondi_terzi, flag_ul_tbold, flag_ul_tbnew, flag_ul_mylife,\
                        int(patrimonio_totale), int(patrimonio_gestito), int(patrimonio_amministrato), int(patr_cc), \
                        int(patr_time_deposit), int(patr_obbl_strutt), int(patr_bb2_medbond), int(patr_bb2_pct), int(patr_fondi), \
                        int(patr_fondi_terzi), int(patr_vita), int(patr_medplus), int(patr_ul),\
                        int(patr_ul_europension), int(patr_ul_mypension), int(patr_ul_tbold), int(patr_ul_tbnew), int(patr_ul_mylife), \
                        int(patr_vita_trad), int(impieghi_mutuo),  int(impieghi_prestito),int(impieghi_fido), int(impieghi_fido_mutuo_prestito),\
                        connessioni_totali, num_linee_prodotto, c_mobile, c_internet, c_operatore, c_ivr_nooperatore ,\
                        classe_impieghi_mutuo, classe_impieghi_prestito, classe_impieghi_fido, classe_impiego_fido_mutuo_prest \
                   from 20170512cleaneddata_parquet where flag_primo_intestatario = 1 order by codice_cliente , new_date ASC")
# df = cust_query.select (year("new_date").alias('year'), month("new_date").alias('month'),"new_date","patrimonio_amministrato")
cust_query2017 = cust_query2017.withColumn('new_date2', regexp_replace('new_date', '-', ''))

# COMMAND ----------

from pyspark.sql.functions import *
cust_query2016 = spark.sql("select int(codice_cliente), TO_DATE(CAST(UNIX_TIMESTAMP(data_rif, 'dd/MM/yyyy') AS TIMESTAMP)) as new_date, persona_fis_giur, \
                        descrizione_stato_civile, flag_email, int(eta), classe_eta, classe_anzianita, sesso, area_geografica, \
                        classe_professione, classe_tit_studio, classe_patrimonio_estesa, prima_banca, num_riass_36mesi, flag_dossier, flag_gestioni,\
                        classi_probab_abbandono_12m, flag_protezione, flag_carta_debito, flag_carta_credito, \
                        flag_bb2_medbond, flag_bb2_pct,  flag_medplus,flag_vita,flag_ul, flag_ul_europension, flag_ul_mypension,\
                        flag_fondi, flag_fondi_terzi, flag_ul_tbold, flag_ul_tbnew, flag_ul_mylife, \
                        int(patrimonio_totale), int(patrimonio_gestito), int(patrimonio_amministrato), int(patr_cc), \
                        int(patr_time_deposit), int(patr_obbl_strutt), int(patr_bb2_medbond), int(patr_bb2_pct), int(patr_fondi), \
                        int(patr_fondi_terzi), int(patr_vita), int(patr_medplus), int(patr_ul),\
                        int(patr_ul_europension), int(patr_ul_mypension), int(patr_ul_tbold), int(patr_ul_tbnew), int(patr_ul_mylife), \
                        int(patr_vita_trad), int(impieghi_mutuo),  int(impieghi_prestito),int(impieghi_fido), int(impieghi_fido_mutuo_prestito),\
                        connessioni_totali, num_linee_prodotto, c_mobile, c_internet, c_operatore, c_ivr_nooperatore ,\
                        classe_impieghi_mutuo, classe_impieghi_prestito, classe_impieghi_fido, classe_impiego_fido_mutuo_prest \
                   from 20162017cleaneddata_parquet where flag_primo_intestatario = 1 order by codice_cliente , new_date ASC")
# df = cust_query.select (year("new_date").alias('year'), month("new_date").alias('month'),"new_date","patrimonio_amministrato")
cust_query2016 = cust_query2016.withColumn('new_date2', regexp_replace('new_date', '-', ''))

# COMMAND ----------

df_concat = cust_query.union(cust_query2017)
df_concat2 = df_concat.union(cust_query2016)
df_concat = df_concat2.orderBy(['codice_cliente','new_date2'], ascending=[0, 1])

# COMMAND ----------

df_concat.write.format("parquet").saveAsTable('df_concat',  mode = "overwrite")

# COMMAND ----------

df_concat.columns

# COMMAND ----------

df_clients = df_concat.select(col('codice_cliente').cast('int')).orderBy(['codice_cliente'], ascending=[1])
num_client = df_clients.toPandas()['codice_cliente'].unique()

# COMMAND ----------

df_clients.distinct().write.format("parquet").saveAsTable('clientlist', mode = "overwrite")

# COMMAND ----------

import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *

mydf = spark.createDataFrame(pd.DataFrame(num_client)) 
mydf.write.format("parquet").saveAsTable('clientlist', mode = "overwrite")

# COMMAND ----------

mydf.select(max('0')).show()

# COMMAND ----------

mypdf = mydf.toPandas()

# COMMAND ----------

import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *

#fieldListTrend = ['patrimonio_totale','patrimonio_gestito','patrimonio_amministrato', 'patr_cc', 'patr_time_deposit', 'patr_obbl_strutt', 'patr_bb2_medbond', 'patr_bb2_pct','patr_fondi', 'patr_fondi_terzi', 'patr_vita', 'patr_medplus', 'patr_ul', 'patr_ul_europension','patr_ul_mypension','patr_ul_tbold','patr_ul_tbnew', 'patr_ul_mylife', 'patr_vita_trad', 'impieghi_mutuo' ,  'impieghi_prestito','impieghi_fido', 'num_linee_prodotto', 'c_mobile', 'c_internet', 'c_operatore','c_ivr_nooperatore','connessioni_totali']
                  
fieldListTrend = ['c_ivr_nooperatore']
  
for y_column in fieldListTrend : 
  
  group_column = 'codice_cliente'
  x_columns = 'new_date2'
  colname = y_column + '_coeff'

  pdf = df_concat.select(group_column,col(x_columns).cast('int') ,y_column).dropna()
  
  schema = StructType([
      StructField(group_column, IntegerType(), True),
      StructField(colname, FloatType(), True)])


# Input/output are both a pandas.DataFrame
  @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
  def smfols(pdf):
    tpd = pd.DataFrame()
    group_key = pdf[group_column].iloc[0]
  #  if (pdf.count() > 8) :
  #     results = smf.ols( 'patrimonio_amministrato ~ new_date2', data=pdf).fit()
    results = smf.ols( y_column + '~' + x_columns, data=pdf).fit()
    tpd = pd.DataFrame([[group_key, results.params[1]]])
    return tpd

  beta = pdf.groupby(group_column).apply(smfols)
  filename = y_column + '_trend'
  beta.write.format("parquet").saveAsTable(filename, mode = "overwrite")


# COMMAND ----------

y_column

# COMMAND ----------

import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import *


a = df_concat.filter(col('codice_cliente') == 16386)

group_column = 'codice_cliente'
y_column = 'patrimonio_totale'
x_columns = 'new_date2'
colname = y_column + '_coeff'

pdf = a.select(group_column,col(x_columns).cast('int') ,y_column).dropna()

# Input/output are both a pandas.DataFrame
group_key = pdf[group_column].iloc[0]

#     results = smf.ols( 'patrimonio_amministrato ~ new_date2', data=pdf).fit()
results = smf.ols( y_column + '~' + x_columns, data=pdf).fit()
    

# COMMAND ----------

display(a.select(col("patrimonio_totale")))

# COMMAND ----------


