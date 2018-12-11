# Databricks notebook source
from pyspark.sql.functions import *
clientlist = spark.sql("select * from clientlist")

patrimonio_totale_trend = spark.sql("select * from patrimonio_totale_trend")
patrimonio_amministrato_trend = spark.sql("select * from patrimonio_amministrato_trend")                
patrimonio_gestito_trend = spark.sql("select * from patrimonio_gestito_trend")
patr_cc_trend = spark.sql("select * from patr_cc_trend")
patr_time_deposit_trend = spark.sql("select * from patr_time_deposit_trend")
patr_ul_trend = spark.sql("select * from patr_ul_trend")
patr_obbl_strutt_trend = spark.sql("select * from patr_obbl_strutt_trend")
patr_bb2_medbond_trend = spark.sql("select * from patr_bb2_medbond_trend")
patr_bb2_pct_trend = spark.sql("select * from patr_bb2_pct_trend")
patr_fondi_trend = spark.sql("select * from patr_fondi_trend")
patr_fondi_terzi_trend = spark.sql("select * from patr_fondi_terzi_trend")
patr_vita_trend = spark.sql("select * from patr_vita_trend")
patr_medplus_trend = spark.sql("select * from patr_medplus_trend")
patr_ul_europension_trend = spark.sql("select * from patr_ul_europension_trend")
patr_ul_mypension_trend = spark.sql("select * from patr_ul_mypension_trend")
patr_ul_tbold_trend = spark.sql("select * from patr_ul_tbold_trend")
patr_ul_tbnew_trend = spark.sql("select * from patr_ul_tbnew_trend")
patr_ul_mylife_trend = spark.sql("select * from patr_ul_mylife_trend")
patr_vita_trad_trend = spark.sql("select * from patr_vita_trad_trend")
impieghi_mutuo_trend = spark.sql("select * from impieghi_mutuo_trend")
impieghi_prestito_trend = spark.sql("select * from impieghi_prestito_trend")
impieghi_fido_trend = spark.sql("select * from impieghi_fido_trend")
num_linee_prodotto_trend = spark.sql("select * from num_linee_prodotto_trend")
c_mobile_trend = spark.sql("select * from c_mobile_trend")
c_internet_trend = spark.sql("select * from c_internet_trend")
c_operatore_trend = spark.sql("select * from c_operatore_trend")
c_ivr_nooperatore_trend = spark.sql("select * from c_ivr_nooperatore_trend")
connessioni_totali_trend = spark.sql("select * from connessioni_totali_trend")

# COMMAND ----------

display(patrimonio_amministrato_trend)

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

def categ_func(coeff):
    if coeff <= -0.05:
        return -1
    if coeff > 0.05:
        return 1
    return 0

func_udf = udf(categ_func, IntegerType())
patrimonio_amministrato_trend = patrimonio_amministrato_trend.withColumn('patr_amm_trend',func_udf(patrimonio_amministrato_trend['patrimonio_amministrato_coeff']))
patrimonio_gestito_trend = patrimonio_gestito_trend.withColumn('patr_gest_trend',func_udf(patrimonio_gestito_trend['patrimonio_gestito_coeff']))
patrimonio_totale_trend = patrimonio_totale_trend.withColumn('patr_tot_trend',func_udf(patrimonio_totale_trend['patrimonio_totale_coeff']))

# COMMAND ----------

patr_cc_trend = patr_cc_trend.withColumn('patr_cc_trend',func_udf(patr_cc_trend['patr_cc_coeff']))
patr_time_deposit_trend = patr_time_deposit_trend.withColumn('patr_time_deposit_trend',func_udf(patr_time_deposit_trend['patr_time_deposit_coeff']))
patr_ul_trend = patr_ul_trend.withColumn('patr_ul_trend',func_udf(patr_ul_trend['patr_ul_coeff']))
patr_vita_trend = patr_vita_trend.withColumn('patr_vita_trend',func_udf(patr_vita_trend['patr_vita_coeff']))
patr_obbl_strutt_trend = patr_obbl_strutt_trend.withColumn('patr_obbl_strutt_trend', func_udf(patr_obbl_strutt_trend['patr_obbl_strutt_coeff']))

# COMMAND ----------

patr_bb2_medbond_trend = patr_bb2_medbond_trend.withColumn('patr_bb2_medbond_trend',func_udf(patr_bb2_medbond_trend['patr_bb2_medbond_coeff']))
patr_bb2_pct_trend = patr_bb2_pct_trend.withColumn('patr_bb2_pct_trend',func_udf(patr_bb2_pct_trend['patr_bb2_pct_coeff']))
patr_fondi_trend = patr_fondi_trend.withColumn('patr_fondi_trend',func_udf(patr_fondi_trend['patr_fondi_coeff']))
patr_fondi_terzi_trend = patr_fondi_terzi_trend.withColumn('patr_fondi_terzi_trend', func_udf(patr_fondi_terzi_trend['patr_fondi_terzi_coeff']))
patr_medplus_trend = patr_medplus_trend.withColumn('patr_medplus_trend', func_udf(patr_medplus_trend['patr_medplus_coeff']))

# COMMAND ----------

patr_ul_europension_trend = patr_ul_europension_trend.withColumn('patr_ul_europension_trend',func_udf(patr_ul_europension_trend['patr_ul_europension_coeff']))
patr_ul_mypension_trend = patr_ul_mypension_trend.withColumn('patr_ul_mypension_trend',func_udf(patr_ul_mypension_trend['patr_ul_mypension_coeff']))
patr_ul_tbold_trend = patr_ul_tbold_trend.withColumn('patr_ul_tbold_trend',func_udf(patr_ul_tbold_trend['patr_ul_tbold_coeff']))
patr_ul_tbnew_trend = patr_ul_tbnew_trend.withColumn('patr_ul_tbnew_trend',func_udf(patr_ul_tbnew_trend['patr_ul_tbnew_coeff']))
patr_ul_mylife_trend = patr_ul_mylife_trend.withColumn('patr_ul_mylife_trend',func_udf(patr_ul_mylife_trend['patr_ul_mylife_coeff']))
patr_vita_trad_trend = patr_vita_trad_trend.withColumn('patr_vita_trad_trend',func_udf(patr_vita_trad_trend['patr_vita_trad_coeff']))

# COMMAND ----------

impieghi_mutuo_trend = impieghi_mutuo_trend.withColumn('imp_mutuo_trend',func_udf(impieghi_mutuo_trend['impieghi_mutuo_coeff']))
impieghi_prestito_trend = impieghi_prestito_trend.withColumn('impieghi_prestito_trend', func_udf(impieghi_prestito_trend['impieghi_prestito_coeff']))
impieghi_fido_trend = impieghi_fido_trend.withColumn('impieghi_fido_trend',func_udf(impieghi_fido_trend['impieghi_fido_coeff']))

# COMMAND ----------

c_mobile_trend =  c_mobile_trend.withColumn('c_mobile_trend',func_udf(c_mobile_trend['c_mobile_coeff']))
c_internet_trend = c_internet_trend.withColumn('c_internet_trend',func_udf(c_internet_trend['c_internet_coeff']))
c_operatore_trend = c_operatore_trend.withColumn('c_operatore_trend',func_udf(c_operatore_trend['c_operatore_coeff']))
c_ivr_nooperatore_trend = c_ivr_nooperatore_trend.withColumn('c_ivr_nooperatore_trend',func_udf(c_ivr_nooperatore_trend['c_ivr_nooperatore_coeff']))
connessioni_totali_trend = connessioni_totali_trend.withColumn('conn_totali_trend',func_udf(connessioni_totali_trend['connessioni_totali_coeff']))
num_linee_prodotto_trend = num_linee_prodotto_trend.withColumn('num_linee_prod_trend',func_udf(num_linee_prodotto_trend['num_linee_prodotto_coeff']))

# COMMAND ----------

df1 = patrimonio_amministrato_trend.join(patrimonio_gestito_trend, "codice_cliente","fullouter")
df2 = df1.join(patrimonio_totale_trend, "codice_cliente","fullouter")
df3 = df2.join(patr_cc_trend,"codice_cliente","fullouter")
df4 = df3.join(patr_time_deposit_trend,"codice_cliente","fullouter")
df5 = df4.join(patr_ul_trend,"codice_cliente","fullouter")
df6 = df5.join(patr_obbl_strutt_trend,"codice_cliente","fullouter")
df7 = df6.join(patr_bb2_medbond_trend,"codice_cliente","fullouter")
df8 = df7.join(patr_bb2_pct_trend,"codice_cliente","fullouter")
df9 = df8.join(patr_fondi_trend,"codice_cliente","fullouter")
df10 = df9.join(patr_fondi_terzi_trend,"codice_cliente","fullouter")             
df11 = df10.join(patr_vita_trend,"codice_cliente","fullouter")
df12 = df11.join(patr_medplus_trend,"codice_cliente","fullouter")
df13 = df12.join(patr_ul_europension_trend,"codice_cliente","fullouter")
df14 = df13.join(patr_ul_mypension_trend,"codice_cliente","fullouter")
df15 = df14.join(patr_ul_tbold_trend,"codice_cliente","fullouter")
df16 = df15.join(patr_ul_tbnew_trend,"codice_cliente","fullouter")
df17 = df16.join(patr_ul_mylife_trend,"codice_cliente","fullouter")
df18 = df17.join(patr_vita_trad_trend,"codice_cliente","fullouter")
df19 = df18.join(impieghi_mutuo_trend,"codice_cliente","fullouter")
df20 = df19.join(impieghi_prestito_trend,"codice_cliente","fullouter")
df21 = df20.join(impieghi_fido_trend,"codice_cliente","fullouter")
df22 = df21.join(num_linee_prodotto_trend,"codice_cliente","fullouter")
df23 = df22.join(c_mobile_trend,"codice_cliente","fullouter")
df24 = df23.join(c_internet_trend,"codice_cliente","fullouter")
df25 = df24.join(c_operatore_trend,"codice_cliente","fullouter")
df26 = df25.join(c_ivr_nooperatore_trend,"codice_cliente","fullouter")
df27 = df26.join(connessioni_totali_trend,"codice_cliente","fullouter")

# COMMAND ----------

df9.filter(col("codice_cliente") == 15790).show()

# COMMAND ----------


df = df27.drop('patrimonio_totale','patrimonio_gestito','patrimonio_amministrato', 'patr_cc', 'patr_time_deposit', 'patr_obbl_strutt', 'patr_bb2_medbond', 'patr_bb2_pct','patr_fondi', 'patr_fondi_terzi', 'patr_vita', 'patr_medplus', 'patr_ul', 'patr_ul_europension','patr_ul_mypension','patr_ul_tbold','patr_ul_tbnew', 'patr_ul_mylife', 'patr_vita_trad', 'impieghi_mutuo' ,  'impieghi_prestito','impieghi_fido', 'num_linee_prodotto', 'c_mobile', 'c_internet', 'c_operatore','c_ivr_nooperatore','connessioni_totali')

# COMMAND ----------

augusto_df = spark.sql("select * from 201808cleaneddata_parquet where data_rif = '31/08/2018'")

# COMMAND ----------

display(df_lda)

# COMMAND ----------

df_lda.columns

# COMMAND ----------

from pyspark.sql.types import StringType
def create_word(col_name, col_val):
    word = str(col_name) + str(col_val)
    return word
func2_udf = udf(create_word, StringType())
df = df.withColumn('w_amm_tr',func2_udf( lit("patr_amm_trend"),df['patr_amm_trend']))
df = df.withColumn('w_gest_tr',func2_udf( lit("patr_gest_trend"),df['patr_gest_trend']))
df = df.withColumn('w_tot_tr',func2_udf( lit("patr_tot_trend"),df['patr_tot_trend']))
df = df.withColumn('w_cc_tr',func2_udf( lit("patr_cc_trend"),df['patr_cc_trend']))
df = df.withColumn('w_time_dep_tr',func2_udf( lit("patr_time_deposit_trend"),df['patr_time_deposit_trend']))
df = df.withColumn('w_ul_tr',func2_udf( lit("patr_ul_trend"),df['patr_ul_trend']))
df = df.withColumn('w_vita_tr',func2_udf( lit("patr_vita_trend"),df['patr_vita_trend']))
df = df.withColumn('w_obb_str_tr',func2_udf( lit("patr_obbl_strutt_trend"),df['patr_obbl_strutt_trend']))
df = df.withColumn('w_bb2_medbnd_tr',func2_udf( lit("patr_bb2_medbond_trend"),df['patr_bb2_medbond_trend']))
df = df.withColumn('w_bb2_pct_tr',func2_udf( lit("patr_bb2_pct_trend"),df['patr_bb2_pct_trend']))
df = df.withColumn('w_fondi_tr', func2_udf( lit("patr_fondi_trend"),df['patr_fondi_trend']))
df = df.withColumn('w_fondi_terz_tr', func2_udf( lit("patr_fondi_terzi_trend"),df['patr_fondi_terzi_trend']))
df = df.withColumn('w_medplus_tr', func2_udf( lit("patr_medplus_trend"),df['patr_medplus_trend']))
df = df.withColumn('w_ul_eurpen_tr', func2_udf( lit("patr_ul_europension_trend"),df['patr_ul_europension_trend']))
df = df.withColumn('w_ul_mypen_tr', func2_udf( lit("patr_ul_mypension_trend"),df['patr_ul_mypension_trend']))
df = df.withColumn('w_ul_tbold_tr', func2_udf( lit("patr_ul_tbold_trend"),df['patr_ul_tbold_trend'])) 
df = df.withColumn('w_ul_tbnew_tr', func2_udf( lit("patr_ul_tbnew_trend"),df['patr_ul_tbnew_trend'])) 
df = df.withColumn('w_ul_mylife_tr', func2_udf( lit("patr_ul_mylife_trend"),df['patr_ul_mylife_trend'])) 
df = df.withColumn('w_vita_trad_tr', func2_udf( lit("patr_vita_trad_trend"),df['patr_vita_trad_trend']))  
df = df.withColumn('w_mutuo_tr',func2_udf( lit("imp_mutuo_trend"),df['imp_mutuo_trend']))
df = df.withColumn('w_mutuo_pre_tr',func2_udf( lit("impieghi_prestito_trend"),df['impieghi_prestito_trend']))
df = df.withColumn('w_fido_tr',func2_udf( lit("impieghi_fido_trend"),df['impieghi_fido_trend']))            
df = df.withColumn('w_cmobile_tr',func2_udf( lit("c_mobile_trend"),df['c_mobile_trend']))  
df = df.withColumn('w_cinternet_tr', func2_udf( lit("c_internet_trend"),df['c_internet_trend']))  
df = df.withColumn('w_coperatore_tr', func2_udf( lit("c_operatore_trend"),df['c_operatore_trend']))  
df = df.withColumn('w_civr_tr', func2_udf( lit("c_ivr_nooperatore_trend"),df['c_ivr_nooperatore_trend']))  
df = df.withColumn('w_con_ttl_tr',func2_udf( lit("conn_totali_trend"),df['conn_totali_trend']))
df = df.withColumn('w_num_line_prod_tr',func2_udf( lit("num_linee_prod_trend"),df['num_linee_prod_trend']))


# COMMAND ----------

df.show(n=5)

# COMMAND ----------

df_lda = augusto_df.join(df,"codice_cliente","inner")

# COMMAND ----------

df_lda.write.format("parquet").saveAsTable("Topic_modelinputAllFields")

# COMMAND ----------

df_lda.columns

# COMMAND ----------


df_input = df_lda.select('word1', 'word2','word3','word4','word5', 'word6','word7','word8','word9','word10','sesso','classe_eta','classe_anzianita','persona_fis_giur','descrizione_stato_civile', 'area_geografica', 'correntista','classe_professione', 'prima_banca').withColumn("index",monotonically_increasing_id())

df_input.show(n=5)

# COMMAND ----------

to_list = [list(row) for row in df_input.collect()]

# COMMAND ----------

textdata =  sc.parallelize(data)

# COMMAND ----------

parsedData = textdata.map(lambda line: Vectors.dense([float(x) for x in line.strip().split(',')]))

# COMMAND ----------

to_list[1]

# COMMAND ----------

data = to_list[0:100]
from pyspark.sql.types import *
from pyspark.sql.functions import udf
# Create your UDF object (which accepts your python function called "my_udf")
udf_object = udf(my_udf, ArrayType(StringType()))
 
# Apply the UDF to your Dataframe (called "df")
new_df = data.withColumn("new_column", udf_object(struct([df[x] for x in df.columns])))
data2 = corpus_df.cast(ArrayType(StringType()))

# COMMAND ----------

from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.sql import SQLContext, Row
from pyspark.ml.feature import CountVectorizer


from pyspark.sql.types import StringType
from pyspark.sql.functions import *

corpus_df = spark.createDataFrame(data, StringType())

corpus_df = corpus_df.withColumn("index",monotonically_increasing_id())

corpus_df = corpus_df.withColumn("arrayColumn", array("value"))
display(corpus_df)

# COMMAND ----------

from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.sql import SQLContext, Row
from pyspark.ml.feature import CountVectorizer

vectorizer = CountVectorizer(inputcol("arrayColumn").setoutputcol("vectors").fit(corpus_df))

# COMMAND ----------

from pyspark.mllib.clustering import LDA, LDAModel
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.sql import SQLContext, Row
from pyspark.ml.feature import CountVectorizer

#http://spark.apache.org/docs/latest/mllib-clustering.html#latent-dirichlet-allocation-lda
# Load and parse the data
#data = to_list[0:1000]
from pyspark.sql.types import StringType
from pyspark.sql.functions import *

corpus_df = spark.createDataFrame(data, StringType())
corpus_df = corpus_df.withColumn("index",monotonically_increasing_id())
corpus_df = corpus_df.withColumn("arrayColumn", array("value"))
#data = sc.textFile(path).zipWithIndex().map(lambda (words,idd): Row(idd= idd, words = words.split(" ")))
#docDF = spark.createDataFrame(data)
Vector = CountVectorizer(inputCol="arrayColumn", outputCol="vectors")
model = Vector.fit(corpus_df)
result = model.transform(corpus_df)

# Cluster the documents into three topics using LDA
ldaModel = LDA.train(data, k=10)

#ldaModel.save(sc, "target/org/apache/spark/PythonLatentDirichletAllocationExample/LDAModel")
#sameModel = LDAModel\
#    .load(sc, "target/org/apache/spark/PythonLatentDirichletAllocationExample/LDAModel")

# COMMAND ----------

num_topics = 10
max_iterations = 100

lda_model = LDA.train(result[['index','vectors']], k=num_topics, maxIterations=max_iterations)

# COMMAND ----------

result.show(truncate=False)

# COMMAND ----------

#https://stackoverflow.com/questions/51390676/how-to-visualize-pyspark-mls-lda-or-other-clustering
corpus = [dictionary.doc2bow(text) for text in texts]



# COMMAND ----------

data = to_list[0:100]

# COMMAND ----------

data

# COMMAND ----------

# Trains a LDA model.
lda = LDA(k=10, maxIter=10)
model = lda.fit(dataset)

ll = model.logLikelihood(dataset)
lp = model.logPerplexity(dataset)
print("The lower bound on the log likelihood of the entire corpus: " + str(ll))
print("The upper bound on perplexity: " + str(lp))

# Describe topics.
topics = model.describeTopics(3)
print("The topics described by their top-weighted terms:")
topics.show(truncate=False)

# Shows the result
transformed = model.transform(dataset)
transformed.show(truncate=False)

# COMMAND ----------

http://lamastex.org/courses/ScalableDataScience/2016/SparkScalaMLlib_LDA_Topic_Modeling.html
  
  
  bunu dene scala da
  
