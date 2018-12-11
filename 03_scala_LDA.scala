// Databricks notebook source
//val data = spark.sql("select codice_cliente, word1, word2,word3,word4,word5, word6,word7,word8,word9,word10,classe_patrimonio, sesso,classe_eta,classe_anzianita,persona_fis_giur,descrizione_stato_civile, area_geografica, correntista,classe_professione, prima_banca from Topic_model_categorized ")
//"select int(codice_cliente), persona_fis_giur, 
//descrizione_stato_civile, flag_email, int(eta), classe_eta, classe_anzianita, sesso, area_geografica, 
 //                       classe_professione, classe_tit_studio, classe_patrimonio_estesa, prima_banca, num_riass_36mesi, flag_dossier, flag_gestioni,
 //                       int(patrimonio_totale), int(patrimonio_gestito), int(patrimonio_amministrato), int(patr_cc), 
 //                       int(patr_time_deposit), int(patr_obbl_strutt), int(patr_bb2_medbond), int(patr_bb2_pct), int(patr_fondi), 
 //                       int(patr_fondi_terzi), int(patr_vita), int(patr_medplus), int(patr_ul),
 //                       int(patr_ul_europension), int(patr_ul_mypension), int(patr_ul_tbold), int(patr_ul_tbnew), int(patr_ul_mylife), 
 //                       int(patr_vita_trad), int(impieghi_mutuo),  int(impieghi_prestito),int(impieghi_fido), 
 //                       connessioni_totali, num_linee_prodotto, c_mobile, c_internet, c_operatore, c_ivr_nooperatore from Topic_modelinputAllFields")
// patr_amm_trend, patr_gest_trend, patr_tot_trend, patr_cc_trend, patr_time_deposit_trend, patr_ul_trend, patr_obbl_strutt_trend, patr_bb2_medbond_trend, patr_bb2_pct_trend, patr_fondi_trend, patr_fondi_terzi_trend, patr_vita_trend, patr_medplus_trend, patr_ul_europension_trend, patr_ul_mypension_trend, patr_ul_tbold_trend, patr_ul_tbnew_trend, patr_ul_mylife_trend, patr_vita_trad_trend, imp_mutuo_trend, impieghi_prestito_trend, impieghi_fido_trend, num_linee_prod_trend, c_mobile_trend,  c_internet_trend, c_operatore_trend,  c_ivr_nooperatore_trend, conn_totali_trend, 

 val data = spark.sql("select codice_cliente, persona_fis_giur, descrizione_stato_civile, flag_email,  classe_eta, classe_anzianita, sesso, area_geografica, classe_patrimonio_estesa, correntista, flag_dossier, flag_gestioni, flag_carta_credito, flag_carta_debito, num_riass_36mesi,  classe_professione, classe_tit_studio, prima_banca, w_amm_tr, w_gest_tr, w_tot_tr, w_cc_tr, w_time_dep_tr, w_ul_tr, w_vita_tr, w_obb_str_tr, w_bb2_medbnd_tr, w_bb2_pct_tr, w_fondi_tr, w_fondi_terz_tr, w_medplus_tr, w_ul_eurpen_tr, w_ul_mypen_tr, w_ul_tbold_tr, w_ul_tbnew_tr, w_ul_mylife_tr, w_vita_trad_tr, w_mutuo_tr, w_mutuo_pre_tr, w_fido_tr, w_cmobile_tr, w_cinternet_tr, w_coperatore_tr, w_civr_tr, w_con_ttl_tr, w_num_line_prod_tr from Topic_modelinputAllFields where persona_fis_giur = 'F'")

// COMMAND ----------

data.count

// COMMAND ----------

import org.apache.spark.sql.functions._ 
val dataFrame1 = data.withColumn("index",monotonically_increasing_id())
.withColumn("xflag_email", concat($"flag_email",lit("_email")))
.withColumn("xflag_carta_credito", concat($"flag_carta_credito",lit("_cartacredit")))
.withColumn("xflag_carta_debito", concat($"flag_carta_debito",lit("_cartadebit")))
.withColumn("xnum_riass_36mesi", concat($"num_riass_36mesi",lit("_riass_36mesi")))
.withColumn("xflag_dossier", concat($"flag_dossier", lit("_flg_dossier")))
.withColumn("xflag_gestioni", concat($"flag_gestioni", lit("_flg_gestioni")))
.withColumn("xclasse_patrimonio_estesa", concat($"classe_patrimonio_estesa".substr(0,2),lit("_classe_patrimonio_estesa")))
.withColumn("xsesso", concat($"sesso",lit("_gender")))
.withColumn("xclasse_eta", concat($"classe_eta".substr(0,2),lit("_eta")))
.withColumn("xclasse_anzianita", concat($"classe_anzianita".substr(0,2),lit("_anzianita")))
.withColumn("xcivile", concat($"descrizione_stato_civile".substr(0,3),lit("_civile")))
.withColumn("xarea_geografica", concat($"area_geografica".substr(0,2),lit("_area")))
.withColumn("xcorrentista", concat($"correntista".substr(0,2),lit("_correntista")))
.withColumn("xclasse_professione", concat($"classe_professione".substr(0,3),lit("_professione")))
.withColumn("xclasse_tit_studio", concat($"classe_tit_studio".substr(0,2),lit("_studio")))
.withColumn("xprima_banca", concat($"prima_banca".substr(0,3),lit("_prima_banca")))
.withColumn("xabbandono_12m", concat($"classi_probab_abbandono_12m".substr(0,2),lit(_"abbondano")))
.withColumn("xprotezione",concat($"flag_protezione",lit("_protezione")))
.withColumn("xclasse_impieghi_mutuo",concat($"classe_impieghi_mutuo",substr(0,1),lit("_cls_imp_mutuo")))
.withColumn("xclasse_impieghi_prestito",concat($"classe_impieghi_prestito",substr(0,1),lit("_cls_imp_pres")))
.withColumn("xclasse_impieghi_fido",concat($"classe_impieghi_fido",substr(0,1),lit("_cls_imp_fido")))
.withColumn("xclasse_impiego_fido_mutuo_prest",concat($"classe_impiego_fido_mutuo_prest",substr(0,1),lit("_cls_impiego_all")))
.drop("flag_email","classe_patrimonio_estesa", "num_riass_36mesi","flag_dossier", "flag_gestioni", "flag_carta_credito", "flag_carta_debito", "sesso","classe_eta","classe_anzianita","descrizione_stato_civile","area_geografica","correntista","classe_professione","classe_tit_studio","prima_banca",
"classi_probab_abbandono_12m","flag_protezione","classe_impieghi_mutuo","classe_impieghi_prestito", "classe_impieghi_fido", "classe_impiego_fido_mutuo_prest")
display(dataFrame1.select("*"))

// COMMAND ----------



// COMMAND ----------

columns_merged = dataFrame1.columns

// COMMAND ----------

//val corpus_df = dataFrame1.select($"index",concat_ws(" ", $"word1", $"word2", $"word3", $"word4",  $"word5",  $"word6",  $"word7",  $"word8",  $"word9",  $"word10", $"xclasse_patrimonio", $"xsesso", $"xclasse_eta", $"xclasse_anzianita", $"persona_fis_giur",  $"xcivile",  $"xarea_geografica", $"xcorrentista", $"xclasse_professione",  $"xprima_banca").alias("corpus"))

val corpus_df = dataFrame1.select($"codice_cliente",concat_ws(" ", $"persona_fis_giur", $"w_amm_tr", $"w_gest_tr", $"w_tot_tr", $"w_cc_tr",  $"w_time_dep_tr",  $"w_ul_tr",  $"w_vita_tr",  $"w_obb_str_tr",  $"w_bb2_medbnd_tr",  $"w_bb2_pct_tr",  $"w_fondi_tr",  $"w_fondi_terz_tr", $"w_medplus_tr",  $"w_ul_eurpen_tr",  $"w_ul_mypen_tr",  $"w_ul_tbold_tr",  $"w_ul_tbnew_tr",  $"w_ul_mylife_tr",  $"w_vita_trad_tr",  $"w_mutuo_tr",  $"w_mutuo_pre_tr",  $"w_fido_tr",  $"w_cmobile_tr",  $"w_cinternet_tr",  $"w_coperatore_tr",  $"w_civr_tr",  $"w_con_ttl_tr",  $"w_num_line_prod_tr",  $"xflag_email",  $"xflag_carta_credito",  $"xflag_carta_debito",  $"xnum_riass_36mesi",  $"xflag_dossier",  $"xflag_gestioni",  $"xclasse_patrimonio_estesa",  $"xsesso",  $"xclasse_eta",  $"xclasse_anzianita",  $"xcivile",  $"xarea_geografica",  $"xcorrentista",  $"xclasse_professione",  $"xclasse_tit_studio",  $"xprima_banca", $"xabbandono_12m", $"xprotezione", $"xclasse_impieghi_mutuo", $"xclasse_impieghi_prestito", $"xclasse_impieghi_fido", $"xclasse_impiego_fido_mutuo_prest").alias("corpus"))

// COMMAND ----------

display(corpus_df)

// COMMAND ----------

corpus_df.na.drop()

// COMMAND ----------

import org.apache.spark.ml.feature.RegexTokenizer

val tokenizer = new RegexTokenizer()
 // .setPattern("[\\W]+")
  .setInputCol("corpus")
  .setOutputCol("tokens")

// Tokenize document
val tokenized_df = tokenizer.transform(corpus_df.na.fill(Map("corpus" -> "")))


// COMMAND ----------

tokenized_df.columns

// COMMAND ----------

display(tokenized_df.select("tokens"))

// COMMAND ----------

display(tokenized_df)

// COMMAND ----------

tokenized_df.write.format("parquet").saveAsTable("tokenized")

// COMMAND ----------

import org.apache.spark.ml.feature.CountVectorizer

// Set params for CountVectorizer
val vectorizer = new CountVectorizer()
.setInputCol("tokens")
.setOutputCol("features")
.fit(tokenized_df)

// COMMAND ----------

// Create vector of token counts
val countVectors = vectorizer.transform(tokenized_df).select("codice_cliente","features")

// COMMAND ----------

countVectors.write.format("parquet").saveAsTable("countVectorsDF")

// COMMAND ----------

display(countVectors)

// COMMAND ----------

countVectors.take(1)

// COMMAND ----------

import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row
// first generate RDD of documents...

val numTopics = 10
val lda = new LDA().setK(numTopics).setMaxIterations(10).setOptimizer("em")

val ldaDF = countVectors.rdd.map { 
  case Row(codice_cliente: Long, features: Vector) => (codice_cliente, features) 
} 


// COMMAND ----------

ldaDF

// COMMAND ----------

val ldaModel = lda.run(ldaDF)

// COMMAND ----------

val distLDAModel = ldaModel.asInstanceOf[DistributedLDAModel]

// COMMAND ----------

import org.apache.spark.ml.clustering.LDA

val lda = new LDA().setK(10).setMaxIter(10)
val model = lda.fit(countVectors)

// COMMAND ----------

val transformed = model.transform(countVectors)

// COMMAND ----------

transformed.show()

// COMMAND ----------

val pd = transformed.toDF

// COMMAND ----------

display(pd)

// COMMAND ----------

import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vector
val func = udf( (x:Vector) => x.toDense.values.toSeq.indices.maxBy(x.toDense.values) )

val pd_cluster = pd.withColumn("max_idx",func($"topicDistribution")).drop($"features").toDF

// COMMAND ----------

val pd_csv = pd_cluster.drop($"topicDistribution")
display(pd_csv)

// COMMAND ----------

val df = dataFrame1.join(pd_csv, "codice_cliente")

// COMMAND ----------

df.write.format("parquet").saveAsTable("LDAclienttopicresults")

// COMMAND ----------

spark.conf.set("fs.azure.account.key.ktmildatabrick.blob.core.windows.net", "1442O10qWUQpGxkt9uFT5ZR7NN7lureVg4pqLF3LvHM4ONLi/lFwTKyol9nNhCSGwe0+QLf9xfK3EuAuvclNhw==")
df.write.format("parquet").saveAsTable("LDAclienttopicresults")


// COMMAND ----------

val zipUDF = udf { (terms: Seq[String], probabilities: Seq[Double]) => terms.zip(probabilities) }
val topicsTmp = topics.withColumn("termWithProb", explode(zipUDF(col("terms"), col("termWeights"))))
val termDF = topicsTmp.select(
  col("topic").as("topicId"),
  col("termWithProb._1").as("term"),
  col("termWithProb._2").as("probability"))

display(termDF)

// COMMAND ----------

display(pd_cluster)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.ml._
// A UDF to convert VectorUDT to ArrayType
val vecToArray = udf( (xs: linalg.Vector) => xs.toArray )

// Add a ArrayType Column   
val dfArr = a.withColumn("topics" , vecToArray($"topicDistribution") )

// Array of element names that need to be fetched
// ArrayIndexOutOfBounds is not checked.
// sizeof `elements` should be equal to the number of entries in column `features`
val elements = Array("Topic1", "Topic2", "Topic3", "Topic4", "Topic5", "Topic6", "Topic7", "Topic8", "Topic9", "Topic10", "Topic11", "Topic12", "Topic13", "Topic14", "Topic15", "Topic16", "Topic16", "Topic17", "Topic18", "Topic19", "Topic20")

// Create a SQL-like expression using the array 
val sqlExpr = elements.zipWithIndex.map{ case (alias, idx) => col("topics").getItem(idx).as(alias) }

// Extract Elements from dfArr    
val df_topicDist = dfArr.select(sqlExpr : _*).toDF
val df_client = pd.select($"codice_cliente")
df_client = df_client.withColumn("cluster", Row())


// COMMAND ----------


import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vector

// Get size of the vector
val n = 20

// Simple helper to convert vector to array<double> 
val vecToSeq = udf((v: Vector) => v.toArray)

// Prepare a list of columns to create
val exprs = (0 until n).map(i => $"_tmp".getItem(i).alias(s"f$i"))

val b = a.select(vecToSeq($"topicDistribution").alias("_tmp")).select(exprs:_*).toDF



// COMMAND ----------

display(b)

// COMMAND ----------

model.write.overwrite().save("/lda_results_model_20topics")

// COMMAND ----------

val ll = model.logLikelihood(countVectors)
val lp = model.logPerplexity(countVectors)
println(s"The lower bound on the log likelihood of the entire corpus: $ll")
println(s"The upper bound on perplexity: $lp")

// COMMAND ----------

// Get vocab
val vocabList = vectorizer.vocabulary
val termsIdx2Str = udf { (termIndices: Seq[Int]) => termIndices.map(idx => vocabList(idx)) }

// Review Results of LDA model with Online Variational Bayes
val topics = model.describeTopics(maxTermsPerTopic = 81)
  .withColumn("terms", termsIdx2Str(col("termIndices")))
display(topics.select("topic", "terms", "termWeights"))

// COMMAND ----------

val zipUDF = udf { (terms: Seq[String], probabilities: Seq[Double]) => terms.zip(probabilities) }
val topicsTmp = topics.withColumn("termWithProb", explode(zipUDF(col("terms"), col("termWeights"))))
val termDF = topicsTmp.select(
  col("topic").as("topicId"),
  col("termWithProb._1").as("term"),
  col("termWithProb._2").as("probability"))

display(termDF)

// COMMAND ----------

termDF.write.format("parquet").saveAsTable("term_df_topic_distr")

// COMMAND ----------

import org.apache.spark.ml.feature._

val numTopics = 20

// COMMAND ----------

val tokenizer = new Tokenizer()
  .setInputCol("corpus")
  .setOutputCol("tokens")

// COMMAND ----------

val vectorizer = new CountVectorizer()
  .setInputCol("tokens")
  .setOutputCol("features")

// COMMAND ----------

import org.apache.spark.ml.clustering.LDA

val lda = new LDA()
  .setK(numTopics)
  .setMaxIter(50)
  .setOptimizer("em")
  .setTopicDistributionCol("topicDistribution")

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline().setStages(Array(tokenizer, vectorizer, lda))

// COMMAND ----------

val pipelineModel = pipeline.fit(corpus_df)

// COMMAND ----------

pipelineModel.stages

// COMMAND ----------

val vectorizerModel = pipelineModel.stages(1).asInstanceOf[CountVectorizerModel]

// COMMAND ----------

import org.apache.spark.ml.clustering.DistributedLDAModel

// Since we trained with the default optimizer (EM), we get back a DistributedLDAModel
val ldaModel = pipelineModel.stages(2).asInstanceOf[DistributedLDAModel]

// COMMAND ----------

ldaModel.trainingLogLikelihood

// COMMAND ----------

display(ldaModel.describeTopics(maxTermsPerTopic = 5))

// COMMAND ----------

// Get vocab
val vocabList = vectorizerModel.vocabulary
val termsIdx2Str = udf { (termIndices: Seq[Int]) => termIndices.map(idx => vocabList(idx)) }

// Review Results of LDA model with Online Variational Bayes
val topics = ldaModel.describeTopics(maxTermsPerTopic = 5)
  .withColumn("terms", termsIdx2Str(col("termIndices")))
display(topics.select("topic", "terms", "termWeights"))

// COMMAND ----------

// Create DF with proper column names
//val termDF = termRDD2.toDF.withColumnRenamed("_1", "term").withColumnRenamed("_2", "probability").withColumnRenamed("_3", "topicId")
val zipUDF = udf { (terms: Seq[String], probabilities: Seq[Double]) => terms.zip(probabilities) }
val topicsTmp = topics.withColumn("termWithProb", explode(zipUDF(col("terms"), col("termWeights"))))
val termDF = topicsTmp.select(
  col("topic").as("topicId"),
  col("termWithProb._1").as("term"),
  col("termWithProb._2").as("probability"))

// COMMAND ----------

display(termDF)

// COMMAND ----------

countVectors.count

// COMMAND ----------

import org.apache.spark.ml.clustering.KMeans
//import org.apache.spark.ml.evaluation.ClusteringEvaluator

// Loads data.
val dataset = countVectors


// COMMAND ----------


// Trains a k-means model.
val kmeans = new KMeans().setK(10).setSeed(1L)
val model = kmeans.fit(dataset)


// COMMAND ----------

//import org.apache.spark.ml.evaluation.ClusteringEvaluator
// Make predictions
val predictions = model.transform(dataset)

// Evaluate clustering by computing Silhouette score
//val evaluator = new ClusteringEvaluator()

//val silhouette = evaluator.evaluate(predictions)
//println(s"Silhouette with squared euclidean distance = $silhouette")

// Shows the result.
println("Cluster Centers: ")
model.clusterCenters.foreach(println)

// COMMAND ----------

display(predictions)

// COMMAND ----------

import org.apache.spark.sql.functions 
val df = predictions.toDF()

// COMMAND ----------

display(df)

// COMMAND ----------

val df3 = dataFrame1.join(df, "codice_cliente")
val df2 = df3.drop("features")
spark.conf.set("fs.azure.account.key.ktmildatabrick.blob.core.windows.net", "1442O10qWUQpGxkt9uFT5ZR7NN7lureVg4pqLF3LvHM4ONLi/lFwTKyol9nNhCSGwe0+QLf9xfK3EuAuvclNhw==")
df3.write.format("parquet").saveAsTable("Kmeansclienttopicresults")


// COMMAND ----------


val df2 = df.drop("features")
spark.conf.set(
  "fs.azure.account.key.ktmildatabrick.blob.core.windows.net",
  "1442O10qWUQpGxkt9uFT5ZR7NN7lureVg4pqLF3LvHM4ONLi/lFwTKyol9nNhCSGwe0+QLf9xfK3EuAuvclNhw==")

df2.write.csv("wasbs://kmeans@ktmildatabrick.blob.core.windows.net/kmeans_results")

// COMMAND ----------

df2.count

// COMMAND ----------

val vocabList = vectorizer.vocabulary

// COMMAND ----------

val vocaDF = vocabList.toList

// COMMAND ----------

display(df)

// COMMAND ----------

display(countVectors)

// COMMAND ----------

countVectors.count

// COMMAND ----------

display(df.filter($"prediction"===8))

// COMMAND ----------

display(df)

// COMMAND ----------

display()

// COMMAND ----------


