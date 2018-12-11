// Databricks notebook source
spark.conf.set("fs.azure.account.key.ktmildatabrick.blob.core.windows.net", "1442O10qWUQpGxkt9uFT5ZR7NN7lureVg4pqLF3LvHM4ONLi/lFwTKyol9nNhCSGwe0+QLf9xfK3EuAuvclNhw==")

val df = spark.read.format("csv")
  .option("inferSchema", "true")
  .option("header", "true")
  .option("delimiter",";")
  .option("mode", "PERMISSIVE")
  .option("dateFormat", "dd-MM-yyyy") // <-- should match 03-10-2016
//  .load("wasbs://2018@ktmildatabrick.blob.core.windows.net/CB_MEDIOLANUM_KANTAR_2018.csv")
  .load("wasbs://20162017@ktmildatabrick.blob.core.windows.net/CB_MEDIOLANUM_KANTAR_2016_2017.csv")
display(df)

// COMMAND ----------

import sqlContext.implicits._ // for `toDF` and $""
import org.apache.spark.sql.functions._ // for `when`
import org.apache.spark.sql.functions.split 
import org.apache.spark.sql.functions.{concat, lit}

// COMMAND ----------

import sqlContext.implicits._ // for `toDF` and $""
import org.apache.spark.sql.functions._ // for `when`
import org.apache.spark.sql.functions.split 
import org.apache.spark.sql.functions.{concat, lit}

val newDf = df.withColumn("_tmp", split($"patrimonio_amministrato", "\\."))
  .withColumn("col1", $"_tmp".getItem(0))
  .withColumn("col2", $"_tmp".getItem(1))
  .withColumn("col3", $"_tmp".getItem(2))
.drop("_tmp")

.withColumn("patrimonio_amministrato", concat($"col1",
                                   when($"col2".isNotNull, $"col2".substr(0, 3)).otherwise(""), 
                                   when($"col3".isNotNull, $"col3".substr(0, 3)).otherwise("")))
.drop("col1")
.drop("col2")
.drop("col3")

//=============
.withColumn("_tmp", split($"patrimonio_gestito", "\\."))
  .withColumn("col1", $"_tmp".getItem(0))
  .withColumn("col2", $"_tmp".getItem(1))
  .withColumn("col3", $"_tmp".getItem(2))
.drop("_tmp")

.withColumn("patrimonio_gestito", concat($"col1",
                                   when($"col2".isNotNull, $"col2".substr(0, 3)).otherwise(""), 
                                   when($"col3".isNotNull, $"col3".substr(0, 3)).otherwise("")))
.drop("col1")
.drop("col2")
.drop("col3")

//==========
.withColumn("_tmp", split($"patrimonio_totale", "\\."))
  .withColumn("col1", $"_tmp".getItem(0))
  .withColumn("col2", $"_tmp".getItem(1))
  .withColumn("col3", $"_tmp".getItem(2))
.drop("_tmp")

.withColumn("patrimonio_totale", concat($"col1",
                                   when($"col2".isNotNull, $"col2".substr(0, 3)).otherwise(""), 
                                   when($"col3".isNotNull, $"col3".substr(0, 3)).otherwise("")))
.drop("col1")
.drop("col2")
.drop("col3")

//=========
.withColumn("_tmp", split($"impieghi_mutuo", "\\."))
  .withColumn("col1", $"_tmp".getItem(0))
  .withColumn("col2", $"_tmp".getItem(1))
  .withColumn("col3", $"_tmp".getItem(2))
.drop("_tmp")

.withColumn("impieghi_mutuo", concat($"col1",
                                   when($"col2".isNotNull, $"col2".substr(0, 3)).otherwise(""), 
                                   when($"col3".isNotNull, $"col3".substr(0, 3)).otherwise("")))
.drop("col1")
.drop("col2")
.drop("col3")

//=========
.withColumn("_tmp", split($"impieghi_prestito", "\\."))
  .withColumn("col1", $"_tmp".getItem(0))
  .withColumn("col2", $"_tmp".getItem(1))
  .withColumn("col3", $"_tmp".getItem(2))
.drop("_tmp")

.withColumn("impieghi_prestito", concat($"col1",
                                   when($"col2".isNotNull, $"col2".substr(0, 3)).otherwise(""), 
                                   when($"col3".isNotNull, $"col3".substr(0, 3)).otherwise("")))
.drop("col1")
.drop("col2")
.drop("col3")


//=========
.withColumn("_tmp", split($"impieghi_fido", "\\."))
  .withColumn("col1", $"_tmp".getItem(0))
  .withColumn("col2", $"_tmp".getItem(1))
  .withColumn("col3", $"_tmp".getItem(2))
.drop("_tmp")

.withColumn("impieghi_fido", concat($"col1",
                                   when($"col2".isNotNull, $"col2".substr(0, 3)).otherwise(""), 
                                   when($"col3".isNotNull, $"col3".substr(0, 3)).otherwise("")))
.drop("col1")
.drop("col2")
.drop("col3")

//=========
.withColumn("_tmp", split($"impieghi_sconfini", "\\."))
  .withColumn("col1", $"_tmp".getItem(0))
  .withColumn("col2", $"_tmp".getItem(1))
  .withColumn("col3", $"_tmp".getItem(2))
.drop("_tmp")

.withColumn("impieghi_sconfini", concat($"col1",
                                   when($"col2".isNotNull, $"col2".substr(0, 3)).otherwise(""), 
                                   when($"col3".isNotNull, $"col3".substr(0, 3)).otherwise("")))
.drop("col1")
.drop("col2")
.drop("col3")

//=========
.withColumn("_tmp", split($"impieghi_totali", "\\."))
  .withColumn("col1", $"_tmp".getItem(0))
  .withColumn("col2", $"_tmp".getItem(1))
  .withColumn("col3", $"_tmp".getItem(2))
.drop("_tmp")

.withColumn("impieghi_totali", concat($"col1",
                                   when($"col2".isNotNull, $"col2".substr(0, 3)).otherwise(""), 
                                   when($"col3".isNotNull, $"col3".substr(0, 3)).otherwise("")))
.drop("col1")
.drop("col2")
.drop("col3")

//=========
.withColumn("_tmp", split($"impieghi_fido_mutuo_prestito", "\\."))
  .withColumn("col1", $"_tmp".getItem(0))
  .withColumn("col2", $"_tmp".getItem(1))
  .withColumn("col3", $"_tmp".getItem(2))
.drop("_tmp")

.withColumn("impieghi_fido_mutuo_prestito", concat($"col1",
                                   when($"col2".isNotNull, $"col2".substr(0, 3)).otherwise(""), 
                                   when($"col3".isNotNull, $"col3".substr(0, 3)).otherwise("")))
.drop("col1")
.drop("col2")
.drop("col3")



display(newDf)

// COMMAND ----------

newDf.write.saveAsTable("20162017CleanedData")

// COMMAND ----------

newDf.createOrReplaceTempView("201808CleanedData_view")

// COMMAND ----------

newDf.write.format("parquet").saveAsTable("20162017CleanedData_parquet")

// COMMAND ----------

listPatr = ["patrimonio_totale","patrimonio_gestito","patrimonio_amministrato","impieghi_mutuo",
"impieghi_prestito",
"impieghi_fido",
"impieghi_sconfini",
"impieghi_totali",
"impieghi_fido_mutuo_prestito"]

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import *
// MAGIC cust_query = spark.sql("select TO_DATE(CAST(UNIX_TIMESTAMP(data_rif, 'dd/MM/yyyy') AS TIMESTAMP)) as new_date, area_geografica,\
// MAGIC                        classe_eta, classe_anzianita, int(patrimonio_amministrato), int(patrimonio_gestito), int(patr_vita), int(impieghi_mutuo), \
// MAGIC                        int(patrimonio_totale), connessioni_totali ,num_linee_prodotto \
// MAGIC                    from 201808cleaneddata_parquet where codice_cliente = '10823' order by new_date ASC")
// MAGIC df = cust_query.select (year("new_date").alias('year'), month("new_date").alias('month'),"new_date","patrimonio_amministrato")
// MAGIC cust_query = cust_query.withColumn('new_date2', regexp_replace('new_date', '-', ''))
// MAGIC display(cust_query)
// MAGIC cust_query.cache()

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC data = cust_query.select(month("new_date").alias('features'),"patrimonio_amministrato".alias('label'))
// MAGIC 
// MAGIC # Import LinearRegression class
// MAGIC from pyspark.ml.regression import LinearRegression
// MAGIC 
// MAGIC # Define LinearRegression algorithm
// MAGIC lr = LinearRegression()
// MAGIC 
// MAGIC # Fit 2 models, using different regularization parameters
// MAGIC modelA = lr.fit(data, {lr.regParam:0.0})
// MAGIC modelB = lr.fit(data, {lr.regParam:100.0})

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.ml.regression import LinearRegression
// MAGIC from pyspark.ml.feature import VectorAssembler
// MAGIC from pyspark.ml.feature import StandardScaler
// MAGIC from pyspark.ml import Pipeline
// MAGIC from pyspark.sql.functions import *
// MAGIC 
// MAGIC from pyspark.ml.regression import LinearRegression
// MAGIC from pyspark.ml.linalg import Vectors, VectorUDT
// MAGIC 
// MAGIC 
// MAGIC df_patamm = cust_query.select (col("new_date2").cast('int').alias('features'),"new_date","patrimonio_amministrato")
// MAGIC features = ["features"]
// MAGIC 
// MAGIC 
// MAGIC spark.udf.register("oneElementVec", lambda d: Vectors.dense([d]), returnType=VectorUDT())
// MAGIC tdata = df_patamm.selectExpr("oneElementVec(features) as features", "patrimonio_amministrato as label")
// MAGIC 
// MAGIC 
// MAGIC # Define LinearRegression algorithm
// MAGIC lr = LinearRegression()
// MAGIC 
// MAGIC # Fit 2 models, using different regularization parameters
// MAGIC modelA = lr.fit(tdata, {lr.regParam:0.0})
// MAGIC modelB = lr.fit(tdata, {lr.regParam:100.0})

// COMMAND ----------

// MAGIC %python
// MAGIC modelC = lr.fit(tdata)
// MAGIC modelC.explainParams()

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC predictionsA = modelA.transform(tdata)
// MAGIC display(predictionsA)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.ml.evaluation import RegressionEvaluator
// MAGIC evaluator = RegressionEvaluator(metricName="rmse")
// MAGIC RMSE = evaluator.evaluate(predictionsA)
// MAGIC print("ModelA: Root Mean Squared Error = " + str(RMSE))
// MAGIC # ModelA: Root Mean Squared Error = 128.602026843
// MAGIC 
// MAGIC predictionsB = modelB.transform(tdata)
// MAGIC RMSE = evaluator.evaluate(predictionsB)
// MAGIC print("ModelB: Root Mean Squared Error = " + str(RMSE))
// MAGIC # ModelB: Root Mean Squared Error = 129.496300193

// COMMAND ----------

// MAGIC %python
// MAGIC modelA.coefficients

// COMMAND ----------

// MAGIC %python
// MAGIC # Import numpy, pandas, and ggplot
// MAGIC import numpy as np
// MAGIC from pandas import *
// MAGIC from ggplot import *
// MAGIC 
// MAGIC # Create Python DataFrame
// MAGIC datefeat = tdata.map(lambda p: (p.features[0])).collect()
// MAGIC price = tdata.map(lambda p: (p.label)).collect()
// MAGIC predA = predictionsA.select("prediction").map(lambda r: r[0]).collect()
// MAGIC predB = predictionsB.select("prediction").map(lambda r: r[0]).collect()
// MAGIC 
// MAGIC # Create a Pandas DataFrame
// MAGIC pydf = DataFrame({'datefeat':datefeat,'price':price,'predA':predA, 'predB':predB})
// MAGIC #Visualizing the Model
// MAGIC # Create scatter plot and two regression models (scaling exponential) using ggplot
// MAGIC p = ggplot(pydf, aes('datefeat','price')) + \
// MAGIC   geom_point(color='blue') + \
// MAGIC   geom_line(pydf, aes('pop','predA'), color='red') + \
// MAGIC   geom_line(pydf, aes('pop','predB'), color='green') + \
// MAGIC   scale_x_log10() + scale_y_log10()
// MAGIC display(p)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.ml import Pipeline
// MAGIC from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
// MAGIC 
// MAGIC stages = [] # stages in our Pipeline

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.ml import Pipeline
// MAGIC from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler
// MAGIC categoricalColumns = ["month"]
// MAGIC stages = [] # stages in our Pipeline
// MAGIC for categoricalCol in categoricalColumns:
// MAGIC     # Category Indexing with StringIndexer
// MAGIC     stringIndexer = StringIndexer(inputCol=categoricalCol, outputCol=categoricalCol + "Index")
// MAGIC     # Use OneHotEncoder to convert categorical variables into binary SparseVectors
// MAGIC     # encoder = OneHotEncoderEstimator(inputCol=categoricalCol + "Index", outputCol=categoricalCol + "classVec")
// MAGIC     encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
// MAGIC     # Add stages.  These are not run here, but will run all at once later on.
// MAGIC     stages += [stringIndexer, encoder]

// COMMAND ----------

// MAGIC %python
// MAGIC # Convert label into label indices using the StringIndexer
// MAGIC label_stringIdx = StringIndexer(inputCol="patrimonio_amministrato", outputCol="label")
// MAGIC stages += [label_stringIdx]

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # Transform all features into a vector using VectorAssembler
// MAGIC numericCols = ["patrimonio_amministrato", "patrimonio_gestito", "patr_vita", "impieghi_mutuo", "connessioni_totali", "patrimonio_totale","num_linee_prodotto"]
// MAGIC assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
// MAGIC assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
// MAGIC stages += [assembler]

// COMMAND ----------

// MAGIC %python
// MAGIC # Create a Pipeline.
// MAGIC pipeline = Pipeline(stages=stages)
// MAGIC # Run the feature transformations.
// MAGIC #  - fit() computes feature statistics as needed.
// MAGIC #  - transform() actually transforms the features.
// MAGIC dataset = cust_query.select("new_date", year("new_date").alias('year'), month("new_date").alias('month'), "patrimonio_amministrato", "patrimonio_gestito", "patr_vita", "impieghi_mutuo", "connessioni_totali", "patrimonio_totale", "num_linee_prodotto")
// MAGIC pipelineModel = pipeline.fit(dataset)
// MAGIC dataset2 = pipelineModel.transform(dataset)
// MAGIC # Keep relevant columns

// COMMAND ----------

// MAGIC %python
// MAGIC print(pipelineModel.extractParamMap())

// COMMAND ----------

// MAGIC %python
// MAGIC dataset = cust_query.select("new_date", year("new_date").alias('year'), month("new_date").alias('month'), "patrimonio_amministrato", "patrimonio_gestito", "patr_vita", "impieghi_mutuo", "connessioni_totali", "patrimonio_totale", "num_linee_prodotto")
// MAGIC display(dataset)

// COMMAND ----------

// MAGIC %python 
// MAGIC ### Randomly split data into training and test sets. set seed for reproducibility
// MAGIC (trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed=100)
// MAGIC print(trainingData.count())
// MAGIC print(testData.count())

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.ml.evaluation import RegressionEvaluator
// MAGIC eval = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
// MAGIC 
// MAGIC # Root Mean Square Error
// MAGIC rmse = eval.evaluate(prediction)
// MAGIC print("RMSE: %.3f" % rmse)
// MAGIC 
// MAGIC # Mean Square Error
// MAGIC mse = eval.evaluate(prediction, {eval.metricName: "mse"})
// MAGIC print("MSE: %.3f" % mse)
// MAGIC 
// MAGIC # Mean Absolute Error
// MAGIC mae = eval.evaluate(prediction, {eval.metricName: "mae"})
// MAGIC print("MAE: %.3f" % mae)
// MAGIC 
// MAGIC # r2 - coefficient of determination
// MAGIC r2 = eval.evaluate(prediction, {eval.metricName: "r2"})
// MAGIC print("r2: %.3f" %r2)

// COMMAND ----------


