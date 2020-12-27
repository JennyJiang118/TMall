from pyspark.sql import SparkSession
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression,RandomForestClassifier
from pyspark.ml.feature import VectorAssembler,StringIndexer, VectorIndexer,IndexToString,StandardScaler
from pyspark.sql.functions import udf,when
from pyspark.sql.types import *


#---------------load df_train-----------------------#

sc = SparkSession.builder.getOrCreate()

train =sc.read.options(header="True", inferSchema="True").csv("out/train.csv")
test =sc.read.options(header="True", inferSchema="True").csv("out/test.csv")




#---------------feature extraction-----------------------#

# feature assemble
feature_colums_train = train.columns[3:]
feature_colums_test = test.columns[3:]

train = VectorAssembler(inputCols=feature_colums_train, outputCol="features").transform(train)
test = VectorAssembler(inputCols=feature_colums_test, outputCol="features").transform(test)

# indexer
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(train)

featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=5).fit(train)
featureIndexer_test = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=5).fit(test)


# indexedLabel -> label
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer.labels)


# lr standard scale
standardscaler = StandardScaler().setInputCol("features").setOutputCol("Scaled_features")
train = standardscaler.fit(train).transform(train)
test = standardscaler.fit(test).transform(test)


# modify unbalance
train_size = train.select("label").count()
negative_num = train.select("label").where("label==0").count()
balance_ratio =float(float(negative_num)/float(train_size))
train = train.withColumn("classWeights",when(train.label==1, balance_ratio).otherwise(1-balance_ratio))



#-------------------train-----------------------#
#build models
md = LogisticRegression(labelCol="indexedLabel", featuresCol="indexedFeatures", weightCol="classWeights", maxIter=50, regParam=0.02)
# md = LogisticRegression(labelCol="indexedLabel", featuresCol="indexedFeatures", maxIter=10, regParam=0.05)

# md = RandomForestClassifier(labelCol="indexedLabel",featuresCol="indexedFeatures", weightCol="classWeights",numTrees=200)



# build model
pipline = Pipeline(stages=[labelIndexer, featureIndexer, md, labelConverter])
model = pipline.fit(train)
predictions = model.transform(test)


# modify predictions
md_prob = udf(lambda x:float(x[1]),FloatType())
predictions = predictions.withColumn("prob", md_prob("probability"))

result = predictions.select("user_id","merchant_id","prob")

pos = result.select("prob").where("prob>=0.5").count()
neg = result.select("prob").where("prob<0.5").count()

print("----------------------------------")
print(pos)
print(neg)


result.toPandas().to_csv("out/result.csv", index=False)






