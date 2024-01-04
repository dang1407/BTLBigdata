import findspark
findspark.init()
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, map_keys, map_values, expr,  monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.ml.feature import CountVectorizer, StringIndexer,Tokenizer, RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LinearSVC, LogisticRegression
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import lower, regexp_replace
from pyspark.ml.feature import Tokenizer, StopWordsRemover, RegexTokenizer, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
# Tạo UDF cho PySpark
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Business Analys") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12-7.17.16,commons-httpclient:commons-httpclient:3.1") \
    .config("spark.jars", "./elasticsearch-spark-30_2.12-7.12.0.jar,./commons-httpclient-3.1.jar") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.resource", "test/_doc") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

# business_file_path = "hdfs://namenode:9000/user/input/yelp_academic_dataset_business.json"
# review_file_path = "hdfs://namenode:9000/user/input/yelp_academic_dataset_review.json"
# positive_file_path = "hdfs://namenode:9000/user/input/positive.txt"
# negative_file_path = "hdfs://namenode:9000/user/input/negative.txt"
# # Doc cac file
# review = spark.read.json(review_file_path)


# Khởi tạo SparkSession
# spark = SparkSession.builder.appName("RestaurantReviewsSentimentAnalysis").getOrCreate()

# Đọc dữ liệu
# Giả sử 'reviews_restaurant' là DataFrame có sẵn với cột 'text' và 'star'
# Ví dụ: spark.read.csv("path_to_file.csv", header=True, inferSchema=True)


# Định nghĩa một cột nhãn mới dựa trên số sao
# def label(star):
#     if star >= 3:
#         return 1  # tích cực
#     else:
#         return 0  # tiêu cực

# def clean_text(c):
#     return regexp_replace(c, "[^a-zA-Z0-9\s]", "")

# label_udf = udf(label, IntegerType())
# review_clear = review.withColumn("cleaned_text", clean_text(col("text")))
# # Áp dụng UDF để tạo cột nhãn mới
# reviews_restaurant = review_clear.withColumn("label", label_udf("stars"))
# top10_review = review_clear.limit(10)
# Chia tập dữ liệu thành tập huấn luyện và kiểm thử
# (trainingData, testData) = reviews_restaurant.randomSplit([0.5, 0.5], seed=1234)

# # Xây dựng pipeline cho việc xử lý và phân loại dữ liệu
# tokenizer = Tokenizer(inputCol="text", outputCol="words")
# remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="filtered")
# hashingTF = HashingTF(inputCol=remover.getOutputCol(), outputCol="rawFeatures")
# idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="features")
# nb = NaiveBayes(modelType="multinomial", labelCol="label", featuresCol="features")
# pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, idf, nb])

# # Huấn luyện mô hình với tập dữ liệu huấn luyện
# model = pipeline.fit(trainingData)

# # Dự đoán và đánh giá mô hình trên tập kiểm thử
# predictions = model.transform(testData)
# evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
# accuracy = evaluator.evaluate(predictions)
# print("Accuracy: ", accuracy)
# Accuracy:  0.8771002374274742
model_path = "hdfs://namenode:9000/user/input/sentiment_model"
model = PipelineModel.load(model_path)

# Sample reviews for testing
reviews_data = [("I love this product!",),
                ("This is not what I expected.",),
                ("Great service and fast delivery.",),
                ("Very disappointing experience.",),
                ("The quality of the product is amazing.",)]

# Define the schema for the DataFrame
schema = StructType([StructField("text", StringType(), True)])

# Create a DataFrame with the sample reviews
df = spark.createDataFrame(reviews_data, schema=schema)
predicted = model.transform(df)
predicted.select("text", "prediction").show()
# Lưu mô hình vào HDFS
# model.write().overwrite().save(model_path)
spark.stop()

# +--------------------+----------+
# |                text|prediction|
# +--------------------+----------+
# |I love this product!|       1.0|
# |This is not what ...|       1.0|
# |Great service and...|       1.0|
# |Very disappointin...|       0.0|
# |The quality of th...|       1.0|
# +--------------------+----------+