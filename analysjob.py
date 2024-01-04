import findspark
findspark.init()
import pandas as pd
from pyspark.sql.types import IntegerType, StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, desc, monotonically_increasing_id, lower, regexp_replace, explode, udf, split,avg, length
from pyspark.sql import functions as F
from pyspark.ml.feature import CountVectorizer, Tokenizer, StopWordsRemover, CountVectorizer, StringIndexer, RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LinearSVC, LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("ElasticsearchIndexCreation") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12-7.17.16.jar,commons-httpclient:commons-httpclient:3.1") \
    .config("spark.jars", "./elasticsearch-spark-30_2.12-7.12.0.jar,./commons-httpclient-3.1.jar") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.resource", "test/_doc") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()


ES_NODES = "elasticsearch"
ES_RESOURCE = "datatest/_doc"

# Đường dẫn đến file JSON
file_path = "hdfs://namenode:9000/user/input/yelp_academic_dataset_business.json"

business = spark.read.json(file_path)

# Sử dụng Spark để đọc file JSON và chuyển thành DataFrame

# Tạo danh sách các tiểu bang cần lọc
states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

# Chọn ra 16 loại ẩm thực và thêm vào cột category
categories = ['American', 'Mexican', 'Italian', 'Japanese', 'Chinese', 'Thai', 'Mediterranean',
              'French', 'Vietnamese', 'Greek', 'Indian', 'Korean', 'Hawaiian', 'African',
              'Spanish', 'Middle_eastern']

# Lọc dữ liệu theo tiểu bang
usa = business.filter(col('state').isin(states))

# Chọn tất cả các nhà hàng ở Mỹ
us_restaurants_df = usa.filter(col('categories').isNotNull() & col('categories').contains('Restaurants'))

# Tạo cột "category" và thêm giá trị dựa trên điều kiện
us_restaurants_df = (us_restaurants_df
                    .withColumn('category',
                                F.when(col('categories').contains('American'), 'American')
                                .when(col('categories').contains('Mexican'), 'Mexican')
                                .when(col('categories').contains('Italian'), 'Italian')
                                .when(col('categories').contains('Japanese'), 'Japanese')
                                .when(col('categories').contains('Chinese'), 'Chinese')
                                .when(col('categories').contains('Thai'), 'Thai')
                                .when(col('categories').contains('Mediterranean'), 'Mediterranean')
                                .when(col('categories').contains('French'), 'French')
                                .when(col('categories').contains('Vietnamese'), 'Vietnamese')
                                .when(col('categories').contains('Greek'), 'Greek')
                                .when(col('categories').contains('Indian'), 'Indian')
                                .when(col('categories').contains('Korean'), 'Korean')
                                .when(col('categories').contains('Hawaiian'), 'Hawaiian')
                                .when(col('categories').contains('African'), 'African')
                                .when(col('categories').contains('Spanish'), 'Spanish')
                                .when(col('categories').contains('Middle_eastern'), 'Middle_eastern')
                                .otherwise(None)))

# Loại bỏ giá trị null ở cột "category"
us_restaurants_df = us_restaurants_df.filter(col('category').isNotNull())
# Reset lại index
us_restaurants_df = us_restaurants_df.withColumn('index', monotonically_increasing_id())

us_restaurants_df.drop('categories')
# us_restaurants_df=us_restaurants_df.reset_index(drop=True)


# Đường dẫn đến file JSON của review
file_path_review = "hdfs://namenode:9000/user/input/yelp_academic_dataset_review.json"

# Đọc dữ liệu review
review = spark.read.json(file_path_review)




# Kết hợp bảng business và bảng review
us_restaurants_df = us_restaurants_df.withColumnRenamed('stars', 'avg_star')
review = review.withColumnRenamed('stars', 'review_star')
restaurants_reviews = us_restaurants_df.join(review, 'business_id')

restaurants_reviews.show()
categories_to_filter = ["American","Middle_eastern","Spanish","African","Hawaiian","Korean","Indian","Greek","Vietnamese","French","Mediterranean","Thai","Chinese","Japanese","Italian","Mexican"]
# Lọc các hàng có giá trị trong danh sách categories_to_filter trong cột "category"
filtered_reviews = restaurants_reviews.filter(col("category").isin(categories_to_filter))

# Xóa khoảng trắng và kí tự đặc biệt khỏi cột "text"
filtered_reviews = filtered_reviews.withColumn("cleaned_text", regexp_replace(col("text"), "[^a-zA-Z0-9\s]", ""))

# Tính trung bình độ dài của văn bản theo từng nhóm "review_star" và "category"
average_text_length_by_star_and_category = filtered_reviews.groupBy("review_star", "category").agg(avg(length("cleaned_text")).alias("average_text_length"))

# Hiển thị kết quả
average_text_length_by_star_and_category.show()

# # Chỗ save("") với chỗ option es.resource phải giống nhau, chỗ id thì là một trường trong DF
average_text_length_by_star_and_category.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.resource", "text_length/_doc") \
    .option("es.mapping.id", "review_star") \
    .option("es.write.operation", "upsert") \
    .option("es.index.auto.create", "true") \
    .option("es.nodes.wan.only", "true") \
    .mode("append") \
    .save("text_length/_doc")
# Đọc danh sách các từ tích cực và tiêu cực từ HDFS
positive_words = set(spark.read.text("hdfs://namenode:9000/user/input/positive.txt").rdd.flatMap(lambda x: x).collect())
negative_words = set(spark.read.text("hdfs://namenode:9000/user/input/negative.txt").rdd.flatMap(lambda x: x).collect())

# Đọc dữ liệu từ bảng restaurants_reviews
restaurants_reviews = restaurants_reviews.withColumn("text", lower(col("text")))
restaurants_reviews = restaurants_reviews.withColumn("removed_punct_text", 
                                                     regexp_replace(col("text"), "[^a-zA-Z0-9\s]", ""))
# Loại bỏ các cụm từ bắt đầu bằng "not" và theo sau là một từ
restaurants_reviews = restaurants_reviews.withColumn("text_without_not_phrases", 
                                                     regexp_replace(col("removed_punct_text"), "not \\w+", ""))
# Chia từ trong trường removed_punct_text và tạo ra một dòng mới cho mỗi từ
words_df = restaurants_reviews.withColumn("word", explode(split(col("text_without_not_phrases"), " ")))

# Lọc reviews tích cực và tiêu cực
# Lọc reviews tích cực và tiêu cực
positive_reviews = words_df.filter(col("review_star") > 3)
negative_reviews = words_df.filter(col("review_star") < 3)
# Đếm số lần xuất hiện của từng từ trong danh sách từ tích cực và tiêu cực
positive_word_counts = positive_reviews.filter(col("word").isin(positive_words)).groupBy("word").count()
negative_word_counts = negative_reviews.filter(col("word").isin(negative_words)).groupBy("word").count()

# Danh sách các từ cần xóa khỏi positive và negative
positive_words_to_remove = ['great', 'amazing', 'love', 'best', 'awesome', 'excellent', 'good',
                            'favorite', 'loved', 'perfect', 'gem', 'perfectly', 'wonderful',
                            'happy', 'enjoyed', 'nice', 'well', 'super', 'like', 'better', 'decent', 'fine',
                            'pretty', 'enough', 'excited', 'impressed', 'ready', 'fantastic', 'glad', 'right',
                            'fabulous']

negative_words_to_remove = ['bad', 'disappointed', 'unfortunately', 'disappointing', 'horrible',
                            'lacking', 'terrible', 'sorry', 'disappoint']

# Lọc bỏ các từ trong danh sách positive_words_to_remove khỏi positive_word_counts
most_common_positive_word = positive_word_counts.filter(~col("word").isin(positive_words_to_remove)) \
    .orderBy(col("count").desc()).limit(20)

# Lọc bỏ các từ trong danh sách negative_words_to_remove khỏi negative_word_counts
most_common_negative_word = negative_word_counts.filter(~col("word").isin(negative_words_to_remove)) \
    .orderBy(col("count").desc()).limit(20)
most_common_positive_word.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.resource", "positive_common_count/_doc") \
    .option("es.mapping.id", "count") \
    .option("es.write.operation", "upsert") \
    .option("es.index.auto.create", "true") \
    .option("es.nodes.wan.only", "true") \
    .mode("overwrite") \
    .save("positive_common_count/_doc")
most_common_negative_word.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.resource", "negative_common_count/_doc") \
    .option("es.mapping.id", "count") \
    .option("es.write.operation", "upsert") \
    .option("es.index.auto.create", "true") \
    .option("es.nodes.wan.only", "true") \
    .mode("overwrite") \
    .save("negative_common_count/_doc")

spark.stop()