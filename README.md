# Phân tích đánh giá của người dùng về nhà hàng Yelp

## Công nghệ sử dụng trong project

- Hadoop
- Spark
- Elasticsearch
- Kibana
- Docker

## Các thành phần trong project

### I. Docker

- Dự án được cài đặt trên môi trường Docker
  - Hadoop: Chạy phiên bản hadoop3.2.1 với java 8 và có một namenode, một secondary-namenode và 3 datanode
  - Spark: Chạy spark phiên bản 3.5 với 3 spark worker
  - Elasticsearch: Sử dụng images docker.elastic.co/elasticsearch/elasticsearch:7.10.0
  - Kibana: Sử dụng docker.elastic.co/kibana/kibana:7.10.0
- Để khởi chạy các container cần làm theo các bước sau:
  - Cài đặt Docker Desktop
  - Clone hoặc download dự án về máy tính
  - Mở terminal, cd vào thư mục dự án
  - Chạy lệnh **docker-compose** up để chạy các container

### II. Hadoop

- Hadoop được cấu hình đường dẫn là: hdfs://namenode:9000
- Có thể dùng đường dẫn này để đọc dữ liệu vào spark
- Hadoop được cấu hình có 3 datanode
- Để thêm dữ liệu vào Hadoop cần làm các bước sau:
  - Đảm bảo Docker Desktop đang chạy các container của hadoop
  - Dùng lệnh `docker cp ./path/to/file/in/pc namenode:/tmp`
  - Dùng lệnh `docker exec -it namenode bash` để vào terminal của namenode
  - Dùng lệnh `cd tmp` để vào thư mục tmp
  - Có thể phải tạo thư mục user trong hadoop, dùng lệnh `hdfs dfs -mkdir hdfs://namenode:9000/user` để tạo
  - Có thể tạo thư mục input để chứa các file dữ liệu đầu vào `hdfs dfs -mkdir /user/input`
  - Dùng lệnh `hdfs dfs -put ./tenfile /user/input` để chuyển file vào hadoop

=> Lúc này dữ liệu sẽ được phân bổ vào các container

### III. Spark

- Dữ liệu xử lý sẽ được spark đọc ra từ hadoop, sau đó sẽ ghi vào elasticsearch rồi trực quan bằng kibana

  ```
  spark = SparkSession.builder \
    .appName("Business Analys") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12-7.17.16,commons-httpclient:commons-httpclient:3.1") \
    .config("spark.jars", "./elasticsearch-spark-30_2.12-7.12.0.jar,./commons-httpclient-3.1.jar") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.resource", "test/_doc") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()
  review_file_path = "hdfs://namenode:9000/user/input/yelp_academic_dataset_review.json"
  review = spark.read.json(review_file_path)
  ```

- Câu lệnh tạo index và lưu dữ liệu vào elastic
  ```
  json_df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.resource", "your_index_name/_type") \
    .option("es.mapping.id", "Name") \
    .option("es.write.operation", "upsert") \
    .option("es.index.auto.create", "true") \
    .option("es.nodes.wan.only", "true") \
    .mode("append") \
    .save("your_index_name/_type")
  ```
- Lưu ý:

  - Trong câu lệnh trên, es.mapping.id phải là tên của một trường trong dataframe
  - Để có thể ghi dữ liệu vào elasticsearch cần phải thêm 2 file jar có trong dự án vào /opt/bitnami/spark/jars. Trong dự án này, nhóm thêm cả 2 file jar vào thư mục làm việc pyspark

- Để khởi chạy pyspark job thì cần thêm thư viện findspark vào đầu file code python (cần cài đặt thư viện này bằng pip vì không có sẵn)
  ```
  import findspark
  findspark.init()
  ```
- Spark được cấu hình có 3 spark-worker, có thể xem WebUI ở localhost:8080, cùng các Running Application và Complete Application
- Câu lệnh submit job:

  ```
  spark-submit --master spark://spark:7077 ./analysjob.py --jars ./commons-httpclient-3.1.jar ./elasticsearch-spark-30_2.12-7.12.0.jar  --driver-class-path ./commons-httpclient-3.1.jar  ./elasticsearch-spark-30_2.12-7.12.0.jar
  ```

  - Có thể thay đổi đường dẫn đến file jar và đường dẫn đến file code

### IV. Elasticsearch và Kibana

- Elasticsearch và Kibana sử dụng images docker.elastic.co/elasticsearch/elasticsearch:7.10.0
- Khi mở container lên WebUI ở localhost:5601 của Kibana sẽ không hoạt động được ngay mà cần chờ thời gian kết nối với Elasticsearch
- Khi chạy job nếu code sai, hoặc vì lí do nào đó container Elasticsearch có thể sẽ tự tắt, cần khởi động lại bằng tay hoặc cấu hình restart always trong docker-compose file
- Có thể xem thông tin các index hiện có trong mục Stack Managment trong WebUI của Kibana

### V. Các công việc đã làm

- Tổng hợp các chủ đề của bình luận, nhận thấy bình luận về nhà hàng là nhiều nhất
- Lọc dữ liệu đánh giá về restaurant ở các bang ở Mỹ trên 16 loại ẩm thực
- Tính trung bình độ dài comment theo hạng mục star đánh giá
- Sử dụng 2 file positive.txt và negative.txt cùng số sao được rate để đánh giá một câu bình luận từ đó rút ra khách hàng mong chờ điều gì ở nhà hàng
- Tổng hợp các từ positive và negative được sử dụng nhiều nhất trong các bình luận
- Training một model theo hình thức học có giám sát để dự đoán một câu bình luận là tích cực hay tiêu cực dựa trên bộ dữ liệu đã có

### V. Dataset

- Link: [Kaggle Yelp Dataset](https://www.kaggle.com/datasets/yelp-dataset/yelp-dataset)
