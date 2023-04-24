mvn package
docker cp stream/dataset.csv spark:/dataset.csv
docker cp target/final-project-1.0-SNAPSHOT.jar spark:/app_sql.jar
docker exec -it spark spark-submit --master=local --conf spark.sql.shuffle.partitions=1 --class com.cs523.SparkSQLAnalyze --packages "org.apache.hbase:hbase-client:2.4.17,org.apache.hbase:hbase-common:2.4.17" /app_sql.jar /dataset.csv