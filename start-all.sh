./stop-all.sh
docker-compose up -d --build
./spark_sql.sh
docker-compose restart dashboard
./spark.sh