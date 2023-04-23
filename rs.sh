docker-compose down 
docker-compose up -d --build
./spark_sql.sh
# docker-compose restart dashboard
./spark.sh