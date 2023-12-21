# Live-Bus-Streaming
Live Bus Streaming (Kafka)
The project is straight forward all done throught the cloud using AWS. NIFI well grab the data from an API called Rest bus, upload it directly using debezium(MySQL), Kafka for instaneous data processing, spark streaming in HUDI table format and athena and superset for analytics and querying.






Kafka bash commands:

curl -X DELETE localhost:8083/connectors/inventory-connector

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{ "name": "inventory-connector1", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1", "database.hostname": "mysql", "database.port": "3306", "database.user": "debezium", "database.password": "dbz", "database.server.id": "184054", "database.server.name": "dbserver1", "database.include.list": "final", "database.history.kafka.bootstrap.servers": "kafka:9092", "database.history.kafka.topic": "dbhistory.final" } }'

bin/kafka-console-consumer.sh --topic dbserver1.final.bus_status --bootstrap-server 1d684d5b22b3:9092

Pyspark Command:

--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false"

./spark-submit --master local[*] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.hudi:hudi-spark3-bundle_2.12:0.12.3 --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" --conf "spark.sql.hive.convertMetastoreParquet=false" /home/ec2-user/final_project/pyspark_streaming.py

MYSQL:

select event_time, count(*) from bus_status group by event_time;
