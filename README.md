# IBD_spark_java

To submit code to Spark in Docker containers:

mvn package
docker cp tpsparklog-1.0-SNAPSHOT.jar nodemaster:/home/hadoop

docker exec -u hadoop -it nodemaster /bin/bash
hadoop@nodemaster:~$ spark-submit --class ibd.spark.LogsApp tpsparklog-1.0-SNAPSHOT.jar /input/big_access_v2.csv 200


