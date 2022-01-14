# IBD_spark_java

To submit code to Spark in Docker containers:

## Package your code and sand it to your nodemaster container
 1) mvn package
 2) docker cp tpsparklog-1.0-SNAPSHOT.jar nodemaster:/home/hadoop

## Go inside nodemaster container
3) docker exec -u hadoop -it nodemaster /bin/bash
4) hadoop@nodemaster:~$ spark-submit --class ibd.spark.LogsApp tpsparklog-1.0-SNAPSHOT.jar /input/big_access_v2.csv 200


