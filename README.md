# IBD_spark_java

## !!! Place big_access_v2.csv in src/main/resources/big_access_v2.csv!!!
## Then send this .csv file to your hdfs in the /input folder
### -> Do these steps only once when creating your Docker containers to place the .csv file in the hdfs

 0) docker cp path_to_csv_file.csv nodemaster:/home/hadoop
 1) docker exec -u hadoop -it nodemaster /bin/bash
 2) hdfs dfs -mkdir /input
 3) hdfs dfs put big_access_v2.csv /input
 

##To submit yout Java code to Spark in the Docker containers:

### Package your code and sand it to your nodemaster container
 1) mvn package
 2) docker cp tpsparklog-1.0-SNAPSHOT.jar nodemaster:/home/hadoop

### Go inside nodemaster container
3) docker exec -u hadoop -it nodemaster /bin/bash
4) hadoop@nodemaster:~$ spark-submit --class ibd.spark.LogsApp tpsparklog-1.0-SNAPSHOT.jar /input/big_access_v2.csv 200


