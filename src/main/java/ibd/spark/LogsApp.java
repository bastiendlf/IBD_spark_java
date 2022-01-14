package ibd.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;

import static org.apache.spark.sql.functions.*;

public class LogsApp implements Serializable {

    static int numberShow = 40;

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("Usage: LogsApp <csv file> <nb show?>");
            System.exit(1);
        }

        if (args.length == 2) {
            try {
                numberShow = Integer.parseInt(args[1]);
            } catch (NumberFormatException ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
        String url = "spark://nodemaster:7077"; // local[*]

        SparkSession spark = SparkSession.builder().master(url).appName("LogsApp").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> logAccessDS = spark.read()
                .option("header", true)
                .option("delimiter", "\t")
                .option("inferSchema", true)
                .csv(args[0]);

//        inspection(logAccessDS);
//        comptage(logAccessDS);
//        filtrage(logAccessDS);
//        aggregation(logAccessDS);
//        tri(logAccessDS);
//        regroupement(logAccessDS);
//        transformation(logAccessDS);
        ecriture(logAccessDS);
    }


    private static void inspection(Dataset<Row> ds) {
        ds.show(numberShow);
        ds.explain();
    }


    static void comptage(Dataset<Row> ds) {
        System.out.println("Total count : " + ds.count());
        long ipCount = ds.select("ip").distinct().count();

        System.out.println("Unique IP count : " + ipCount);
    }

    static void filtrage(Dataset<Row> ds) {
        Dataset<Row> filtredRows = ds
                .filter((FilterFunction<Row>) row -> row.getInt(row.fieldIndex("status")) == 200)
                .filter((FilterFunction<Row>) row -> row.getString(row.fieldIndex("ip")).startsWith("178."));
        filtredRows.show(numberShow);
    }

    private static void aggregation(Dataset<Row> ds) {
        ds.withColumn("year", year(col("date")))
                .filter((FilterFunction<Row>) row -> row.getInt(row.fieldIndex("year")) >= 2016)
                .select(avg("size"))
                .show(numberShow);
    }


    private static void tri(Dataset<Row> ds) {
        ds.filter((FilterFunction<Row>) row -> row.getInt(row.fieldIndex("status")) == 200)
                .sort(col("size").desc())
                .select("ip", "size")
                .show(numberShow);
    }

    private static void regroupement(Dataset<Row> ds) {
        ds.groupBy("ip")
                .sum("size")
                .show(numberShow);
    }


    private static void transformation(Dataset<Row> ds) {
        Dataset<Row> newDs = ds.withColumn("size kb", col("size").$div(100));
        newDs.show(numberShow);
    }

    private static void ecriture(Dataset<Row> ds) {
        Dataset<Row> newDs = ds.filter((FilterFunction<Row>) row -> row.getInt(row.fieldIndex("status")) != 200);
        newDs.write().csv("/input/filtering_result.csv");
    }

}
