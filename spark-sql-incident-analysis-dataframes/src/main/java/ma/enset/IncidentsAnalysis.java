package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class IncidentsAnalysis {
    public static void main(String[] args) {
        // Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Incidents Analysis")
                .master("local[*]")
                .getOrCreate();

        // Reading from CSV file
        Dataset<Row> incidents = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("incidents.csv");

        // Creating a temporary view to use SQL
        incidents.createOrReplaceTempView("incidents");

        // 1. Number of incidents per service
        System.out.println("Number of incidents per service:");
        Dataset<Row> incidentsPerService = spark.sql(
                "SELECT service, COUNT(*) as incident_count " +
                        "FROM incidents " +
                        "GROUP BY service " +
                        "ORDER BY incident_count DESC"
        );
        incidentsPerService.show();

        // 2. Two years with most incidents
        System.out.println("Top 2 years with most incidents:");
        Dataset<Row> incidentsPerYear = spark.sql(
                "SELECT YEAR(date) as year, COUNT(*) as incident_count " +
                        "FROM incidents " +
                        "GROUP BY YEAR(date) " +
                        "ORDER BY incident_count DESC " +
                        "LIMIT 2"
        );
        incidentsPerYear.show();

        // Alternative using DataFrame API
        System.out.println("Using DataFrame API - Incidents per service:");
        incidents.groupBy("service")
                .count()
                .orderBy(functions.col("count").desc())
                .show();

        // Stop Spark Session
        spark.stop();
    }
}