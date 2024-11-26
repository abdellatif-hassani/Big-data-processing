package ma.enset;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class IncidentsAnalysis {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        // Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Hospital Incidents Analysis")
                .master("local[*]")
                .getOrCreate();

        // Define schema for CSV files
        StructType schema = new StructType()
                .add("id", DataTypes.StringType)
                .add("titre", DataTypes.StringType)
                .add("description", DataTypes.StringType)
                .add("service", DataTypes.StringType)
                .add("date", DataTypes.TimestampType);

        // Read streaming data
        Dataset<Row> incidents = spark.readStream()
                .schema(schema)
                .option("sep", ",")
                .option("header", "true")
                .csv("hdfs://namenode:9000/incidents/*.csv");

        // Query 1: Count incidents by service
        Dataset<Row> incidentsByService = incidents
                .groupBy("service")
                .count()
                .orderBy(col("count").desc());

        // Query 2: Find top 2 years with most incidents
        Dataset<Row> incidentsByYear = incidents
                .withColumn("year", year(col("date")))
                .groupBy("year")
                .count()
                .orderBy(col("count").desc())
                .limit(2);

        // Start streaming query for incidents by service
        StreamingQuery serviceQuery = incidentsByService.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .start();

        // Start streaming query for incidents by year
        StreamingQuery yearQuery = incidentsByYear.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", false)
                .start();

        // Wait for queries to terminate
        serviceQuery.awaitTermination();
        yearQuery.awaitTermination();
    }
}