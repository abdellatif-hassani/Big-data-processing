package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SalesAnalysis {
    public static void main(String[] args) {
        // Create Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("Sales Analysis")
                .setMaster("local[*]");

        // Create Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> salesRDD = sc.textFile("sales_data.csv");

        // Skip header and transform data
        JavaRDD<String[]> salesDataRDD = salesRDD
                .filter(line -> !line.startsWith("date"))
                .map(line -> line.split(","));

        // 1. Total sales by product
        JavaPairRDD<String, Double> salesByProduct = salesDataRDD
                .mapToPair(fields -> new Tuple2<>(fields[1], Double.parseDouble(fields[4])))
                .reduceByKey(Double::sum);

        System.out.println("Total sales by product:");
        salesByProduct.collect().forEach(tuple ->
                System.out.printf("%s: $%.2f%n", tuple._1(), tuple._2()));

        // 2. Total quantity sold by product
        JavaPairRDD<String, Integer> quantityByProduct = salesDataRDD
                .mapToPair(fields -> new Tuple2<>(fields[1], Integer.parseInt(fields[2])))
                .reduceByKey(Integer::sum);

        System.out.println("\nTotal quantity sold by product:");
        quantityByProduct.collect().forEach(tuple ->
                System.out.printf("%s: %d units%n", tuple._1(), tuple._2()));

        // 3. Daily total sales
        JavaPairRDD<String, Double> dailySales = salesDataRDD
                .mapToPair(fields -> new Tuple2<>(fields[0], Double.parseDouble(fields[4])))
                .reduceByKey(Double::sum);

        System.out.println("\nDaily total sales:");
        dailySales.collect().forEach(tuple ->
                System.out.printf("%s: $%.2f%n", tuple._1(), tuple._2()));

        // Close Spark context
        sc.close();
    }
}