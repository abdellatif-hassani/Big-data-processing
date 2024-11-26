package ma.enset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class HospitalDataAnalysis {
    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Hospital Data Analysis")
                .master("local[*]")
                .getOrCreate();

        // Load data from MySQL
        Dataset<Row> consultationsDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/hospital")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", "CONSULTATIONS")
                .option("user", "root")
                .option("password", "password")
                .load();

        Dataset<Row> medecinsDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/hospital")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", "MEDECINS")
                .option("user", "root")
                .option("password", "password")
                .load();

        Dataset<Row> patientsDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/hospital")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("dbtable", "PATIENTS")
                .option("user", "root")
                .option("password", "password")
                .load();

        // Register temporary views
        consultationsDF.createOrReplaceTempView("consultations");
        medecinsDF.createOrReplaceTempView("medecins");
        patientsDF.createOrReplaceTempView("patients");

        // 1. Number of consultations per day
        System.out.println("Nombre de consultations par jour:");
        Dataset<Row> consultationsPerDay = spark.sql(
                "SELECT date, COUNT(*) as nombre_consultations " +
                        "FROM consultations " +
                        "GROUP BY date " +
                        "ORDER BY date"
        );
        consultationsPerDay.show();

        // 2. Number of consultations per doctor
        System.out.println("Nombre de consultations par médecin:");
        Dataset<Row> consultationsPerDoctor = spark.sql(
                "SELECT m.name, COUNT(*) as nombre_consultations " +
                        "FROM consultations c " +
                        "JOIN medecins m ON c.medecin_id = m.id " +
                        "GROUP BY m.name " +
                        "ORDER BY m.name"
        );
        consultationsPerDoctor.show();

        // 3. Number of unique patients per doctor
        System.out.println("Nombre de patients par médecin:");
        Dataset<Row> patientsPerDoctor = spark.sql(
                "SELECT m.name, COUNT(DISTINCT c.patient_id) as nombre_patients " +
                        "FROM consultations c " +
                        "JOIN medecins m ON c.medecin_id = m.id " +
                        "GROUP BY m.name " +
                        "ORDER BY m.name"
        );
        patientsPerDoctor.show();

        spark.stop();
    }
}