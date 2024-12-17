package ma.enset.fraudedetection.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.logging.Logger;

@Service
public class FraudAlertReaderService {
    private static final Logger LOGGER = Logger.getLogger(FraudAlertReaderService.class.getName());

    private final InfluxDBClient influxDBClient;
    private final QueryApi queryApi;

    public FraudAlertReaderService() {
        String url = System.getenv().getOrDefault("INFLUXDB_URL", "http://localhost:8086");
        char[] token = System.getenv().getOrDefault("INFLUXDB_TOKEN",
                        "O3rj5SvhXgtoaV7X2GBLe9GzjgdwWhKMcTFuZtOsK0u5RXmo2tsCx6WfsPOYQ4ZhDAYQ5wIX7pitoG80W6gCgQ==")
                .toCharArray();
        String org = System.getenv().getOrDefault("INFLUXDB_ORG", "myorg");
        String bucket = System.getenv().getOrDefault("INFLUXDB_BUCKET", "frauddetection");

        this.influxDBClient = InfluxDBClientFactory.create(url, token, org, bucket);
        this.queryApi = influxDBClient.getQueryApi();
    }

    public void readFraudAlerts() {
        // Flux query to read from suspicious_transactions measurement
        String fluxQuery = "from(bucket:\"frauddetection\")"
                + " |> range(start: 0)"
                + " |> filter(fn: (r) => r._measurement == \"suspicious_transactions\")"
                + " |> pivot(rowKey:[\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")";

        try {
            List<FluxTable> tables = queryApi.query(fluxQuery);

            if (tables.isEmpty()) {
                LOGGER.info("No suspicious transactions found.");
                return;
            }

            LOGGER.info("--- Suspicious Transactions ---");
            for (FluxTable table : tables) {
                for (FluxRecord record : table.getRecords()) {
                    System.out.println("Transaction Details:");
                    System.out.println("Timestamp: " + record.getTime());
                    System.out.println("User ID: " + record.getValueByKey("userId"));
                    Object amount = record.getValueByKey("amount");
                    System.out.println("Amount: " + (amount != null ? amount : "N/A"));
                    System.out.println("--------------------");
                }
            }
        } catch (Exception e) {
            LOGGER.severe("Error reading fraud alerts: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Ensure client is closed
    public void closeConnection() {
        if (influxDBClient != null) {
            influxDBClient.close();
        }
    }
}