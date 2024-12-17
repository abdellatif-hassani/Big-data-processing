package ma.enset.fraudedetection.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import ma.enset.fraudedetection.model.Transaction;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.logging.Logger;

@Service
public class FraudAlertStorageService {
    private static final Logger LOGGER = Logger.getLogger(FraudAlertStorageService.class.getName());

    private final InfluxDBClient influxDBClient;
    private final WriteApiBlocking writeApi;

    public FraudAlertStorageService() {
        String url = System.getenv().getOrDefault("INFLUXDB_URL", "http://localhost:8086");
        char[] token = System.getenv().getOrDefault("INFLUXDB_TOKEN",
                        "O3rj5SvhXgtoaV7X2GBLe9GzjgdwWhKMcTFuZtOsK0u5RXmo2tsCx6WfsPOYQ4ZhDAYQ5wIX7pitoG80W6gCgQ==")
                .toCharArray();
        String org = System.getenv().getOrDefault("INFLUXDB_ORG", "myorg");
        String bucket = System.getenv().getOrDefault("INFLUXDB_BUCKET", "frauddetection");

        this.influxDBClient = InfluxDBClientFactory.create(url, token, org, bucket);
        this.writeApi = influxDBClient.getWriteApiBlocking();
    }

    public void storeFraudAlert(Transaction transaction) {
        try {
            LOGGER.info("Attempting to store transaction: " + transaction);

            // Validate transaction data
            if (transaction == null) {
                LOGGER.severe("Transaction is null");
                return;
            }

            // Ensure timestamp is not null
            Instant timestamp = transaction.getTimestamp() != null
                    ? Instant.parse(transaction.getTimestamp())
                    : Instant.now();

            // Create Point with explicit double conversion for amount
            Point point = Point
                    .measurement("suspicious_transactions")
                    .addTag("userId", transaction.getUserId())
                    .addField("amount", transaction.getAmount() != null
                            ? transaction.getAmount().doubleValue()
                            : 0.0)
                    .time(timestamp, WritePrecision.MS);

            System.out.println("Line Protocol: " + point.toLineProtocol());  // Debug actual data

            LOGGER.info("Created InfluxDB point: " + point.toLineProtocol());

            writeApi.writePoint(point);
            LOGGER.info("Successfully stored fraud alert for user: " + transaction.getUserId());
        } catch (Exception e) {
            LOGGER.severe("Detailed error storing transaction: " + e.getMessage());
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