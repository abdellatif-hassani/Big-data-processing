package ma.enset.fraudedetection.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import ma.enset.fraudedetection.model.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.function.Consumer;
import java.util.logging.Logger;

@Service
public class FraudStorageProcessor implements Consumer<String> {
    private static final Logger LOGGER = Logger.getLogger(FraudStorageProcessor.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private FraudAlertStorageService storageService;

    @Override
    public void accept(String message) {
        try {
            Transaction transaction = objectMapper.readValue(message, Transaction.class);
            storageService.storeFraudAlert(transaction);
            LOGGER.info("Transaction stored in InfluxDB for user: " + transaction.getUserId());
        } catch (Exception e) {
            LOGGER.severe("Error storing transaction: " + e.getMessage());
        }
    }
}