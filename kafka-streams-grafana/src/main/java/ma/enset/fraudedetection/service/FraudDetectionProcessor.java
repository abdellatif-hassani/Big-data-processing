package ma.enset.fraudedetection.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import ma.enset.fraudedetection.model.Transaction;
import org.springframework.stereotype.Service;

import java.util.function.Function;
import java.util.logging.Logger;

@Service
public class FraudDetectionProcessor implements Function<String, Transaction> {
    private static final Logger LOGGER = Logger.getLogger(FraudDetectionProcessor.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public Transaction apply(String message) {
        try {
            Transaction transaction = objectMapper.readValue(message, Transaction.class);
            boolean isSuspicious = isFraudulent(transaction);
            if (isSuspicious) {
                LOGGER.info("Suspicious transaction detected: " + message);
                return transaction;
            } else {
                LOGGER.info("Non-suspicious transaction detected: " + message);
                return null;
            }
        } catch (Exception e) {
            LOGGER.severe("Error processing transaction: " + e.getMessage());
            return null;
        }
    }

    // Fraud detection methods remain the same
    private boolean isFraudulent(Transaction transaction) {
        return isHighValueTransaction(transaction) ||
                isUnusualTransactionPattern(transaction);
    }

    private boolean isHighValueTransaction(Transaction transaction) {
        return transaction.getAmount() > 10000;
    }

    private boolean isUnusualTransactionPattern(Transaction transaction) {
        // Example: Detect multiple high-value transactions in short time
        // In a real-world scenario, you'd implement more complex logic
        return false;
    }
}