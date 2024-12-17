package ma.enset.fraudedetection.model;

import lombok.Data;

@Data
public class Transaction {
    private String userId;
    private Double amount;
    private String timestamp;
}
