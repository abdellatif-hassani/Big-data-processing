package ma.enset.fraudedetection.web;

import ma.enset.fraudedetection.service.FraudAlertReaderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/fraud-alerts")
public class FraudAlertReaderController {
    private final FraudAlertReaderService readerService;

    public FraudAlertReaderController(FraudAlertReaderService readerService) {
        this.readerService = readerService;
    }

    @GetMapping("/read")
    public String readFraudAlerts() {
        readerService.readFraudAlerts();
        return "Fraud alerts read and printed to console";
    }
}
