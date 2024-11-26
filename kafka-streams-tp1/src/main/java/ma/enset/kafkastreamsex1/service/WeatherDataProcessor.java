package ma.enset.kafkastreamsex1.service;

import org.springframework.stereotype.Service;

import java.util.function.Function;

@Service
public class WeatherDataProcessor implements Function<String, String> {

    @Override
    public String apply(String message) {
        // Parse input message
        String[] parts = message.split(",");
        String station = parts[0];
        double celsius = Double.parseDouble(parts[1]);
        double humidity = Double.parseDouble(parts[2]);

        // Filter high temperatures
        if (celsius <= 30) {
            return null;
        }

        // Convert to Fahrenheit
        double fahrenheit = (celsius * 9.0/5.0) + 32;

        // Return transformed message
        return String.format("%s,%.1f,%.1f", station, fahrenheit, humidity);
    }
}
