package ma.enset.kafkastreamsex1.config;

import ma.enset.kafkastreamsex1.service.MessageProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.function.Function;

@Configuration
public class MessageStreamConfig {

    @Bean
    public Function<String, String> processMessage(MessageProcessor<String> processor) {
        return message -> processor.process(message);
    }
}