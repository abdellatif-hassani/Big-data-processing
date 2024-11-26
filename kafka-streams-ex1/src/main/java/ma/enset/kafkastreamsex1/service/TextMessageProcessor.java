package ma.enset.kafkastreamsex1.service;

import org.springframework.stereotype.Service;

@Service
public class TextMessageProcessor<T> implements MessageProcessor<String> {
    @Override
    public String process(String message) {
        return message.toUpperCase();
    }
}
