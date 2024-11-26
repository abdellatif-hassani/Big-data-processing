package ma.enset.kafkastreamsex1.service;


@FunctionalInterface
public interface MessageProcessor<T> {
    T process(T message);
}
