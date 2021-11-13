package kafka.producer.configuration;

public class KafkaConfig {
    public final static String applicationId = "Aditya";
    public final static String topicName1 = "transaction-topic-1";
    public final static String topicName2 = "transaction-topic-2";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static int numberOfEvents = 1000000;
    public final static String transactionId = "transaction-id";
}
