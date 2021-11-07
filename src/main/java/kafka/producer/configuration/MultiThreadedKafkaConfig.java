package kafka.producer.configuration;

public class MultiThreadedKafkaConfig {
    public final static String applicationId = "Multi-Threaded-Producer";
    public final static String topicName = "nse-eod-topic";
    public final static String kafkaConfigFileLocation = "kafka.properties";
    public final static String[] eventFiles = {"data/NSE05NOV2018BHAV.csv", "data/NSE06NOV2018BHAV.csv"};
}
