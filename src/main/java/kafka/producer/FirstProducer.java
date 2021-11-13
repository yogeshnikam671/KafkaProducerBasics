package kafka.producer;

import kafka.producer.configuration.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FirstProducer {
    private static final Logger logger = LoggerFactory.getLogger(FirstProducer.class);

    public static void main(String[] args) {
        logger.info("Creating the kafka producer...");

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConfig.applicationId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        producer.send(new ProducerRecord<>(KafkaConfig.topicName1, 0, "Hi Aditya!"));
        producer.send(new ProducerRecord<>(KafkaConfig.topicName2, 1, "I am Yogesh :)"));

        producer.close();
    }
}
