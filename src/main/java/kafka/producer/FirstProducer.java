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
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, KafkaConfig.transactionId);

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        logger.info("The very important step before beginning any transaction, takes care of many things that are required for a successful transaction");
        producer.initTransactions();

        logger.info("Beginning the transaction...");
        producer.beginTransaction();

        try {
            logger.info("Sending the messages in transaction...");
            producer.send(new ProducerRecord<>(KafkaConfig.topicName1, 0, "Hi Aditya!"));
            producer.send(new ProducerRecord<>(KafkaConfig.topicName2, 1, "I am Yogesh :)"));

            logger.info("Committing the messages in transaction...");
            producer.commitTransaction();
        }
        catch(Exception e) {
            logger.info("Aborting the transaction because of error during transaction...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException("Error during transaction and hence no messages are sent to the topics (ATOMICITY)");
        }

        producer.close();
    }
}
