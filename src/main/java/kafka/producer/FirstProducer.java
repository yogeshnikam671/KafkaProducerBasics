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

        ALLTransaction(producer);
        NOTHINGTransaction(producer);

        producer.close();
    }

    public static void ALLTransaction(KafkaProducer<Integer, String> producer) {
        logger.info("Beginning the ALL transaction...");
        producer.beginTransaction();
        try {
            logger.info("Sending the messages in ALL transaction...");
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
    }

    public static void NOTHINGTransaction(KafkaProducer<Integer, String> producer) {
        logger.info("Beginning the NOTHING transaction...");
        producer.beginTransaction();
        try {
            logger.info("Sending the messages in NOTHING transaction...");
            producer.send(new ProducerRecord<>(KafkaConfig.topicName1, 0, "Hello Nisha :)"));
            producer.send(new ProducerRecord<>(KafkaConfig.topicName2, 1, "Hola Friends!"));

            logger.info("Aborting the messages in transaction to demonstrate the All or Nothing (ATOMICITY) behaviour of a transaction...");
            logger.info("In simple words, none of the above messages should be sent in case we abort the transaction");
            producer.abortTransaction();
        }
        catch(Exception e) {
            logger.info("Aborting the transaction because of error during transaction...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException("Error during transaction and hence no messages are sent to the topics (ATOMICITY)");
        }
    }
}
