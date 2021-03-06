package kafka.producer;


import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.producer.configuration.KafkaConfig;
import kafka.producer.serializer.JSONSerializer;
import kafka.producer.models.People;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class JSONProducer {
    private static final Logger logger = LoggerFactory.getLogger(JSONProducer.class);
    private static final String peopleTopicName = "people-topic";

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, KafkaConfig.applicationId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerializer.class.getName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers);

        KafkaProducer<Integer, People> producer = new KafkaProducer<Integer, People>(props);


        String filesLocation = "src/main/resources/json-data";
        File filesFolder = new File(filesLocation);

        for(File file: Objects.requireNonNull(filesFolder.listFiles())) {
            People people = getTheJSONDataAsPOJO(file);
            logger.info("The People POJO object --> " + people.toString());

            ProducerRecord<Integer, People> message = new ProducerRecord<Integer, People>(peopleTopicName, 0, people);
            producer.send(message);
        }

        producer.close();
        logger.info("The message is sent successfully");
    }

    private static People getTheJSONDataAsPOJO(File file) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(file, People.class);
    }
}







//TODO - Ignore this, you don't need to worry about this multi threaded producer. Left here just for reference.
/*
class JSONDispatcher implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(JSONDispatcher.class);
    private String fileLocation;
    private String topicName;
    private KafkaProducer<Integer, People> producer;

    public JSONDispatcher(String fileLocation, String topicName, KafkaProducer<Integer, People> producer) {
        this.fileLocation = fileLocation;
        this.topicName = topicName;
        this.producer = producer;
    }

    @Override
    public void run() {
        logger.info("Started processing the JSON objects...");
        ObjectMapper objectMapper = new ObjectMapper();
        File file = new File(fileLocation);
        try {
            People people = objectMapper.readValue(file, People.class);
            ProducerRecord<Integer, People> message = new ProducerRecord<Integer, People>(topicName, null, people);
            producer.send(message);
            logger.info("The messages are sent");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
*/











