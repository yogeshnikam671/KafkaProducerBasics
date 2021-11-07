package kafka.producer;

import kafka.producer.configuration.MultiThreadedKafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MultiThreadedProducer {
    private static final Logger logger = LoggerFactory.getLogger(MultiThreadedProducer.class);

    public static void main(String[] args) {
        //TODO - Here is how you load the properties from kafka.properties (Using inputStream)
        Properties props = new Properties();

        try(InputStream inputStream = new FileInputStream(MultiThreadedKafkaConfig.kafkaConfigFileLocation)) {
            props.load(inputStream); // <-- This is how you do it!

            props.put(ProducerConfig.CLIENT_ID_CONFIG, MultiThreadedKafkaConfig.applicationId);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        } catch (IOException e) {
            e.printStackTrace();
        }


        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        //TODO - This is how you create dispatcher threads.

        // TODO - Creating and starting the threads.
        Thread[] dispatchers = new Thread[MultiThreadedKafkaConfig.eventFiles.length];
        for(int i=0; i<MultiThreadedKafkaConfig.eventFiles.length; i++) {
            Dispatcher dispatcher =  new Dispatcher(MultiThreadedKafkaConfig.eventFiles[i],MultiThreadedKafkaConfig.topicName,producer);
            dispatchers[i] = new Thread(dispatcher);
            dispatchers[i].start();
        }

        // TODO - Joining the threads so that the main thread will start when all threads have stopped
        try {
            for(Thread t : dispatchers) t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Main thread interrupted");
        } finally {
            producer.close();
            logger.info("All files have been streamed by separate threads");
        }

    }
}




















