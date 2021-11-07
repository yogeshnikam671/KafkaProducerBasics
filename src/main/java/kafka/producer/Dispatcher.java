package kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

//TODO - Runnable allows us to execute an instance of this class as a separate thread.
//TODO - This class will read all the records from a given file and send those records to a kafka topic.
public class Dispatcher implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);
    private String fileLocation;
    private String topicName;
    private KafkaProducer<Integer, String> producer;

    public Dispatcher(String fileLocation, String topicName, KafkaProducer<Integer, String> producer) {
        this.fileLocation = fileLocation;
        this.topicName = topicName;
        this.producer = producer;
    }

    @Override
    public void run() {
        logger.info("Started processing the file: " + fileLocation);
        File file = new File(fileLocation);

        try(Scanner scanner = new Scanner(file)) {
            while(scanner.hasNextLine()) {
                String line = scanner.nextLine();
                ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topicName, null, line);
                producer.send(record);
            }

            logger.info("Finished processing the file: " + fileLocation);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
