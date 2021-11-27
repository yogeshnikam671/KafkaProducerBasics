package kafka.consumer.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.producer.models.People;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JSONDeserializer implements Deserializer {
    public final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if(data == null) return null;
        try {
            People people = objectMapper.convertValue(data, People.class);
            return objectMapper.writeValueAsString(people);
        }
        catch(Exception e) {
            throw new SerializationException("Error deserializing JSON message", e);
        }
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }
}
