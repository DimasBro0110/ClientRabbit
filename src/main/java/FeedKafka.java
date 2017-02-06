import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Dmitry on 03.02.2017.
 */

public class FeedKafka {
    private final Properties props = new Properties();
    private KafkaProducer<String, byte[]> producer;
    static Logger log = Logger.getLogger(FeedKafka.class.getName());

    FeedKafka(){
        props.put("bootstrap.servers",
                "cloudera-nn-master.ds.local:9092,cloudera-nn-slave.ds.local:9092");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("batch.size", 16384);
        props.put("buffer.memory", 33554432);
        props.put("acks", "all");
        props.put("group.id", "test_avro");
        producer = new KafkaProducer<String, byte[]>(props);
    }

    public void runFeedKafka(byte[] record){
        ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<String, byte[]>("test_avro", record);
        producer.send(producerRecord);
        log.log(Level.INFO, "Record sent to kafka!");
    }
}
