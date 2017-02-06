import com.rabbitmq.client.*;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.codec.binary.Base64;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Dmitry on 02.02.2017.
 */
public class RabbitQueueClient {

    private ConnectionFactory connectionFactory = new ConnectionFactory();
    private final String queueFileMonitorService = "FileServiceMQ";
    private AvroMaker avroMaker = null;
    private FeedKafka feedKafka = null;
    static Logger log = Logger.getLogger(RabbitQueueClient.class.getName());

    RabbitQueueClient(String host, String port, String pathToAvroSchema){
        this.connectionFactory.setUsername("guest");
        this.connectionFactory.setPassword("guest");
        this.connectionFactory.setHost(host);
        this.connectionFactory.setPort(Integer.valueOf(port));
        this.avroMaker = new AvroMaker(new File(pathToAvroSchema));
        this.feedKafka = new FeedKafka();
    }

    public void runFeedKafka(){
        Connection connection = null;
        try {
            connection = this.connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(this.queueFileMonitorService, true, false, false, null);
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    log.log(Level.INFO, " [x] Received '" + message + "'");
                    JSONParser jsonParser = new JSONParser();
                    JSONObject jsonObject = null;
                    try {
                        jsonObject = (JSONObject) jsonParser.parse(message);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    if(jsonObject != null){
                        File file = new File((String)jsonObject.get("path"));
                        //feedKafka.runFeedKafka(avroMaker.getBytesFromGenericRecord(record));
                        List<GenericRecord> records = avroMaker.demarshallFileAvro(file);
                        for(GenericRecord record: records){
                            byte[] bytes = avroMaker.getBytesFromGenericRecord(record);
                            feedKafka.runFeedKafka(bytes);
                        }
                        file.delete();
                        log.log(Level.INFO, "Data sent to Kafka!");
                    }
                }
            };
            channel.basicConsume(this.queueFileMonitorService, true, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
