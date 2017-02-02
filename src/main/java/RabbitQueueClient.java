import com.rabbitmq.client.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.IOException;

/**
 * Created by Dmitry on 02.02.2017.
 */
public class RabbitQueueClient {

    private ConnectionFactory connectionFactory = new ConnectionFactory();
    private final String queueFileMonitorService = "FileServiceMQ";

    RabbitQueueClient(String host, String port){
        this.connectionFactory.setUsername("guest");
        this.connectionFactory.setPassword("guest");
        this.connectionFactory.setHost(host);
        this.connectionFactory.setPort(Integer.valueOf(port));
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
                    System.out.println(" [x] Received '" + message + "'");
                    JSONParser jsonParser = new JSONParser();
                    JSONObject jsonObject = null;
                    try {
                        jsonObject = (JSONObject) jsonParser.parse(message);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    if(jsonObject != null){
                        String path = (String) jsonObject.get("path");
                        File file = new File(path);
                        if(file.isFile()){
                            System.out.println("File");
                            System.out.println(file.exists());
                            file.delete();
                        }else if(file.isDirectory()){
                            System.out.println("Directory");
                        }else{
                            System.out.println("None");
                        }
                    }
                }
            };
            channel.basicConsume(this.queueFileMonitorService, true, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
