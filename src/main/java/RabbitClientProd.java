/**
 * Created by Dmitry on 02.02.2017.
 */
public class RabbitClientProd {

    public static void main(String[] args){
        new RabbitQueueClient("192.168.100.124", "5672").runFeedKafka();
    }

}
