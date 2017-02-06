import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import sun.net.www.content.text.Generic;

import java.io.File;
import java.io.IOException;

/**
 * Created by Dmitry on 02.02.2017.
 */

public class RabbitClientProd {

    /*
    * "C:\\Users\\Dmitry\\IdeaProjects\\ProducerProductionClientRabbit\\src\\main\\java\\test.avcs";
    * */

    public static void main(String[] args) throws IOException {

        if(args.length < 3){
            System.out.println("Not enough arguments passed");
            System.exit(1);
        }else {
            String pathToAvroScheme = args[0];
            String rabbitHost = args[1];
            String rabbitPort = args[2];
            new RabbitQueueClient(rabbitHost, rabbitPort, pathToAvroScheme).runFeedKafka();
        }

    }

}
