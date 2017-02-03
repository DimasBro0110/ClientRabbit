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

    public static void main(String[] args) throws IOException {

//        new RabbitQueueClient("192.168.100.124", "5672").runFeedKafka();

        Schema schema = new Schema.Parser().parse(new File("/home/dimas/IdeaProjects/ProducerProductionClientRabbit/src/main/java/test.avcs"));
        GenericRecord test_1 = new GenericData.Record(schema);
        test_1.put("msidn", 1000L);
        test_1.put("lacCell", "test");
        test_1.put("mainUrl", "test");
        test_1.put("cookie", "test");
        test_1.put("protocol", "test");
        test_1.put("timestampStart", 1000L);
        test_1.put("timestampEnd", 1000L);
        test_1.put("downloadKb", 1000L);
        test_1.put("uploadKb", 1000L);
        test_1.put("reason", "test");
        File file = new File("test.avro");
        DatumWriter<GenericRecord> avroTestDatumWriter = new SpecificDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(avroTestDatumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(test_1);
        dataFileWriter.close();
    }

}
