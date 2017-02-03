import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.json.simple.JSONObject;

import java.io.File;
import java.util.Random;

/**
 * Created by dimas on 03.02.17.
 */
public class AvroMaker {

    private Schema avroSchema = null;
    private File pathToAvroSchema = null;
    private static Random rnd = new Random();

    AvroMaker(File pathAvro){
        this.pathToAvroSchema = pathAvro;
        try{
            this.avroSchema = new Schema.Parser().parse(this.pathToAvroSchema);
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }

    public GenericRecord giveMeGenericRecord(JSONObject jsonObject){

        /*
        В идеале на входе будет jsonbject, полученный при чтении из директории локальной фс
        В jsonObject список из объектов, релевантных схеме, но не отмаршалинных в авро запись
        * */

        GenericRecord entityGenericRecord = new GenericData.Record(this.avroSchema);

        /*А пока рандомные поля :) */

        entityGenericRecord.put("msidn", rnd.nextLong());
        entityGenericRecord.put("lacCell", "test" + String.valueOf(rnd.nextGaussian()));
        entityGenericRecord.put("mainUrl", "test" + String.valueOf(rnd.nextGaussian()));
        entityGenericRecord.put("cookie", "test" + String.valueOf(rnd.nextGaussian()));
        entityGenericRecord.put("protocol", "test" + String.valueOf(rnd.nextGaussian()));
        entityGenericRecord.put("timestampStart", rnd.nextLong());
        entityGenericRecord.put("timestampEnd", rnd.nextLong());
        entityGenericRecord.put("downloadKb", rnd.nextLong());
        entityGenericRecord.put("uploadKb", rnd.nextLong());
        entityGenericRecord.put("reason", "test" + String.valueOf(rnd.nextGaussian()));

        /*Тут надо подумать, все таки мб возвращать список отмаршалинных объектов*/

        return entityGenericRecord;
    }

}
