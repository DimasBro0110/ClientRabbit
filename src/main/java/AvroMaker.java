import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.json.simple.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
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

    public Schema getSchema(){
        return this.avroSchema;
    }

    public byte[] getBytesFromGenericRecord(GenericRecord record) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(this.avroSchema);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            Encoder e = EncoderFactory.get().binaryEncoder(os, null);
            writer.write(record, e);
            e.flush();
            return os.toByteArray();
        } catch (IOException e1) {
            e1.printStackTrace();
            return null;
        }finally {
            os.close();
        }
    }

    public GenericRecord marshallBytesToAvroRecord(String str){

        GenericDatumReader<GenericRecord> serveReader = new GenericDatumReader<GenericRecord>(this.avroSchema);
        try {
            return serveReader.read(null, DecoderFactory.get().binaryDecoder(str.getBytes("UTF-8"), null));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    public GenericRecord fromBytesToGenericRecord(byte[] bytes) throws IOException {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(this.avroSchema);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        return reader.read(null, decoder);
    }

    public byte[] getByteFromGenericRecord(List<GenericRecord> lst) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(this.avroSchema);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try{
            Encoder e = EncoderFactory.get().binaryEncoder(os, null);
            for(GenericRecord record: lst){
                writer.write(record, e);
            }
            e.flush();
            return os.toByteArray();
        }catch (Exception ex){
            return null;
        }finally {
            os.close();
        }
    }

    public List<GenericRecord> demarshallFileAvro(File file) throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(this.avroSchema);
        DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(file, reader);
        GenericRecord record = null;
        List<GenericRecord> lst = new ArrayList<GenericRecord>();
        while(fileReader.hasNext()){
            record = fileReader.next();
            lst.add(record);
        }
        fileReader.close();
        return lst;
    }

    public byte[] giveMeGenericRecord(JSONObject jsonObject) throws IOException {

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

        return getBytesFromGenericRecord(entityGenericRecord);
    }

}
