package tools.utils.kafka.avro;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {

    private static final Logger LOGGER = Logger.getLogger(AvroSerializer.class.getName());

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
    }

    @Override
    public byte[] serialize(String topic, T data) {

        byte[] result = null;

        if (data != null) {

            LOGGER.log(Level.INFO, "data " + data);

            try {

                final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                final BinaryEncoder binaryEncoder =
                        EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);

                final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(data.getSchema());
                datumWriter.write(data, binaryEncoder);

                binaryEncoder.flush();
                byteArrayOutputStream.close();

                result = byteArrayOutputStream.toByteArray();

                LOGGER.log(Level.INFO, "serialized data " + DatatypeConverter.printHexBinary(result));
            } catch (IOException ex) {

                LOGGER.log(Level.SEVERE, "Can't serialize data " + data, ex);
            }
        }

        return result;
    }
}
