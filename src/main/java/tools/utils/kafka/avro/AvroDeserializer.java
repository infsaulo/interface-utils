package tools.utils.kafka.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {

    private static final Logger LOGGER = Logger.getLogger(AvroDeserializer.class.getName());

    protected final Class<T> targetType;

    public AvroDeserializer(Class<T> targetType) {

        this.targetType = targetType;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(String topic, byte[] data) {

        T result = null;

        if (data != null) {

            LOGGER.log(Level.INFO, "data " + DatatypeConverter.printHexBinary(data));

            try {

                final DatumReader<GenericRecord> datumReader =
                        new SpecificDatumReader<>(targetType.newInstance().getSchema());
                final Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);

                result = (T) datumReader.read(null, decoder);

                LOGGER.log(Level.INFO, "deserialized data " + result);
            } catch (IOException | InstantiationException | IllegalAccessException ex) {

                LOGGER.log(Level.SEVERE, "Can't deserialize data " + Arrays.toString(data), ex);
            }
        }

        return result;
    }
}