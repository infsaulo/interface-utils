package tools.utils.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import tools.utils.kafka.avro.AvroDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.logging.Logger;

public class KafkaAvroInterface<T extends SpecificRecordBase> {

    private static final Logger LOGGER = Logger.getLogger(KafkaAvroInterface.class.getName());

    private static final long CONSUMER_POLL_TIMEOUT = 6000;

    private final KafkaProducer<String, T> producer;

    private KafkaConsumer<String, T> consumer;

    public KafkaAvroInterface(final String kafkaUrl) {

        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaUrl);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "tools.utils.kafka.avro.AvroSerializer");

        producer = new KafkaProducer<>(producerProps);

        consumer = null;
    }

    public KafkaAvroInterface(final String kafkaUrl, final String keystoreFilePath, final String keystorePass,
                              final String truststoreFilePath, final String truststorePass) {

        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaUrl);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "tools.utils.kafka.avro.AvroSerializer");
        producerProps.put("security.protocol", "SSL");
        producerProps.put("ssl.keystore.location", keystoreFilePath);
        producerProps.put("ssl.keystore.password", keystorePass);
        producerProps.put("ssl.truststore.location", truststoreFilePath);
        producerProps.put("ssl.truststore.password", truststorePass);

        producer = new KafkaProducer<>(producerProps);

        consumer = null;
    }

    public KafkaAvroInterface(final String kafkaUrl, final Class<T> targetType, final String groupId) {

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", false);
        consumerProps.put("session.timeout.ms", "5000");
        consumerProps.put("max.poll.records", "1");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "tools.utils.kafka.avro.AvroDeserializer");

        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new AvroDeserializer<>(targetType));

        producer = null;
    }

    public KafkaAvroInterface(final String kafkaUrl, final Class<T> targetType, final String groupId,
                              final String keystoreFilePath, final String keystorePass, final String truststoreFilePath,
                              final String truststorePass) {

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", false);
        consumerProps.put("session.timeout.ms", "5000");
        consumerProps.put("max.poll.records", "1");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "tools.utils.kafka.avro.AvroDeserializer");
        consumerProps.put("security.protocol", "SSL");
        consumerProps.put("ssl.keystore.location", keystoreFilePath);
        consumerProps.put("ssl.keystore.password", keystorePass);
        consumerProps.put("ssl.truststore.location", truststoreFilePath);
        consumerProps.put("ssl.truststore.password", truststorePass);

        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new AvroDeserializer<>(targetType));

        producer = null;
    }

    public void checkSubscription(final String topic) {

        final Set<String> subs = consumer.subscription();

        if (!subs.contains(topic)) {

            consumer.subscribe(Arrays.asList(topic));
        }
    }

    public void sendMessage(final String key, final String topic, final T msg) {

        final ProducerRecord<String, T> recordMsg = new ProducerRecord<>(topic, key, msg);

        producer.send(recordMsg);
        producer.flush();
    }

    public void sendMessage(final String key, final String topic, final T msg, final int partition) {

        final ProducerRecord<String, T> recordMsg = new ProducerRecord<>(topic, partition, key, msg);

        producer.send(recordMsg);
        producer.flush();
    }

    public List<Map<String, Object>> consumeMessage(final String topic) {

        checkSubscription(topic);

        final ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT));

        final List<Map<String, Object>> msgs = new LinkedList<>();

        for (ConsumerRecord<String, T> record : records) {

            final Map<String, Object> resultMap = new HashMap<>();

            final SpecificRecordBase message = record.value();
            final String key = record.key();
            final long resultOffset = record.offset();
            final int partition = record.partition();

            resultMap.put("msg", message);
            resultMap.put("key", key);
            resultMap.put("offset", resultOffset);
            resultMap.put("partition", partition);

            msgs.add(resultMap);
        }

        return msgs;
    }

    public void closeInterface() {

        if (producer != null) {

            producer.close();
        }

        if (consumer != null) {

            consumer.close();
        }
    }

    public void commitConsumer() {

        consumer.commitSync();
    }
}
