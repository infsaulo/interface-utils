package tools.utils.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaAvroConfluentInterfaceKV <K extends SpecificRecordBase, V extends SpecificRecordBase> {

    private static final Logger LOGGER = Logger.getLogger(KafkaAvroConfluentInterface.class.getName());

    private static final long CONSUMER_POLL_TIMEOUT = 6000;

    private static final int MAX_MSG_SIZE = 15728640;

    private final KafkaProducer<K, V> producer;

    private KafkaConsumer<K, V> consumer;


    public KafkaAvroConfluentInterfaceKV(final String kafkaUrl, final String registryUrl) {

        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaUrl);
        producerProps.put("schema.registry.url", registryUrl);
        producerProps.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("max.request.size", MAX_MSG_SIZE);
        producerProps.put("compression.type", "snappy");
        producerProps.put("acks", "-1");
        //producerProps.put("retries", "3");
        //producerProps.put("max.in.flight.requests.per.connection", "1");
        //producerProps.put("retry.backoff.ms", "1000");
        //producerProps.put("request.timeout.ms", "15000");
        producerProps.put("min.insync.replicas", "3");
        //producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        //producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 30000);
        //producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 15728640);

        producer = new KafkaProducer<>(producerProps);

        consumer = null;
    }

    public KafkaAvroConfluentInterfaceKV(final String kafkaUrl, final String registryUrl, final String keystoreFilePath,
                                       final String keystorePass, final String truststoreFilePath,
                                       final String truststorePass) {

        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaUrl);
        producerProps.put("schema.registry.url", registryUrl);
        producerProps.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("max.request.size", MAX_MSG_SIZE);
        producerProps.put("compression.type", "snappy");
        producerProps.put("security.protocol", "SSL");
        producerProps.put("ssl.keystore.location", keystoreFilePath);
        producerProps.put("ssl.keystore.password", keystorePass);
        producerProps.put("ssl.truststore.location", truststoreFilePath);
        producerProps.put("ssl.truststore.password", truststorePass);
        producerProps.put("acks", "-1");
        //producerProps.put("retries", "3");
        //producerProps.put("max.in.flight.requests.per.connection", "1");
        //producerProps.put("retry.backoff.ms", "1000");
        //producerProps.put("request.timeout.ms", "15000");
        producerProps.put("min.insync.replicas", "3");
        //producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 30000);
        //producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 15728640);

        producer = new KafkaProducer<>(producerProps);

        consumer = null;
    }

    public KafkaAvroConfluentInterfaceKV(final String kafkaUrl, final String groupId, final String registryUrl) {

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        consumerProps.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("max.poll.interval.ms", "600000");
        consumerProps.put("max.poll.records", "1");
        consumerProps.put("schema.registry.url", registryUrl);
        consumerProps.put("specific.avro.reader", true);
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("max.partition.fetch.bytes", MAX_MSG_SIZE);

        consumer = new KafkaConsumer<>(consumerProps);

        producer = null;
    }

    public KafkaAvroConfluentInterfaceKV(final String kafkaUrl, final String groupId, final String registryUrl,
                                       final int maxPollRecords) {

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("max.poll.interval.ms", "600000");
        consumerProps.put("max.poll.records", String.valueOf(maxPollRecords));
        consumerProps.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        consumerProps.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        consumerProps.put("schema.registry.url", registryUrl);
        consumerProps.put("specific.avro.reader", true);
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("max.partition.fetch.bytes", MAX_MSG_SIZE);

        consumer = new KafkaConsumer<>(consumerProps);

        producer = null;
    }

    public KafkaAvroConfluentInterfaceKV(final String kafkaUrl, final String groupId, final String registryUrl,
                                       final String keystoreFilePath, final String keystorePass,
                                       final String truststoreFilePath, final String truststorePass) {

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        consumerProps.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("max.poll.interval.ms", "600000");
        consumerProps.put("max.poll.records", "1");
        consumerProps.put("schema.registry.url", registryUrl);
        consumerProps.put("specific.avro.reader", true);
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("security.protocol", "SSL");
        consumerProps.put("ssl.keystore.location", keystoreFilePath);
        consumerProps.put("ssl.keystore.password", keystorePass);
        consumerProps.put("ssl.truststore.location", truststoreFilePath);
        consumerProps.put("ssl.truststore.password", truststorePass);
        consumerProps.put("max.partition.fetch.bytes", MAX_MSG_SIZE);

        consumer = new KafkaConsumer<>(consumerProps);

        producer = null;
    }

    public KafkaAvroConfluentInterfaceKV(final String kafkaUrl, final String groupId, final String registryUrl,
                                       final String keystoreFilePath, final String keystorePass,
                                       final String truststoreFilePath, final String truststorePass,
                                       final int maxPollRecords) {

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        consumerProps.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("max.poll.interval.ms", "600000");
        consumerProps.put("max.poll.records", String.valueOf(maxPollRecords));
        consumerProps.put("schema.registry.url", registryUrl);
        consumerProps.put("specific.avro.reader", true);
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("security.protocol", "SSL");
        consumerProps.put("ssl.keystore.location", keystoreFilePath);
        consumerProps.put("ssl.keystore.password", keystorePass);
        consumerProps.put("ssl.truststore.location", truststoreFilePath);
        consumerProps.put("ssl.truststore.password", truststorePass);
        consumerProps.put("max.partition.fetch.bytes", MAX_MSG_SIZE);

        consumer = new KafkaConsumer<>(consumerProps);

        producer = null;
    }

    public void checkSubscription(final String topic) {

        final Set<String> subs = consumer.subscription();

        if (!subs.contains(topic)) {

            consumer.subscribe(Arrays.asList(topic));
        }
    }

    public void sendMessage(final K key, final String topic, final V msg) {

        final ProducerRecord<K, V> recordMsg = new ProducerRecord<>(topic, key, msg);

        LOGGER.log(Level.INFO, "Sending msg with key " + key + " to topic " + topic);
        producer.send(recordMsg);
        producer.flush();
        LOGGER.log(Level.INFO, "Sent msg with key " + key + " to topic " + topic);
    }

    public void sendMessage(final K key, final String topic, final V msg, final int partition) {

        final ProducerRecord<K, V> recordMsg = new ProducerRecord<>(topic, partition, key, msg);

        LOGGER.log(Level.INFO, "Sending msg with key " + key + " to topic " + topic);
        producer.send(recordMsg);
        producer.flush();
        LOGGER.log(Level.INFO, "Sent msg with key " + key + " to topic " + topic);
    }

    public List<Map<String, Object>> consumeMessage(final String topic) {

        checkSubscription(topic);

        final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT));

        final List<Map<String, Object>> msgs = new LinkedList<>();

        for (ConsumerRecord<K, V> record : records) {

            final Map<String, Object> resultMap = new HashMap<>();

            final V message = record.value();
            final K key = record.key();
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

    public List<Map<String, Object>> consumeMessageSync(final String topic) {

        checkSubscription(topic);

        ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT));

        while(records.isEmpty()){

            records = consumer.poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT));
        }

        final List<Map<String, Object>> msgs = new LinkedList<>();

        for (ConsumerRecord<K, V> record : records) {

            final Map<String, Object> resultMap = new HashMap<>();

            final V message = record.value();
            final K key = record.key();
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

            producer.flush();
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
