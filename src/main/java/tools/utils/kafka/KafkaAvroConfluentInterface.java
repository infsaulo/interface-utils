package tools.utils.kafka;

import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaAvroConfluentInterface {

    private static final Logger LOGGER = Logger.getLogger(KafkaAvroConfluentInterface.class.getName());

    private static final long CONSUMER_POLL_TIMEOUT = 6000;

    private KafkaProducer<String, SpecificRecordBase> simpleKeyProducer;

    private KafkaConsumer<String, SpecificRecordBase> simpleKeyConsumer;

    private KafkaProducer<SpecificRecordBase, SpecificRecordBase> avroKeyProducer;

    private KafkaConsumer<SpecificRecordBase, SpecificRecordBase> avroKeyConsumer;

    private final boolean avroKey;

    private Properties loadSerializers(final boolean avroKey) {

        final Properties props = new Properties();

        if (avroKey) {

            props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        } else {

            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        }

        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

        return props;
    }

    private Properties loadDeserializers(final boolean avroKey) {

        final Properties props = new Properties();

        if (avroKey) {

            props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        } else {

            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }

        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        return props;
    }

    public KafkaAvroConfluentInterface(final String kafkaUrl, final String registryUrl, final boolean avroKey) {

        this.avroKey = avroKey;
        final Properties producerProps = loadSerializers(avroKey);
        producerProps.put("bootstrap.servers", kafkaUrl);
        producerProps.put("schema.registry.url", registryUrl);
        //producerProps.put("acks", "-1");
        //producerProps.put("retries", "3");
        //producerProps.put("max.in.flight.requests.per.connection", "1");
        //producerProps.put("retry.backoff.ms", "1000");
        //producerProps.put("request.timeout.ms", "15000");
        //producerProps.put("min.insync.replicas", "2");
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384 * 4);

        if (avroKey) {

            avroKeyProducer = new KafkaProducer<>(producerProps);
        } else {

            simpleKeyProducer = new KafkaProducer<>(producerProps);
        }

        avroKeyConsumer = null;
        simpleKeyConsumer = null;
    }

    public KafkaAvroConfluentInterface(final String kafkaUrl, final String registryUrl, final String keystoreFilePath,
                                       final String keystorePass, final String truststoreFilePath,
                                       final String truststorePass, final boolean avroKey) {

        this.avroKey = avroKey;
        final Properties producerProps = loadSerializers(avroKey);
        producerProps.put("bootstrap.servers", kafkaUrl);
        producerProps.put("schema.registry.url", registryUrl);
        producerProps.put("security.protocol", "SSL");
        producerProps.put("ssl.keystore.location", keystoreFilePath);
        producerProps.put("ssl.keystore.password", keystorePass);
        producerProps.put("ssl.truststore.location", truststoreFilePath);
        producerProps.put("ssl.truststore.password", truststorePass);
        //producerProps.put("acks", "-1");
        //producerProps.put("retries", "3");
        //producerProps.put("max.in.flight.requests.per.connection", "1");
        //producerProps.put("retry.backoff.ms", "1000");
        //producerProps.put("request.timeout.ms", "15000");
        //producerProps.put("min.insync.replicas", "2");
        producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        producerProps.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384 * 4);

        if (avroKey) {

            avroKeyProducer = new KafkaProducer<>(producerProps);
        } else {

            simpleKeyProducer = new KafkaProducer<>(producerProps);
        }

        avroKeyConsumer = null;
        simpleKeyConsumer = null;
    }

    public KafkaAvroConfluentInterface(final String kafkaUrl, final String groupId, final String registryUrl, final boolean avroKey) {

        this.avroKey = avroKey;
        final Properties consumerProps = loadDeserializers(avroKey);
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("max.poll.records", "1");
        consumerProps.put("schema.registry.url", registryUrl);
        consumerProps.put("specific.avro.reader", true);
        consumerProps.put("auto.offset.reset", "earliest");

        if (avroKey) {

            avroKeyConsumer = new KafkaConsumer<>(consumerProps);
        } else {

            simpleKeyConsumer = new KafkaConsumer<>(consumerProps);
        }

        avroKeyProducer = null;
        simpleKeyProducer = null;
    }

    public KafkaAvroConfluentInterface(final String kafkaUrl, final String groupId, final String registryUrl,
                                       final int maxPollRecords, final boolean avroKey) {

        this.avroKey = avroKey;
        final Properties consumerProps = loadDeserializers(avroKey);
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("max.poll.records", String.valueOf(maxPollRecords));
        consumerProps.put("schema.registry.url", registryUrl);
        consumerProps.put("specific.avro.reader", true);
        consumerProps.put("auto.offset.reset", "earliest");

        if (avroKey) {

            avroKeyConsumer = new KafkaConsumer<>(consumerProps);
        } else {

            simpleKeyConsumer = new KafkaConsumer<>(consumerProps);
        }

        avroKeyProducer = null;
        simpleKeyProducer = null;
    }

    public KafkaAvroConfluentInterface(final String kafkaUrl, final String groupId, final String registryUrl,
                                       final String keystoreFilePath, final String keystorePass,
                                       final String truststoreFilePath, final String truststorePass, final boolean avroKey) {

        this.avroKey = avroKey;
        final Properties consumerProps = loadDeserializers(avroKey);
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("max.poll.records", "1");
        consumerProps.put("schema.registry.url", registryUrl);
        consumerProps.put("specific.avro.reader", true);
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("security.protocol", "SSL");
        consumerProps.put("ssl.keystore.location", keystoreFilePath);
        consumerProps.put("ssl.keystore.password", keystorePass);
        consumerProps.put("ssl.truststore.location", truststoreFilePath);
        consumerProps.put("ssl.truststore.password", truststorePass);

        if (avroKey) {

            avroKeyConsumer = new KafkaConsumer<>(consumerProps);
        } else {

            simpleKeyConsumer = new KafkaConsumer<>(consumerProps);
        }

        avroKeyProducer = null;
        simpleKeyProducer = null;
    }

    public KafkaAvroConfluentInterface(final String kafkaUrl, final String groupId, final String registryUrl,
                                       final String keystoreFilePath, final String keystorePass,
                                       final String truststoreFilePath, final String truststorePass,
                                       final int maxPollRecords, final boolean avroKey) {

        this.avroKey = avroKey;
        final Properties consumerProps = loadDeserializers(avroKey);
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("max.poll.records", String.valueOf(maxPollRecords));
        consumerProps.put("schema.registry.url", registryUrl);
        consumerProps.put("specific.avro.reader", true);
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("security.protocol", "SSL");
        consumerProps.put("ssl.keystore.location", keystoreFilePath);
        consumerProps.put("ssl.keystore.password", keystorePass);
        consumerProps.put("ssl.truststore.location", truststoreFilePath);
        consumerProps.put("ssl.truststore.password", truststorePass);

        if (avroKey) {

            avroKeyConsumer = new KafkaConsumer<>(consumerProps);
        } else {

            simpleKeyConsumer = new KafkaConsumer<>(consumerProps);
        }

        avroKeyProducer = null;
        simpleKeyProducer = null;
    }

    public void checkSubscription(final String topic) {

        if (this.avroKey) {

            final Set<String> subs = avroKeyConsumer.subscription();

            if (!subs.contains(topic)) {

                avroKeyConsumer.subscribe(Arrays.asList(topic));
            }
        } else {

            final Set<String> subs = simpleKeyConsumer.subscription();

            if (!subs.contains(topic)) {

                simpleKeyConsumer.subscribe(Arrays.asList(topic));
            }
        }
    }

    public void sendMessage(final Object key, final String topic, final SpecificRecord msg) {

        if (avroKey) {

            final ProducerRecord<SpecificRecordBase, SpecificRecordBase> recordMsg = new ProducerRecord(topic,
                    (SpecificRecord) key, msg);

            LOGGER.log(Level.WARNING, "Sending msg with key " + key + " to topic " + topic);
            avroKeyProducer.send(recordMsg);
            avroKeyProducer.flush();
            LOGGER.log(Level.WARNING, "Sent msg with key " + key + " to topic " + topic);
        } else {

            final ProducerRecord<String, SpecificRecordBase> recordMsg = new ProducerRecord(topic, (String) key, msg);

            LOGGER.log(Level.WARNING, "Sending msg with key " + key + " to topic " + topic);
            simpleKeyProducer.send(recordMsg);
            simpleKeyProducer.flush();
            LOGGER.log(Level.WARNING, "Sent msg with key " + key + " to topic " + topic);
        }
    }

    public void sendMessage(final Object key, final String topic, final SpecificRecordBase msg, final int partition) {

        if (avroKey) {

            final ProducerRecord<SpecificRecordBase, SpecificRecordBase> recordMsg = new ProducerRecord(topic, partition,
                    (SpecificRecordBase) key, msg);

            LOGGER.log(Level.WARNING, "Sending msg with key " + key + " to topic " + topic);
            avroKeyProducer.send(recordMsg);
            avroKeyProducer.flush();
            LOGGER.log(Level.WARNING, "Sent msg with key " + key + " to topic " + topic);
        } else {

            final ProducerRecord<String, SpecificRecordBase> recordMsg = new ProducerRecord(topic, partition,
                    (String) key, msg);

            LOGGER.log(Level.WARNING, "Sending msg with key " + key + " to topic " + topic);
            simpleKeyProducer.send(recordMsg);
            simpleKeyProducer.flush();
            LOGGER.log(Level.WARNING, "Sent msg with key " + key + " to topic " + topic);
        }
    }

    public List<Map<String, Object>> consumeMessage(final String topic) {

        checkSubscription(topic);

        if (avroKey) {

            final ConsumerRecords<SpecificRecordBase, SpecificRecordBase> records = avroKeyConsumer.
                    poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT));

            final List<Map<String, Object>> msgs = new LinkedList<>();

            for (ConsumerRecord<SpecificRecordBase, SpecificRecordBase> record : records) {

                final Map<String, Object> resultMap = new HashMap<>();

                final SpecificRecordBase message = record.value();
                final SpecificRecordBase key = record.key();
                final long resultOffset = record.offset();
                final int partition = record.partition();

                resultMap.put("msg", message);
                resultMap.put("key", key);
                resultMap.put("offset", resultOffset);
                resultMap.put("partition", partition);

                msgs.add(resultMap);
            }

            return msgs;
        } else {

            final ConsumerRecords<String, SpecificRecordBase> records = simpleKeyConsumer.
                    poll(Duration.ofMillis(CONSUMER_POLL_TIMEOUT));

            final List<Map<String, Object>> msgs = new LinkedList<>();

            for (ConsumerRecord<String, SpecificRecordBase> record : records) {

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
    }

    public void closeInterface() {

        if (avroKeyProducer != null) {

            avroKeyProducer.flush();
            avroKeyProducer.close();
        }

        if (simpleKeyProducer != null) {

            simpleKeyProducer.flush();
            simpleKeyProducer.close();
        }

        if (avroKeyConsumer != null) {

            avroKeyConsumer.close();
        }

        if (simpleKeyConsumer != null) {

            simpleKeyConsumer.close();
        }
    }

    public void commitConsumer() {

        if (avroKey) {

            avroKeyConsumer.commitSync();
        } else {

            simpleKeyConsumer.commitSync();
        }
    }
}
