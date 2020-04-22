package tools.utils.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.logging.Logger;

public class KafkaInterface {

    private static final Logger LOGGER = Logger.getLogger(KafkaInterface.class.getName());

    private static final long CONSUMER_POLL_TIMEOUT = 6000;

    private final KafkaProducer<String, String> producer;

    private KafkaConsumer<String, String> consumer;

    public KafkaInterface(final String kafkaUrl) {

        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaUrl);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(producerProps);

        consumer = null;
    }

    public KafkaInterface(final String kafkaUrl, final String keystoreFilePath, final String keystorePass,
                          final String truststoreFilePath, final String truststorePass) {

        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaUrl);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("security.protocol", "SSL");
        producerProps.put("ssl.keystore.location", keystoreFilePath);
        producerProps.put("ssl.keystore.password", keystorePass);
        producerProps.put("ssl.truststore.location", truststoreFilePath);
        producerProps.put("ssl.truststore.password", truststorePass);

        producer = new KafkaProducer<>(producerProps);

        consumer = null;
    }

    public KafkaInterface(final String kafkaUrl, final String groupId) {

        this(kafkaUrl);

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("max.poll.records", "1");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(consumerProps);
    }

    public KafkaInterface(final String kafkaUrl, final String groupId, final String keystoreFilePath,
                          final String keystorePass, final String truststoreFilePath, final String truststorePass) {

        this(kafkaUrl);

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("max.poll.records", "1");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("security.protocol", "SSL");
        consumerProps.put("ssl.keystore.location", keystoreFilePath);
        consumerProps.put("ssl.keystore.password", keystorePass);
        consumerProps.put("ssl.truststore.location", truststoreFilePath);
        consumerProps.put("ssl.truststore.password", truststorePass);

        consumer = new KafkaConsumer<>(consumerProps);
    }

    public void sendMessage(final String key, final String topic, final String msg) {

        final ProducerRecord<String, String> recordMsg = new ProducerRecord<>(topic, key, msg);

        producer.send(recordMsg);
        producer.flush();
    }

    public void sendMessage(final String key, final String topic, final String msg, final int partition) {

        final ProducerRecord<String, String> recordMsg = new ProducerRecord<>(topic, partition, key, msg);

        producer.send(recordMsg);
        producer.flush();
    }

    public void checkSubscription(final String topic, final int amountPartitions) {

        final Set<String> subs = consumer.subscription();

        if (!subs.contains(topic)) {

            consumer.subscribe(Arrays.asList(topic));
        }
    }

    public List<Map<String, Object>> consumeMessage(final String topic, final int amountPartitions) {

        checkSubscription(topic, amountPartitions);

        final ConsumerRecords<String, String> records = consumer.poll(CONSUMER_POLL_TIMEOUT);

        final List<Map<String, Object>> msgs = new LinkedList<>();

        for (ConsumerRecord<String, String> record : records) {

            final Map<String, Object> resultMap = new HashMap<>();

            final String message = record.value();
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
