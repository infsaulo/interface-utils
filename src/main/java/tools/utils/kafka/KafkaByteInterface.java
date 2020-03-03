package tools.utils.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaByteInterface {

    private static final Logger LOGGER = Logger.getLogger(KafkaByteInterface.class.getName());

    private static final long CONSUMER_POLL_TIMEOUT = 6000;

    private final KafkaProducer<String, byte[]> producer;

    private KafkaConsumer<String, byte[]> consumer;

    public KafkaByteInterface(final String kafkaUrl) {

        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaUrl);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<>(producerProps);

        consumer = null;
    }

    public KafkaByteInterface(final String kafkaUrl, final String groupId) {

        this(kafkaUrl);

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumer = new KafkaConsumer<>(consumerProps);
    }

    public void sendMessage(final String key, final String topic, final byte[] msg) {

        final ProducerRecord<String, byte[]> recordMsg = new ProducerRecord<>(topic, key, msg);

        producer.send(recordMsg);
        producer.flush();
    }

    public void sendMessage(final String key, final String topic, final byte[] msg, final int partition) {

        final ProducerRecord<String, byte[]> recordMsg = new ProducerRecord<>(topic, partition, key, msg);

        producer.send(recordMsg);
        producer.flush();
    }

    public Map<String, Object> consumeMessage(final String topic, final int partition, final Long offset) {

        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));

        // Consume the first message from the topic
        if (offset == null) {
            consumer.seekToBeginning(Arrays.asList(topicPartition));

        } else {

            consumer.seek(topicPartition, offset);
        }

        final ConsumerRecords<String, byte[]> records = consumer.poll(CONSUMER_POLL_TIMEOUT);

        final List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(topicPartition);

        final Map<String, Object> resultMap = new HashMap<>();
        try {

            final ConsumerRecord<String, byte[]> record = partitionRecords.get(0);
            final byte[] message = record.value();
            final String key = record.key();
            final long resultOffset = record.offset();

            resultMap.put("msg", message);
            resultMap.put("key", key);
            resultMap.put("offset", resultOffset);

            return resultMap;
        } catch (IndexOutOfBoundsException ex) {

            LOGGER.log(Level.WARNING, ex.toString(), ex);
            return null;
        }
    }

    public Map<String, Object> consumeLastMessage(final String topic, final int partition) {

        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));

        consumer.seekToEnd(Arrays.asList(topicPartition));
        final Long lastMsgPosition = consumer.position(topicPartition) - 1;

        return this.consumeMessage(topic, partition, lastMsgPosition);
    }

    public List<Map<String, Object>> consumeMessages(final String topic, final int partition, final Long offset,
                                                     final Long amountMessages) {

        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));

        // Consume the first message from the topic
        if (offset == null) {

            consumer.seekToBeginning(Arrays.asList(topicPartition));
        } else {

            consumer.seek(topicPartition, offset);
        }

        final List<Map<String, Object>> resultMapList = new ArrayList<>();
        int index = 0;

        while (index < amountMessages) {

            final ConsumerRecords<String, byte[]> records = consumer.poll(CONSUMER_POLL_TIMEOUT);

            final List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(topicPartition);

            for (final ConsumerRecord<String, byte[]> record : partitionRecords) {

                if (index >= amountMessages) {

                    break;
                }

                final byte[] message = record.value();
                final String key = record.key();
                final long resultOffset = record.offset();

                final Map<String, Object> resultMap = new HashMap<>();
                resultMap.put("msg", message);
                resultMap.put("key", key);
                resultMap.put("offset", resultOffset);

                resultMapList.add(resultMap);

                index++;
            }
        }

        return resultMapList;
    }

    public void closeInterface() {

        producer.close();
        consumer.close();
    }
}
