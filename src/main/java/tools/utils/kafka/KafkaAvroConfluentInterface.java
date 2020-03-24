package tools.utils.kafka;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaAvroConfluentInterface<T extends SpecificRecordBase> {

    private static final Logger LOGGER = Logger.getLogger(KafkaAvroConfluentInterface.class.getName());

    private static final long CONSUMER_POLL_TIMEOUT = 6000;

    private final KafkaProducer<String, T> producer;

    private KafkaConsumer<String, T> consumer;

    public KafkaAvroConfluentInterface(final String kafkaUrl, final String registryUrl) {

        final Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaUrl);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        producerProps.put("schema.registry.url", registryUrl);


        producer = new KafkaProducer<>(producerProps);

        consumer = null;
    }

    public KafkaAvroConfluentInterface(final String kafkaUrl, final String groupId, final String registryUrl) {

        final Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", kafkaUrl);
        consumerProps.put("group.id", groupId);
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.commit.interval.ms", "1000");
        consumerProps.put("session.timeout.ms", "30000");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        consumerProps.put("schema.registry.url", registryUrl);
        consumerProps.put("specific.avro.reader", true);
        consumer = new KafkaConsumer<>(consumerProps);

        producer = null;
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

    public Map<String, Object> consumeMessage(final String topic, final int partition, final Long offset) {

        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));

        // Consume the first message from the topic
        if (offset == null) {
            consumer.seekToBeginning(Arrays.asList(topicPartition));

        } else {

            consumer.seek(topicPartition, offset);
        }

        ConsumerRecords<String, T> records = consumer.poll(CONSUMER_POLL_TIMEOUT);

        while (records.isEmpty()) {

            records = consumer.poll(CONSUMER_POLL_TIMEOUT);
            LOGGER.log(Level.INFO, "Trying to poll from topic " + topic);
        }

        final List<ConsumerRecord<String, T>> partitionRecords = records.records(topicPartition);

        final Map<String, Object> resultMap = new HashMap<>();
        try {

            final ConsumerRecord<String, T> record = partitionRecords.get(0);
            final T message = record.value();
            final String key = record.key();
            final long resultOffset = record.offset();

            resultMap.put("msg", message);
            resultMap.put("key", key);
            resultMap.put("offset", resultOffset);

            consumer.unsubscribe();
            return resultMap;
        } catch (IndexOutOfBoundsException ex) {

            LOGGER.log(Level.WARNING, ex.toString(), ex);
            consumer.unsubscribe();
            return null;
        }
    }

    public Map<String, Object> consumeLastMessage(final String topic, final int partition) {

        final TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));

        consumer.seekToEnd(Arrays.asList(topicPartition));
        final Long lastMsgPosition = consumer.position(topicPartition) - 1;

        consumer.unsubscribe();
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

            final ConsumerRecords<String, T> records = consumer.poll(CONSUMER_POLL_TIMEOUT);

            final List<ConsumerRecord<String, T>> partitionRecords = records.records(topicPartition);

            for (final ConsumerRecord<String, T> record : partitionRecords) {

                if (index >= amountMessages) {

                    break;
                }

                final T message = record.value();
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

        consumer.unsubscribe();
        return resultMapList;
    }

    public void closeInterface() {

        if (producer != null) {

            producer.close();
        }
        if (consumer != null) {

            consumer.close();
        }
    }
}
