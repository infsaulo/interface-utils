package tools.utils.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.logging.Logger;

public class KafkaInterface {

  private static final Logger LOGGER = Logger.getLogger(KafkaInterface.class.getName());

  final KafkaProducer<String, String> producer;

  public KafkaInterface(String kafkaUrlProducer) {

    Properties props = new Properties();
    props.put("bootstrap.servers", kafkaUrlProducer);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    producer = new KafkaProducer<>(props);
  }

  public void sendMessage(final String key, final String topic, final String msg){

    final ProducerRecord<String, String> recordMsg = new ProducerRecord<>(topic, key, msg);

    producer.send(recordMsg);
    producer.flush();
  }
}
