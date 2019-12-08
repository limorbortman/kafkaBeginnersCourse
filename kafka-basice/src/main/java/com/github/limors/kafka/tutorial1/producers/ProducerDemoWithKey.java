package com.github.limors.kafka.tutorial1.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

/**
 * 3 steps when creating producer :
 * 1. Creating producer properties -> https://kafka.apache.org/22/documentation.html#producerconfigs
 * 2. Creating the producer
 * 3. Send the data
 */
public class ProducerDemoWithKey {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class);
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "topic1";


    public static void main(String[] args) {
        //Creating producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creating the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Send the data
        IntStream.rangeClosed(0, 10).forEach(i -> {
            String key = String.format("id_%d", i);
            String value = String.format("Hi, I am here %d", i);
            String value2 = "I am going to the same partition";

            final ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
            final ProducerRecord<String, String> record2 = new ProducerRecord<>(TOPIC_NAME, key, value2);
            sendToKafka(producer, record);
            sendToKafka(producer, record2); // This is only to show that the same key will go to the same partition
        });

        //Send is async so if we want to make the producer send the data now we need to use te flush command to the close
        // f"I am going to the same partition"or closing and flushing
        producer.close();
    }

    private static void sendToKafka(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        producer.send(record, (recordMetadata, e) -> Optional.ofNullable(e).ifPresentOrElse(
                exception -> logger.error("Error while producing", e),
                () -> {
                    logger.info(String.format("record for: topic = %s, partition = %d and offset = %d and key = %s",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), record.key()));
                }
        ));
    }

}
