package com.github.lilmors.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

/**
 * 3 steps when creating producer :
 * 1. Creating producer properties -> https://kafka.apache.org/22/documentation.html#producerconfigs
 * 2. Creating the producer
 * 3. Send the data
 */
public class ProducerDemoWithCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
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
        final ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Hi, I am here");
        producer.send(record, (recordMetadata, e) -> Optional.ofNullable(e).ifPresentOrElse(
                exception -> logger.error("Error while producing", e),
                () -> logger.info(String.format("record for: topic = %s, partition = %d and offset = %d",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()))
        ));
        //Send is async so if we want to make the producer send the data now we need to use te flush command to the close
        // for closing and flushing
        producer.close();
    }

}
