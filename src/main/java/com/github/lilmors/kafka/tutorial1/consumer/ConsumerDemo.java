package com.github.lilmors.kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 4 steps when creating consumer :
 * 1. Creating consumer properties -> https://kafka.apache.org/22/documentation.html#consumerconfigs
 * 2. Creating the consumer
 * 3. Subscribe the consumer to a topic(s)
 * 4. poll for new data
 *
 * ConsumerDemoGroups
 * When adding consumers to a group (By starting a new consumer with the same group id)
 * the ConsumerCoordinator will reassign new partition to every consumer and will log it
 * The same thing happened when a consumer gos down
 */
public class ConsumerDemo {

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "topic1";
    private static final String GROUP_ID = "my-group-id_1";

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        //Creating consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Creating the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe the consumer to a topic
        consumer.subscribe(Collections.singleton(TOPIC_NAME));

        //Poll the data
        //the while loop is only for the example
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> logger.info(String.format("Key = %s, and value = %s", record.key(), record.value())));
        }
    }
}
