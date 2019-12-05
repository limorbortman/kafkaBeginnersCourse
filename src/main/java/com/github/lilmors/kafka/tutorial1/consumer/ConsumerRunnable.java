package com.github.lilmors.kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable {

    private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

    private CountDownLatch latch;
    KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(CountDownLatch countDownLatch,
                            String bootstrapServer,
                            String topic,
                            String groupId) {
        this.latch = countDownLatch;

        //Creating consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Creating the consumer
        consumer = new KafkaConsumer<>(properties);

        //Subscribe the consumer to a topic
        consumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public void run() {
        try {
            //Poll the data
            //the while loop is only for the example
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> logger.info(String.format("Key = %s, and value = %s", record.key(), record.value())));
            }
        } catch (WakeupException we) {
            logger.info("Received shutdown... bay");

        } finally {
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown() {
        //Stop the consumer.poll it will throw wakeUpException
        consumer.wakeup();
    }
}
