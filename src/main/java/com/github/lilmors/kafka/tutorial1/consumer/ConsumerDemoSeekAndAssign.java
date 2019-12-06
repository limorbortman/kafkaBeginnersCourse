package com.github.lilmors.kafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

/**
 * 3 steps when creating consumer seek and assign:
 * 1. Creating consumer properties -> https://kafka.apache.org/22/documentation.html#consumerconfigs
 * 2. Creating the consumer
 * 3. poll for new data
 *
 * We don't subscribe to a topic
 * We use this consumer to replay data and look for a specific message
 *
 * Assign :
 * However, in some cases you may need finer control over the specific partitions that are assigned. For example:
 * 1. If the process is maintaining some kind of local state associated with that partition (like a local on-disk key-value store),
 * then it should only get records for the partition it is maintaining on disk.
 * 2. If the process itself is highly available and will be restarted if it fails
 * (perhaps using a cluster management framework like YARN, Mesos, or AWS facilities, or as part of a stream processing framework).
 * In this case there is no need for Kafka to detect the failure and reassign the partition since the consuming
 * process will be restarted on another machine.
 *
 *
 * Seek:
 * Overrides the fetch offsets that the consumer will use on the next poll(timeout).
 * If this API is invoked for the same partition more than once, the latest offset will be used on the next poll().
 * Note that you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets
 */
public class ConsumerDemoSeekAndAssign {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoSeekAndAssign.class);

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "topic1";

    public static void main(String[] args) {

        //Creating consumer properties
        //In this case we don't have group id
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Creating the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        TopicPartition topicPartition = new TopicPartition(TOPIC_NAME, 0);

        //assign
        consumer.assign(Collections.singleton(topicPartition)); //will work without the seek and will read only from partition 0

        //seek
        consumer.seek(topicPartition, 15L); //When not using the assign API the next exception is thrown IllegalStateException: No current assignment for partition topic1-0

        //Poll the data
        IntStream.rangeClosed(0, 5).forEach(i -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> logger.info(String.format("Key = %s, value = %s, partition = %d and offset = %d",
                    record.key(), record.value(), record.partition(), record.offset())));
        });

        consumer.close();
    }
}
