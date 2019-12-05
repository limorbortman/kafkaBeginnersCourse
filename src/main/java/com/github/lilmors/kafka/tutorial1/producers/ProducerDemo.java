package com.github.lilmors.kafka.tutorial1.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 3 steps when creating producer :
 * 1. Creating producer properties -> https://kafka.apache.org/22/documentation.html#producerconfigs
 * 2. Creating the producer
 * 3. Send the data
 */
public class ProducerDemo {

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
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Hi, I am here");
        producer.send(record);
        //Send is async so if we want to make the producer send the data now we need to use te flush command to the close
        // for closing and flushing
        producer.close();
    }

}
