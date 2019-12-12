package com.github.limors.kafka.tutorial2;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * To create Kafka Stream app we need to do 4 steps :
 * 1. Create Properties
 * 2. Create topology
 * 3. Build the topology from 2
 * 4. Start stream application
 */
public class StreamFilterTweets {

    private static final Logger logger = LoggerFactory.getLogger(StreamFilterTweets.class);

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String APP_ID = "kafka_stream";
    private static final String INPUT_NAME = "Twitter_ex";
    private static final String OUTPUT_TOPIC = "Important_tweets";

    public static void main(String[] args) {

        //Create Properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        //Create topology
        StreamsBuilder streamsBuilder = CreateTopology();

        //Build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        //Start Application
        kafkaStreams.start();
    }

    private static StreamsBuilder CreateTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
                .<String, String>stream(INPUT_NAME)
                .filter((key, jsonTweets) -> isMoreThenThousandFollowers(jsonTweets))
                .to(OUTPUT_TOPIC);
        return streamsBuilder;
    }

    private static boolean isMoreThenThousandFollowers(String jsonTweets) {
        try {
            return JsonParser.parseString(jsonTweets)
                    .getAsJsonObject()
                    .get("user").getAsJsonObject()
                    .get("followers_count")
                    .getAsInt() > 1000;
        } catch (NullPointerException e) {
            logger.error(String.format("No user.followers_count in tweet \n %s \n", jsonTweets));
            return false;
        }
    }
}
