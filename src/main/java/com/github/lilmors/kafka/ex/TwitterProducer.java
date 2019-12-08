package com.github.lilmors.kafka.ex;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private static final String CONSUMER_KEY = "*****";
    private static final String CONSUMER_SECRET = "*****";
    private static final String TOKEN = "*******";
    private static final String SECRET = "******";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "Twitter_ex";
    private static final ArrayList<String> TERMS = Lists.newArrayList("kafka");
    private static final String COMPRESSION_TYPE = "snappy";
    private static final String LINGER_MS = "20";
    private static final String BATCH_SIZE = Integer.toString(32 * 1024); //32KB

    private final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
    private final BasicClient twitterClient = createTwitterClient(msgQueue);
    private final KafkaProducer<String, String> kafkaProducer = createKafkaProducer();


    public TwitterProducer() {
        logger.info("Creating Twitter client");
        twitterClient.connect();
    }

    public void run() {
        addShutdownHook(twitterClient, kafkaProducer);

        try (kafkaProducer) {
            while (!twitterClient.isDone()) {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                Optional.ofNullable(msg).ifPresent(message -> sendMessageToKafka(kafkaProducer, message));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            twitterClient.stop();
        }

        logger.info("End of app");
    }

    private void sendMessageToKafka(KafkaProducer<String, String> kafkaProducer, String message) {
        logger.info(String.format("Sending the message is %s to kafka", message));
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, message);
        kafkaProducer.send(record, (recordMetadata, e) -> Optional.ofNullable(e).ifPresentOrElse(
                exception -> logger.error("Error while producing", e),
                () -> logger.info(String.format("record for: topic = %s, partition = %d and offset = %d",
                        recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset()))
        ));
    }

    private void addShutdownHook(BasicClient twitterClient, KafkaProducer<String, String> kafkaProducer) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown...");
            logger.info("Close down twitter client...");
            twitterClient.stop();
            logger.info("Close down Kafka producer...");
            kafkaProducer.close();
        }));
    }

    private BasicClient createTwitterClient(BlockingQueue<String> msgQueue) {
        //** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(TERMS);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(new HttpHosts(Constants.STREAM_HOST))
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //add safe retry without duplication of data
        //With this the default properties are:
        //acks = 1
        //retry = MAX_INT
        //MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 5 (fro kafka  >= 2.0)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        //High throughput producer (at the expense of latency and CPU)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE);
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS);
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, BATCH_SIZE);

        return new KafkaProducer<>(properties);
    }
}
