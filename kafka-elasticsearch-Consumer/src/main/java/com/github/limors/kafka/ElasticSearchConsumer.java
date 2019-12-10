package com.github.limors.kafka;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.StreamSupport;

public class ElasticSearchConsumer implements Runnable {

    private Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "Twitter_ex";
    private static final String GROUP_ID = "my-ES-consumer-id-2";

    private static final String ELASTIC_SEARCH_HOSTNAME = "poc-for-kafka-5282602666.ap-southeast-2.bonsaisearch.net";
    private static final int ELASTIC_SEARCH_PORT = 443;
    private static final String USER_NAME = "t6iwrpcd9u";
    private static final String PASSWORD = "u256a2oefu";

    private CountDownLatch latch;
    private final KafkaConsumer<String, String> consumer;

    private final RestHighLevelClient elasticSearchClient;


    public ElasticSearchConsumer(CountDownLatch countDownLatch) {
        this.latch = countDownLatch;

        consumer = createKafkaConsumer();

        elasticSearchClient = createElasticSearchClient();
    }

    @Override
    public void run() {
        try (consumer; elasticSearchClient) {

            addShutdownHook();

            //Poll the data
            //the while loop is only for the example
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    logger.info(String.format("Received %d records", records.count()));
                    BulkRequest bulkRequest = new BulkRequest();
                    StreamSupport.stream(records.spliterator(), false)
                            .map(this::toIndexRequest)
                            .forEach(bulkRequest::add);

                    BulkResponse bulkResponse = elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info(String.format("Response status is %s", bulkResponse.status()));

                    //Commit offset to kafka
                    logger.info("Commit offsets....");
                    consumer.commitSync();
                    logger.info("Offsets have been committed");
                }
            }
        } catch (WakeupException we) {
            logger.info("Received shutdown... bay");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook...");
            shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Exception was thrown", e);
            } finally {
                logger.info("Application is exit...");
            }
        }));
    }

    private IndexRequest toIndexRequest(ConsumerRecord<String, String> record) {
        //Because we are using consumer conf of at least 1 in a case of failure we can read the same data 2 times.
        //So we need to make the consumer idempotence. We can do this by adding ID to the ES index request
        //We can create the ID using kafka metadata or twitter
        String id = String.format("%s_%s_%s", record.topic(), record.partition(), record.offset());
        return new IndexRequest("twitter", "twits", id).source(record.value(), XContentType.JSON);
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        //Creating consumer properties
        //at least 1
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Update the offset of the partition manually and not automatically after 5 sec
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // poll only 10 records at a time

        //Creating the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Subscribe the consumer to a topic
        consumer.subscribe(Collections.singleton(TOPIC_NAME));
        return consumer;
    }

    private RestHighLevelClient createElasticSearchClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(ELASTIC_SEARCH_HOSTNAME, ELASTIC_SEARCH_PORT, "https"))
                        .setHttpClientConfigCallback(createClintConfigCallback())
        );
    }

    private RestClientBuilder.HttpClientConfigCallback createClintConfigCallback() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USER_NAME, PASSWORD));

        return httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    }

    private void shutdown() {
        //Stop the consumer.poll it will throw wakeUpException
        consumer.wakeup();
    }
}
