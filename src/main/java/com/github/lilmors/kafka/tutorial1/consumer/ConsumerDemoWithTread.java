package com.github.lilmors.kafka.tutorial1.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * 4 steps when creating consumer :
 * 1. Creating consumer properties -> https://kafka.apache.org/22/documentation.html#consumerconfigs
 * 2. Creating the consumer
 * 3. Subscribe the consumer to a topic(s)
 * 4. poll for new data
 * <p>
 * ConsumerDemoGroups
 * When adding consumers to a group (By starting a new consumer with the same group id)
 * the ConsumerCoordinator will reassign new partition to every consumer and will log it
 * The same thing happened when a consumer gos down
 */
public class ConsumerDemoWithTread {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithTread.class);

    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC_NAME = "topic1";
    private static final String GROUP_ID = "my-group-with-threads-id";

    public static void main(String[] args) {

        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating consumerRunnable...");
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(latch, BOOTSTRAP_SERVER, TOPIC_NAME, GROUP_ID);
        Thread myThread = new Thread(consumerRunnable);
        myThread.start();

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook...");
            consumerRunnable.shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("Exception was thrown", e);
            } finally {
                logger.info("Application is exit...");
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Exception was thrown", e);
        } finally {
            logger.info("Closing application...");
        }


    }
}
