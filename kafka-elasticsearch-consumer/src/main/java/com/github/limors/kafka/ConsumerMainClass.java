package com.github.limors.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerMainClass {

    private static Logger logger = LoggerFactory.getLogger(ConsumerMainClass.class);

    public static void main(String[] args) {
        new ElasticSearchConsumer(new CountDownLatch(1)).run();

    }
}
