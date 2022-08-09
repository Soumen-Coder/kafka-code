package com.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Transactional {

    public static void main(String[] args) {

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txn-id-1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        try {
            producer.beginTransaction();
            // write message - p1

            // something bad happen
            // write message - p2

            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
        }

    }

}
