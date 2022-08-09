package com.example.producer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class Transactional {

    public static void main(String[] args) {

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txn-id-1");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "G1");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton("demo-topic"));
        producer.beginTransaction();
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                if (records.count() > 0) {
                    producer.beginTransaction();
                    for (ConsumerRecord<String, String> record : records) {
                        ProducerRecord<String, String> customizedRecord = transform(record);
                        producer.send(customizedRecord); // partition-write
                    }
                    Map<TopicPartition, OffsetAndMetadata> offsets = consumerOffsets();
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata()); // partion-write __consumer_offsets
                    producer.commitTransaction();
                }
            } catch (ProducerFencedException | InvalidProducerEpochException e) {
                throw new KafkaException(String.format(
                        "The transactional.id %s is used by another process", "transactionalId"));
            } catch (KafkaException e) {
                producer.abortTransaction();
                resetToLastCommittedPositions(consumer);
            }
        }

    }

    private static void resetToLastCommittedPositions(KafkaConsumer<String, String> consumer) {
    }

    private static Map<TopicPartition, OffsetAndMetadata> consumerOffsets() {
        return null;
    }

    private static ProducerRecord<String, String> transform(ConsumerRecord<String, String> record) {
        return null;
    }

}
