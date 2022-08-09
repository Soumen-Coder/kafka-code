package com.example.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class MessageConsumer {
    private static final Logger log = LoggerFactory.getLogger("consumer");

    public static void main(String[] args) {

        log.info("I am a Kafka Consumer");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "G1";
        String topic = "demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topic(s)
        MessageConsumerRebalanceListener listener = new MessageConsumerRebalanceListener(consumer);
        consumer.subscribe(List.of(topic), listener);

        try {
            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                log.info("Received records count " + records.count());
                int i = 0;

                for (ConsumerRecord<String, String> record : records) {
                    i++;
                    log.info("Key: " + record.key() + ", Value: " + record.value() + " Topic: " + record.topic() + " Partition: " + record.partition() + ", Offset:" + record.offset());
                    listener.addOffsetToTrack(record.topic(), record.partition(), record.offset());
                    if (i % 10 == 0) {
                        consumer.commitSync(listener.getCurrentOffsets());
                    }
                }
                consumer.commitSync();
//                consumer.commitAsync();
            }

        } catch (WakeupException e) {
            log.info("Wake up exception!");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            try {
                consumer.commitSync(listener.getCurrentOffsets()); // we must commit the offsets synchronously here
            } finally {
                consumer.close(); // this will also commit the offsets if need be.
            }
            log.info("The consumer is now gracefully closed.");
        }

        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();
        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();
                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

}
