package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MessageStandaloneConsumer {

    private static final Logger log = LoggerFactory.getLogger("consumer");

    public static void main(String[] args) {


        log.info("I am a Kafka Consumer");

        String bootstrapServers = "127.0.0.1:9092";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"SG1");
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        try {

            List<PartitionInfo> partitionInfos = null;
            partitionInfos = consumer.partitionsFor("greetings");
            List<TopicPartition> partitions = new ArrayList<>();
            if (partitionInfos != null) {

                for (PartitionInfo partition : partitionInfos)
                    partitions.add(new TopicPartition(partition.topic(), partition.partition()));

                consumer.assign(partitions);

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(100));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                    consumer.commitSync();
                }
            }


        } catch (WakeupException e) {
            log.info("Wake up exception!");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            try {
                consumer.commitSync(); // we must commit the offsets synchronously here
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
