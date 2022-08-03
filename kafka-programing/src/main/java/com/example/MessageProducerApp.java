package com.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class MessageProducerApp {

    private static final Logger log = LoggerFactory.getLogger("kp");

    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka Producer");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG,MessagePartitioner.class.getName());
        properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,MessageInterceptor.class.getName());
        KafkaProducer<String, Message> producer = new KafkaProducer<String, Message>(properties);

        String topic = "greetings";

        String key = "id_" + Integer.toString(0);
        Message message = new Message("hello-world - " + 0);
        ProducerRecord<String, Message> producerRecord = new ProducerRecord<>(topic, key, message);
        producerRecord.headers().add("privacy-level","YOLO".getBytes(StandardCharsets.UTF_8));

        producer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                // the record was successfully sent
                log.info("Received new metadata. \n" +
                        "Topic:" + recordMetadata.topic() + "\n" +
                        "Key:" + producerRecord.key() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp());
            } else {
                log.error("Error while producing", e);
            }
        });

        producer.flush();
        producer.close();


    }
}
