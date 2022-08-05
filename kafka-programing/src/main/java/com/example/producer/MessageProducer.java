package com.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class MessageProducer {

    private static final Logger log = LoggerFactory.getLogger("producer");

    public static void main(String[] args) throws InterruptedException {

        log.info("I am a Kafka Producer");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String topic = "greetings";

        int i = 0;
        while (i<50) {
            i++;
            String key = "id_" + Integer.toString(i);
            String message = "hello world - " + i; // p-0
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message);
            producerRecord.headers().add("privacy-level", "YOLO".getBytes(StandardCharsets.UTF_8));
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
            TimeUnit.MILLISECONDS.sleep(1);
        }

//        producer.flush();
//        producer.close();


    }
}
