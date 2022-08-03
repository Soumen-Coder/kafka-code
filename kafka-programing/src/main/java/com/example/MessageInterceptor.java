package com.example;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class MessageInterceptor implements ProducerInterceptor<String,Message> {

    static AtomicLong numSent = new AtomicLong(0);
    static AtomicLong numAcked = new AtomicLong(0);

    @Override
    public ProducerRecord<String,Message> onSend(ProducerRecord<String,Message> producerRecord) {
        System.out.println("onSend()");
        numSent.incrementAndGet();
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        System.out.println("onAcknowledgement()");
        numAcked.incrementAndGet();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
