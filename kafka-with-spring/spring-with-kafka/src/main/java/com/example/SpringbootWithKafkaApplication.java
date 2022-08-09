package com.example;

import com.github.javafaker.Faker;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.stream.Stream;

@SpringBootApplication
@EnableKafka
public class SpringbootWithKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootWithKafkaApplication.class, args);
    }


    @Component
    @RequiredArgsConstructor
    class Producer {

        private final KafkaTemplate<String, String> kafkaTemplate;
        Faker faker;

        @EventListener(ApplicationStartedEvent.class)
        public void generate() {
            faker = Faker.instance();
            final Flux<Long> intervalStream = Flux.interval(Duration.ofMillis(1_000));
            final Flux<String> quotesStream = Flux.fromStream(Stream.generate(() -> faker.hobbit().quote()));
            Flux.zip(intervalStream, quotesStream)
                    .map(it -> kafkaTemplate.send("hobbit", Integer.toString(faker.random().nextInt(42)), it.getT2())).blockLast();
        }

    }


    @Slf4j
    @Component
    static class Consumer {
        @KafkaListener(
                topics = {"hobbit"},
                groupId = "spring-group"
        )
        public void consume(String quote) {
            log.info("received= " + quote);
        }
    }

}
