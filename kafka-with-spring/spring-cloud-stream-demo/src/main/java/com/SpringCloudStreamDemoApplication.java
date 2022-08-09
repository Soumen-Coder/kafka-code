package com;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SpringBootApplication
public class SpringCloudStreamDemoApplication {


//    @Bean
//    public Consumer<String> consumer() {
//        return message -> {
//            System.out.println("message received : " + message);
//        };
//    }

//    @Bean
//    public Supplier<String> producer() {
//        return () -> {
//            int i = 0;
//            while (true) {
//                i++;
//                return "hello-" + i;
//            }
//        };
//    }


    @Bean
    public Function<String, Integer> function() {
        return messsage -> {
            //..
            System.out.println("Received Message - " + messsage);
            return messsage.length();
        };
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringCloudStreamDemoApplication.class, args);
    }

}
