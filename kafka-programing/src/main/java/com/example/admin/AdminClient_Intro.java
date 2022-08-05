package com.example.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.time.Duration;
import java.util.Properties;

public class AdminClient_Intro {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(props);



        admin.close(Duration.ofSeconds(30));

    }
}
