package com.example.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClient_TopicManagement_Delete {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(props);

        String TOPIC_NAME = "demo";

        admin.deleteTopics(List.of(TOPIC_NAME)).all().get();
// Check that it is gone. Note that due to the async nature of deletes,
// it is possible that at this point the topic still exists
        try {
            DescribeTopicsResult demoTopic = admin.describeTopics(List.of("TOPIC_NAME"));
            TopicDescription topicDescription = demoTopic.values().get(TOPIC_NAME).get();
            System.out.println("Topic " + TOPIC_NAME + " is still around");
        } catch (ExecutionException e) {
            System.out.println("Topic " + TOPIC_NAME + " is gone");
        }


        admin.close(Duration.ofSeconds(30));

    }

}
