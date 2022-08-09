package com.example.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClient_TopicManagement_Describe {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(props);

        DescribeTopicsResult demoTopic = admin.describeTopics(List.of("demo"));


        try {

            TopicDescription topicDescription = demoTopic.values().get("demo").get();
            System.out.println("Description of demo topic:" + topicDescription);

            if (topicDescription.partitions().size() != 3) {
                System.out.println("Topic has wrong number of partitions. Exiting.");
                System.exit(-1);
            }

        } catch (ExecutionException | InterruptedException e) {
            // exit early for almost all exceptions
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                e.printStackTrace();
                throw e;
            }
            // if we are here, topic doesn't exist
            System.out.println("Topic " + "demo" + " does not exist. Going to create it now");
            // Note that number of partitions and replicas is optional. If they are not specified, the defaults configured on the Kafka brokers will be used
            CreateTopicsResult newTopic = admin.createTopics(Collections.singletonList(new NewTopic("demo", 1, (short) 1)));
            // Check that the topic was created correctly:
            if (newTopic.numPartitions("demo").get() != 3) {
                System.out.println("Topic has wrong number of partitions.");
                System.exit(-1);
            }
        }

        admin.close(Duration.ofSeconds(30));

    }
}
