package com.example.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownMemberIdException;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClient_ConsumerGroup {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(props);

//        Exploring Consumer Groups

//        admin.listConsumerGroups().valid().get().forEach(System.out::println);
//
        String CONSUMER_GROUP = "G1";

//
//        ConsumerGroupDescription groupDescription = admin
//                .describeConsumerGroups(List.of(CONSUMER_GROUP))
//                .describedGroups().get(CONSUMER_GROUP).get();
//        System.out.println("Description of group " + CONSUMER_GROUP + ":" + groupDescription);



        Map<TopicPartition, OffsetAndMetadata> offsets =
                admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                        .partitionsToOffsetAndMetadata().get();
        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        for (TopicPartition tp : offsets.keySet()) {
            requestLatestOffsets.put(tp, OffsetSpec.latest());
        }

//        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = admin.listOffsets(requestLatestOffsets).all().get();
//        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
//            String topic = e.getKey().topic();
//            int partition = e.getKey().partition();
//            long committedOffset = e.getValue().offset();
//            long latestOffset = latestOffsets.get(e.getKey()).offset();
//            System.out.println("Consumer group " + CONSUMER_GROUP
//                    + " has committed offset " + committedOffset
//                    + " to topic " + topic + " partition " + partition
//                    + ". The latest offset in the partition is "
//                    + latestOffset + " so consumer group is "
//                    + (latestOffset - committedOffset) + " records behind");
//        }

        // Modifying Consumer Groups
//
//        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets = admin.listOffsets(requestLatestOffsets).all().get();
//        Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
//        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e :
//                earliestOffsets.entrySet()) {
//            resetOffsets.put(e.getKey(), new OffsetAndMetadata(e.getValue().offset()));
//        }
//        try {
//            admin.alterConsumerGroupOffsets(CONSUMER_GROUP, resetOffsets).all().get();
//        } catch (ExecutionException e) {
//            System.out.println("Failed to update the offsets committed by group " + CONSUMER_GROUP + " with error " + e.getMessage());
//            if (e.getCause() instanceof UnknownMemberIdException)
//                System.out.println("Check if consumer group is still active.");
//        }


        admin.close(Duration.ofSeconds(30));
    }
}
