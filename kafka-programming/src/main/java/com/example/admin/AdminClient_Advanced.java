package com.example.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClient_Advanced {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(props);

        // Adding Partitions to a Topic

//        String TOPIC_NAME = "demo";
//        int NUM_PARTITIONS = 1;
//
//        Map<String, NewPartitions> newPartitions = new HashMap<>();
//        newPartitions.put(TOPIC_NAME, NewPartitions.increaseTo(NUM_PARTITIONS + 2));
//        admin.createPartitions(newPartitions).all().get();
//
//        admin.close(Duration.ofSeconds(30));

        //Deleting Records from a Topic

//        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> olderOffsets = admin.listOffsets(requestOlderOffsets).all().get();
//        Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
//        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e:
//                olderOffsets.entrySet())
//            recordsToDelete.put(e.getKey(),
//                    RecordsToDelete.beforeOffset(e.getValue().offset()));
//        admin.deleteRecords(recordsToDelete).all().get();

    }

}
