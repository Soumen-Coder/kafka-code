package com.example.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminClient_ConfigurationManagement {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(props);

        String TOPIC_NAME = "demo";

        ConfigResource configResource =
                new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);

        DescribeConfigsResult configsResult =
                admin.describeConfigs(Collections.singleton(configResource));
        Config configs = configsResult.all().get().get(configResource);
// print nondefault configs
        configs.entries().stream().filter(entry -> !entry.isDefault()).forEach(System.out::println);

// Check if topic is compacted
        ConfigEntry compaction = new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        if (!configs.entries().contains(compaction)) {
            // if topic is not compacted, compact it
            Collection<AlterConfigOp> configOp = new ArrayList<AlterConfigOp>();
            configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET));
            Map<ConfigResource, Collection<AlterConfigOp>> alterConf = new HashMap<>();
            alterConf.put(configResource, configOp);
            admin.incrementalAlterConfigs(alterConf).all().get();
        } else {
            System.out.println("Topic " + TOPIC_NAME + " is compacted topic");
        }


        admin.close(Duration.ofSeconds(30));
    }
}
