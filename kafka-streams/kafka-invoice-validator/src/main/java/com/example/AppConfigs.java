package com.example;

public class AppConfigs {
    public final static String applicationID = "InvoiceValidator";
    public final static String bootstrapServers = "localhost:9092";
    public final static String groupID = "InvoiceValidatorGroup";
    public final static String[] sourceTopicNames = {"pos"};
    public final static String validTopicName = "valid-invoice";
    public final static String invalidTopicName = "invalid-invoice";
}
