package com.tvc.kafka.producer;

public class AppConfigs {
    public final static String applicationID = "SimpleProducer";
    public final static String bootstrapServers = "localhost:9092,localhost:9093";
    public final static String input_topicName = "streams-input-topic";
    public final static String output_topicName = "streams-output-topic";
    public final static int numEvents = 1000000;
}
