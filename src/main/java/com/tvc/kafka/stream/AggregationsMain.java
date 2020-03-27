package com.tvc.kafka.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class AggregationsMain {

    public static void main(String args[]){
        Properties props= new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder= new StreamsBuilder();
        KStream<String,String> aggregation= builder.stream("aggregations-input-topic");
        KGroupedStream<String,String> kGroupedStream=aggregation.groupByKey();
        // Create an aggregation that totals the length in characters of the value for all records sharing the same key.
        KTable<String, Integer>
            aggregatedTable = kGroupedStream.aggregate(()->0,(aggKey,newValue,aggrValue)->aggrValue+newValue.length(),
            Materialized.with(Serdes.String(), Serdes.Integer()));

        // Count the number of records for each key.
        KTable<String, Long> countedTable = kGroupedStream.count(Materialized.with(Serdes.String(), Serdes.Long()));

        countedTable.toStream().to("aggregations-output-count-topic", Produced
            .with(Serdes.String(), Serdes.Long()));

        // Combine the values of all records with the same key into a string separated by spaces.
        KTable<String, String> reducedTable = kGroupedStream.reduce((aggValue, newValue) -> aggValue + " " + newValue);
        reducedTable.toStream().to("aggregations-output-reduce-topic");

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        // Print the topology to the console.
        System.out.println(topology.describe());
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach a shutdown handler to catch control-c and terminate the application gracefully.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }

    }

