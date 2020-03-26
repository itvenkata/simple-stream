package com.tvc.kafka.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class StatelessTransformationsMain {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String[] args){
        logger.info("Start Consumer");
        Properties props= new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder= new StreamsBuilder();
        KStream<String,String> KS0=builder.stream(AppConfigs.stateless_transformations_input_topic);

        KStream<String, String>[] branches= KS0.branch((k,v)->k.startsWith("a"),(k,v)->true);
        KStream<String, String> aKeysStream = branches[0];
        KStream<String, String> othersStream = branches[1];

        // Remove any records from the "a" stream where the value does not also start with "a".
        aKeysStream = aKeysStream.filter((key, value) -> value.startsWith("a"));

         // For the "a" stream, convert each record into two records, one with an uppercased value and one with a lowercased value.
            aKeysStream = aKeysStream.flatMap((key, value) -> {
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, value.toUpperCase()));
            result.add(KeyValue.pair(key, value.toLowerCase()));
            return result;
        });

        // For the "a" stream, modify all records by uppercasing the key.
        aKeysStream = aKeysStream.map((key, value) -> KeyValue.pair(key.toUpperCase(), value));

        //Merge the two streams back together.
        KStream<String, String> mergedStream = aKeysStream.merge(othersStream);

        //Print each record to the console.
        mergedStream = mergedStream.peek((key, value) -> System.out.println("key=" + key + ", value=" + value));

        //Output the transformed data to a topic.
        mergedStream.to(AppConfigs.stateless_transformations_output_topic);

        Topology topology =builder.build();

        KafkaStreams streams =new KafkaStreams(topology,props);

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

