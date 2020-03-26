package com.tvc.kafka.stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;


import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class StreamMain {

        public static void main(String[] args) {
            log.info("Start Producer");
            Properties props= new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass() );

            StreamsBuilder builder = new StreamsBuilder();
            KStream<String,String> KS0= builder.stream(AppConfigs.input_topicName);
            KS0.to(AppConfigs.output_topicName);

            final Topology topology = builder.build();
            final KafkaStreams streams = new KafkaStreams(topology, props);
            // Print the topology to the console.
            System.out.println(topology.describe());
            final CountDownLatch latch = new CountDownLatch(1);

            // Attach a shutdown handler to catch control-c and terminate the application gracefully.
            Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
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



