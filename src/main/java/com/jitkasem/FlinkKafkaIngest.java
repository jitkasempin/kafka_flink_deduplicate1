package com.jitkasem;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.kafka.clients.producer.ProducerConfig;


import java.util.Properties;

public class FlinkKafkaIngest {

    private static long CHECKPOINTING_INTERVAL_MS = 5000;
    private static long CACHE_EXPIRATION_TIME_MS = 5_000;

    private static String KAFKA_BROKER_ADDRESS = "localhost:9092";
    private static String INPUT_TOPIC = "test-input-value";
    private static String OUTPUT_TOPIC = "test-output-value";

    private static FlinkKafkaConsumer09<String> kafkaConsumer() {

        final Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER_ADDRESS);

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-stream-basket");

        return new FlinkKafkaConsumer09<>(

                INPUT_TOPIC,

                new SimpleStringSchema(),

                properties);

    }


    private static FlinkKafkaProducer09<String> kafkaProducer() {

        final Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER_ADDRESS);

        return new FlinkKafkaProducer09<>(

                OUTPUT_TOPIC,

                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),

                properties);

    }

    private static KeySelector<String, String> keySelector() {

        return new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        };
    }


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(CHECKPOINTING_INTERVAL_MS);

        env.addSource(kafkaConsumer())

                .keyBy(keySelector())

                .filter(new DedupeFilterFunction<>(keySelector(), CACHE_EXPIRATION_TIME_MS))

                .addSink(kafkaProducer());


        env.execute("Deduplication Example");


    }
}
