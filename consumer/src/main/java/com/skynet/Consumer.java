package com.skynet;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private final static String TOPIC = "test_topic";

    public static void main(String[] args) {
        System.out.println("consumer is starting");

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Collections.singletonList(TOPIC));

        /* Value for giving up recurring empty list */
        final int noResponseGiveUp = 100;
        int noRecordCount = 0;

        try {
            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(1000);

                if (records.count() == 0) {
                    noRecordCount++;

                    if(noRecordCount > noResponseGiveUp) break;

                    continue;
                }

                noRecordCount = 0;

                records.forEach(record -> System.out.println(
                        String.format("key: %s, value: %s", record.key(), record.value())
                ));

                consumer.commitAsync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
