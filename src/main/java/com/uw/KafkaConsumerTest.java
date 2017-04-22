package com.uw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class KafkaConsumerTest {

    final String brokers="localhost:9092";
    final String zookeepers="localhost:2181";
    final String topic= "testTopic";
    final String consumerGroup="group";

    public static void main(String args[]){
        KafkaConsumerTest t = new KafkaConsumerTest();
        List<String> resultList = t.getLatest(100);
        System.out.println("size : "+resultList.size());
        for(String s : resultList){
            System.out.println(s);
        }

    }

    public List<String> getLatest(int limit){
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", consumerGroup);
        props.put("zookeeper.connect", zookeepers);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer consumer = new KafkaConsumer(props);

        List<String> result = new ArrayList<>();

        try {
            List<PartitionInfo> partInfoList = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = new ArrayList<>();
            for(PartitionInfo pi : partInfoList){
                TopicPartition tp = new TopicPartition(topic, pi.partition());
                partitions.add(tp);
            }
            consumer.assign(partitions);
            consumer.seekToEnd();

            int partitionOffset = (int)Math.ceil(((double)limit)/((double)partitions.size()));
            for(TopicPartition tp :partitions){
                long offset = (consumer.position(tp) < partitionOffset) ? consumer.position(tp) : partitionOffset;
                consumer.seek(tp, consumer.position(tp)-offset);
            }

            ConsumerRecords<String, String> records = consumer.poll(60000);
            for (ConsumerRecord<String, String> record : records) {
                result.add(record.value());
            }
        } finally {
            consumer.close();
        }

        int startIndex = (result.size()-limit) <= 0 ? 0 : result.size()-limit;
        return result.subList(startIndex,result.size());
    }
}
