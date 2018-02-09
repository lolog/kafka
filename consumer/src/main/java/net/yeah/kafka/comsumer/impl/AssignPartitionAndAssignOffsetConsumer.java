package net.yeah.kafka.comsumer.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * 指定分区指定偏移量订阅
 * @author lolog
 */
public class AssignPartitionAndAssignOffsetConsumer implements net.yeah.kafka.comsumer.KafkaConsumer {
	public static LinkedBlockingQueue<ConsumerRecord<String, String>> buffer = new LinkedBlockingQueue<ConsumerRecord<String, String>>();
	
	private String brokers;
	private List<Map<String, String>> topics;
	private String group;
	
	private KafkaConsumer<String, String> consumer;

	public AssignPartitionAndAssignOffsetConsumer(String brokers, List<Map<String, String>> topics, String group) {
		this.brokers = brokers;
		this.topics = topics;
		this.group = group;
		
		this.createConsumer();
	}

	public void createConsumer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", this.brokers);
		props.put("group.id", this.group);
		props.put("enable.auto.commit", "false");
		// 限制每次调用poll返回的消息数
		props.put("max.poll.records", "10");
		// poll的时间间隔,时间如果设置太小可能收不到消息
		props.put("max.poll.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		// 指定为没有消费的起始偏移量
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		consumer = new KafkaConsumer<String, String>(props);
	}
	
	@Override
	public void starts() {
		// 订阅主题
		List<TopicPartition> partitions = new ArrayList<TopicPartition>();
		for (Map<String, String> topic: this.topics) {
			TopicPartition partition = new TopicPartition(topic.get("topic"), Integer.valueOf(topic.get("partition")));
			partitions.add(partition);
		}
		consumer.assign(partitions);
		for (Map<String, String> topic: this.topics) {
			TopicPartition partition = new TopicPartition(topic.get("topic"), Integer.valueOf(topic.get("partition")));
			consumer.seek(partition, 1);
		}
		consume();
	}
	
	public void consume () {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for(TopicPartition partition: records.partitions()) {
				List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, String> record : partitionRecords) {
                	System.out.printf("{topic = %s, partition=%d, offset = %d, key = %s, value = %s}\n",
    						record.topic(),record.partition(), record.offset(), record.key(), record.value());
                }
                // 获取partition最后一条消息的offset
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                // 提交偏移量
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
			}
		}
	}
}
