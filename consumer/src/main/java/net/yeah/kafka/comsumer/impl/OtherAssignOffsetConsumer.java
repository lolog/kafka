package net.yeah.kafka.comsumer.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * 其他指定偏移量订阅
 * @author lolog
 */
public class OtherAssignOffsetConsumer implements net.yeah.kafka.comsumer.KafkaConsumer {
	public static LinkedBlockingQueue<ConsumerRecord<String, String>> buffer = new LinkedBlockingQueue<ConsumerRecord<String, String>>();
	
	private String brokers;
	private List<String> topics;
	private String group;
	private boolean fromBeiginning;
	
	private KafkaConsumer<String, String> consumer;

	public OtherAssignOffsetConsumer(String brokers, List<String> topics, String group, boolean fromBeiginning) {
		this.brokers = brokers;
		this.topics = topics;
		this.group = group;
		
		this.fromBeiginning = fromBeiginning;
		
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
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		consumer = new KafkaConsumer<String, String>(props);
	}
	
	@Override
	public void starts () {
		TopicPartition topicPartition = new TopicPartition(this.topics.get(0), 0);
		List<TopicPartition> partitions = Arrays.asList(new TopicPartition[] {topicPartition});
		consumer.assign(partitions);
		if (this.fromBeiginning) {
			// 从头开始消费数据
			consumer.seekToBeginning(partitions);
		}
		else {
			// 从最新位置消费
			consumer.seekToEnd(partitions);
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
