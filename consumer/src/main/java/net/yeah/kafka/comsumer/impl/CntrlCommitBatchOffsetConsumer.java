package net.yeah.kafka.comsumer.impl;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 手动提交偏移量
 * @author lolog
 */
public class CntrlCommitBatchOffsetConsumer implements net.yeah.kafka.comsumer.KafkaConsumer {
	public static LinkedBlockingQueue<ConsumerRecord<String, String>> buffer = new LinkedBlockingQueue<ConsumerRecord<String, String>>();
	
	private String brokers;
	private List<String> topics;
	private String group;
	
	private KafkaConsumer<String, String> consumer;

	public CntrlCommitBatchOffsetConsumer(String brokers, List<String> topics, String group) {
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
	public void starts () {
		// 订阅主题
		consumer.subscribe(this.topics);
		consume();
	}
	
	public void consume () {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				buffer.add(record);
				
				System.out.printf("{topic = %s, partition=%d, offset = %d, key = %s, value = %s}\n",
						record.topic(),record.partition(), record.offset(), record.key(), record.value());
			}
			if (buffer.isEmpty() == false) {
				// 提交偏移量
				consumer.commitSync();
				buffer.clear();
				System.out.println("Commit Sync Succeful...");
			}
		}
	}
}
