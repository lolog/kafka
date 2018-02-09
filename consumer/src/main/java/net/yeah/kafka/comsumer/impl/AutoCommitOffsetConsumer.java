package net.yeah.kafka.comsumer.impl;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 自动提交偏移量<br><br>
 * broker通过心跳自动检测test组中失败的进程，消费者会自动ping集群，告诉进群它还活着。
 * 只要消费者能够做到这一点，它就被认为是活着的，并保留分配给它分区的权利，如果它停止心跳的
 * 时间超过session.timeout.ms, 那么就会认为是故障的，它的分区将被分配到别的进程。
 * @author lolog
 */
public class AutoCommitOffsetConsumer implements net.yeah.kafka.comsumer.KafkaConsumer {
	private String brokers;
	private List<String> topics;
	private String group;
	
	private KafkaConsumer<String, String> consumer;

	public AutoCommitOffsetConsumer(String brokers, List<String> topics, String group) {
		this.brokers = brokers;
		this.topics = topics;
		this.group = group;
		
		this.createConsumer();
	}

	public void createConsumer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", this.brokers);
		props.put("group.id", this.group);
		props.put("enable.auto.commit", "true");
		// 限制每次调用poll返回的消息数
		props.put("max.poll.records", "10");
		// poll的时间间隔,时间如果设置太小可能收不到消息
		props.put("max.poll.interval.ms", "1000");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		consumer = new KafkaConsumer<String, String>(props);
	}
	
	@Override
	public void starts () {
		consumer.subscribe(this.topics);
		consume();
	}
	
	public void consume () {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("{topic = %s, partition=%d, offset = %d, key = %s, value = %s}\n",
						record.topic(),record.partition(), record.offset(), record.key(), record.value());
			}
		}
	}
}
