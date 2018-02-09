package net.yeah.kafka.comsumer.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * 暂停和开启主题分区消费
 * @author lolog
 */
public class LimitFlowConsumer implements net.yeah.kafka.comsumer.KafkaConsumer {
	public static LinkedBlockingQueue<ConsumerRecord<String, String>> buffer = new LinkedBlockingQueue<ConsumerRecord<String, String>>();
	
	private String brokers;
	private List<String> topics;
	private String group;
	// 暂停消费之后，慢分区消费的消费数量
	private int stopPartitionTimes = 0;
	
	private KafkaConsumer<String, String> consumer;

	public LimitFlowConsumer(String brokers, List<String> topics, String group) {
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
		props.put("max.poll.records", "2");
		// poll的时间间隔,时间如果设置太小可能收不到消息
		props.put("max.poll.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		consumer = new KafkaConsumer<String, String>(props);
	}
	
	@Override
	public void starts () {
		List<TopicPartition> partitions = new ArrayList<TopicPartition>();
		partitions.add(new TopicPartition(this.topics.get(0), 0));
		partitions.add(new TopicPartition(this.topics.get(1), 0));
		consumer.assign(partitions);
		// 从头开始消费
		consumer.seekToBeginning(partitions);
		
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
                if (partition.topic().equals(this.topics.get(1))) {
                	this.stopPartitionTimes++;
                }
			}
			
			// 暂停消费的意义：1个消费者消费主题的多个分区时，当其中一个主题的消息远落后与其他时，暂停速度快的分区的消费，即暂停的先不消费
			List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
			topicPartitions.add(new TopicPartition(this.topics.get(0), 0));
			if (this.stopPartitionTimes == 0) {
				consumer.pause(topicPartitions);
			}
			if(this.stopPartitionTimes == 4) {
				// 开启继续消费
				consumer.resume(topicPartitions);
			}
		}
	}
}