package net.yeah.kafka.comsumer.impl;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

/**
 * 多线程消费<br>
 * wakeup可以安全地从外部线程来中断活动操作
 * @author lolog
 */
public class MutiThreadWakeupConsumer extends Thread implements net.yeah.kafka.comsumer.KafkaConsumer {
	private String brokers;
	private List<String> topics;
	private String group;
	
	private int recodNums = 0;
	private AtomicBoolean already = new AtomicBoolean(false);

	private KafkaConsumer<String, String> consumer;

	public MutiThreadWakeupConsumer(String brokers, List<String> topics, String group) {
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
		// 指定为没有消费的起始偏移量
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		consumer = new KafkaConsumer<String, String>(props);
	}

	@Override
	public void starts() {
		this.start();
	}

	@Override
	public void run() {
		// 订阅主题
		consumer.subscribe(topics);
		consume();
	}

	public void consume()  {
		while (true) {
			try {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
					for (ConsumerRecord<String, String> record : partitionRecords) {
						System.out.printf("{topic = %s, partition=%d, offset = %d, key = %s, value = %s}\n",
										record.topic(), record.partition(), record.offset(), record.key(), record.value());
					}
				}
				if (recodNums == 1) {
					already.set(true);
					// 暂停10s
					sleep(1000L);
				}
				recodNums++;
			} catch (InterruptedException e) {
				consumer.close();
				System.out.println(e);
			}
		}
	}
	
	public void shut() {
		// 安全地从外部线程来中断活动操作
		consumer.wakeup();
	}
	
	public boolean already() {
		return already.get();
	}
}
