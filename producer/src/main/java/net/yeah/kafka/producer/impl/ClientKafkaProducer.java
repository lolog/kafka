package net.yeah.kafka.producer.impl;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author lolog
 */
@SuppressWarnings("unused")
public class ClientKafkaProducer implements net.yeah.kafka.producer.KafkaProducer{
	private String topic;
	/**
	 * 1. hosts新增DNS映射
	 * 2. 只能用IP或hosts配置的域名
	 */
	private String brokers;
	
	private Producer<String, String> producer;

	public ClientKafkaProducer(String brokers, String topic) {
		this.topic = topic;
		this.brokers = brokers;
		
		this.createProducer();
	}
	
	@Override
	public void sendAsync(String key, String data) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, key, data);
		/**
		 * 1. 通过Handler回调,异步非阻塞
		 * 2. 通过future.get()回调,阻塞方式
		 */
		Future<RecordMetadata> future = producer.send(record, new ClientKafkaSendAfter());
	}
	
	@Override
	public void sendAsync(String key, int partition, String data) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, partition, key, data);
		/**
		 * 1. 通过Handler回调,异步非阻塞
		 * 2. 通过future.get()回调,阻塞方式
		 */
		Future<RecordMetadata> future = producer.send(record, new ClientKafkaSendAfter());
	}
	
	@Override
	public void sendSync(String key, String data) {
		/**
		 * 通过future.get()回调,阻塞方式
		 */
		Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(this.topic, key, data));
	}

	public void createProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", this.brokers);
		// 0：不需要等待任何确认信息,副本添加到socket buffer并认为已经发送。
		// 1：等待leader将数据写入本地log,但是并没有等待所有follower是否成功写入。
		// all：leader等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。
		props.put("acks", "all");
		// 设置数据发送失败，重试发送的次数
		props.put("retries", 0);
		// 批处理提交的数据大小,16384Byte
		props.put("batch.size", 16384);
		// 消息延迟发送的时间,1ms
		props.put("linger.ms", 1);
		// 缓存数据的内存大小,33554432Byte
		props.put("buffer.memory", 33554432);
		// 试图重试失败的produce请求之前的等待时间。避免陷入发送-失败的死循环中。
		props.put("retry.backoff.ms", 100);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		this.producer = new KafkaProducer<>(props);
	}
	
	@Override
	public void close() {
		try {
			producer.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		producer = null;
	}
}

/**
 * 异步回调Hanlder,如果发送失败将exception
 * @author Adolf Felix
 */
class ClientKafkaSendAfter implements Callback {
	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		System.out.println("{topic = " +  metadata.topic() + ", offset = " + metadata.offset() + ", partition= " + metadata.partition() + "}");
		
		if (exception == null) {
			System.out.println("Send Successful...");
		}
		else {
			System.out.println("exception = " + exception.getMessage());
		}
	}
	
}
