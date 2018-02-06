package net.yeah.kafka.producer.impl;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import net.yeah.kafka.producer.KafkaProducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author lolog
 */
public class ScalaKafkaProducer implements KafkaProducer{
	private Properties properties = new Properties();
	
	private String topic;
	
	/**
	 * 1. hosts新增DNS映射
	 * 2. 只能用IP或hosts配置的域名
	 */
	private String brokers;
	
	/**
	 * 是否获取反馈
	 *  0 : 不获取反馈(消息有可能传输失败)
	 *  1 : 获取消息传递给leader后反馈(其他副本有可能接受消息失败)
	 * -1 : 所有in-sync replicas接受到消息时的反馈
	 */
	private String requiredAcks = "1";
	/**
     * 内部发送数据是异步还是同步
     * sync ：同步, 默认
     * async：异步
     */
	private String producerType = "sync";
	/**
	 * 设置缓冲区大小，默认10240Byte=10KB
	 */
	private String sendBufferBytes = "10240";
	
	private String serialize_class = "kafka.serializer.StringEncoder";
	
	private Producer<String, String> producer;
	
	public ScalaKafkaProducer(String brokers, String topic) {
		this.brokers = brokers;
		this.topic = topic;
		
		this.createProducer();
	}
	
	public ScalaKafkaProducer(String brokers, String topic, String requiredAcks, String producerType) {
		this.brokers = brokers;
		this.topic = topic;
		this.requiredAcks = requiredAcks;
		this.producerType = producerType;
		
		this.createProducer();
	}
	
	@Override
	public void sendAsync(String key, String data) {
		KeyedMessage<String, String> msg = new KeyedMessage<String, String>(this.topic, key, data);
		producer.send(msg);
	}
	
	@Override
	public void sendAsync(String key, int partition, String data) {
		KeyedMessage<String, String> msg = new KeyedMessage<String, String>(this.topic, key, partition, data);
		producer.send(msg);
	}
	
	
	@Override
	public void sendSync(String key, String data) {
		try {
			KeyedMessage<String, String> msg = new KeyedMessage<String, String>(this.topic, key, data);
			synchronized (this) {
				producer.send(msg);
			}
		} catch (Exception e) {
		}
	}
	
	public void createProducer() {
		properties.put("serializer.class", this.serialize_class);
		properties.put("metadata.broker.list", this.brokers);
		properties.put("request.required.acks", this.requiredAcks);
		properties.put("producer.type", this.producerType);
		properties.put("message.send.max.retries", "0");
		properties.put("send.buffer.bytes", this.sendBufferBytes);
		
		producer = new Producer<>(new ProducerConfig(properties));
	}
	
	@Override
	public void close() {
		try {
			producer.close();
		} catch (Exception e) {
			
		}
		producer = null;
	}
}

class ApacheSendAfter implements Callback {
	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		System.out.println("{topic = " +  metadata.topic() + ", offset = " + metadata.offset() + ", partition= " + metadata.partition() + "}");
	}
	
}
