package net.yeah.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * adapter for apache kafka
 * @author lolog
 */
public class ApacheKafkaProducer {
	private Properties properties = new Properties();
	
	private String topic;
	private String brokers;
	
	/**
	 * 是否获取反馈
	 * 0  : 不获取反馈(消息有可能传输失败)
	 * 1  : 获取消息传递给leader后反馈(其他副本有可能接受消息失败)
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
	
	public ApacheKafkaProducer(String brokers, String topic) {
		this.brokers = brokers;
		this.topic = topic;
		
		this.createProducer();
	}
	
	public boolean send(String key, String data) {
		try {
			KeyedMessage<String, String> msg = new KeyedMessage<String, String>(this.topic, key, data);
			synchronized (this) {
				producer.send(msg);
			}
		} catch (Exception e) {
		}
		return true;
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
	
	public void close() {
		try {
			producer.close();
		} catch (Exception e) {
			
		}
		producer = null;
	}

}
