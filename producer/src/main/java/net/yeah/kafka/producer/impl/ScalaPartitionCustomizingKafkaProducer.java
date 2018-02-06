package net.yeah.kafka.producer.impl;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.utils.VerifiableProperties;
import net.yeah.kafka.producer.KafkaProducer;

/**
 * @author lolog
 */
public class ScalaPartitionCustomizingKafkaProducer implements KafkaProducer{
	private Properties props = new Properties();
	
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
	
	public ScalaPartitionCustomizingKafkaProducer(String brokers, String topic) {
		this.brokers = brokers;
		this.topic = topic;
		
		this.createProducer();
	}
	
	public ScalaPartitionCustomizingKafkaProducer(String brokers, String topic, String requiredAcks, String producerType) {
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
		props.put("serializer.class", this.serialize_class);
		props.put("metadata.broker.list", this.brokers);
		props.put("request.required.acks", this.requiredAcks);
		props.put("producer.type", this.producerType);
		props.put("message.send.max.retries", "0");
		props.put("send.buffer.bytes", this.sendBufferBytes);
		// 指定自定义分区类
		props.put("partitioner.class", "net.yeah.kafka.producer.impl.PartitionerCustomizing");
		
		producer = new Producer<>(new ProducerConfig(props));
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

/**
 * 自定义分区类
 * @author lolog
 */
class PartitionerCustomizing implements Partitioner {
	public PartitionerCustomizing (VerifiableProperties props) {
		
	}

	@Override
	public int partition(Object key, int partition_number) {
		if (key == null) {
			return 0;
		}
		int offset = key.toString().hashCode();
		return offset % partition_number;
	}
}
