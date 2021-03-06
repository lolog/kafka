package net.yeah.kafka;

import net.yeah.kafka.producer.KafkaProducer;
import net.yeah.kafka.producer.impl.ClientKafkaProducer;
import net.yeah.kafka.producer.impl.ScalaKafkaProducer;
import net.yeah.kafka.producer.impl.ScalaPartitionCustomizingKafkaProducer;

import org.junit.Test;

/**
 * @author lolog
 *  1. InterruptException - 线程阻塞中断。<br>
 *  2. SerializationException - key或value不是给定有效配置的serializers。<br>
 *  3. TimeoutException - 获取元数据或消息分配内存话费的时间超过max.block.ms。<br>
 *  4. KafkaException - Kafka有关的错误（不属于公共API的异常）。<br>
 */
public class App {
	private String topic = "test";
	private String brokers = "guojl:9092";
	
	@Test
	public void clientSyncSend() {
		KafkaProducer producer = new ClientKafkaProducer(brokers, topic);
		
		producer.sendSync("client_sync_key", "client_sync_data");
		producer.close();
	}
	
	@Test
	public void scalaSyncSend() {
		KafkaProducer producer = new ScalaKafkaProducer(brokers, topic);
		
		producer.sendSync("scala_sync_key", "scala_sync_data");
		producer.close();
	}
	
	@Test
	public void clientAsyncSend() {
		KafkaProducer producer = new ClientKafkaProducer(brokers, topic);
		
		producer.sendAsync("client_async_key", "client_async_data");
		producer.close();
	}
	
	@Test
	public void scalaAsyncSend() {
		String requiredAcks = "0";
		String producerType = "async";
		KafkaProducer producer = new ScalaKafkaProducer(brokers, topic, requiredAcks, producerType);
		
		producer.sendAsync("scala_async_key", "scala_async_data");
		producer.close();
	}
	
	@Test
	public void clientAsyncPartitionSend() {
		int partition = 0;
		KafkaProducer producer = new ClientKafkaProducer(brokers, topic);
		
		// 指定分区发送，那么发送是按照先后顺序执行的
		producer.sendAsync("client_async_partition_key_0", partition, "client_async_partition_data_0");
		producer.sendAsync("client_async_partition_key_1", partition, "client_async_partition_data_1");
		producer.close();
	}
	
	@Test
	public void scalaAsyncPartitionSend() {
		int partition = 0;
		String requiredAcks = "0";
		String producerType = "async";
		KafkaProducer producer = new ScalaKafkaProducer(brokers, topic, requiredAcks, producerType);
		
		// 指定分区发送，那么发送是按照先后顺序执行的
		producer.sendAsync("scala_async_partition_key_0", partition, "scala_async_partition_data_0");
		producer.sendAsync("scala_async_partition_key_1", partition, "scala_async_partition_data_1");
		producer.close();
	}
	
	@Test
	public void scalaAsyncPartitionCustomizingSend() {
		int partition = 0;
		String requiredAcks = "0";
		String producerType = "async";
		KafkaProducer producer = new ScalaPartitionCustomizingKafkaProducer(brokers, topic, requiredAcks, producerType);
		
		// 分区自定义
		producer.sendAsync("scala_async_partition_customizing_key", "scala_async_partition_customizing_data");
		// 固定分区
		producer.sendAsync("scala_async_partition_customizing_key_0", partition, "scala_async_partition_customizing_data_0");
		producer.close();
	}
}
