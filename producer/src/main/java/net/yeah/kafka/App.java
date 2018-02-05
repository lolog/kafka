package net.yeah.kafka;

import org.junit.Test;

public class App {
	private String topic = "test";
	private String brokers = "master:9092";
	
	@Test
	public void cdhSend() {
		CdhKafkaProducer producer = new CdhKafkaProducer(brokers, topic);
		
		producer.send("key0", "data0");
		producer.close();
	}
	
	@Test
	public void apacheSend() {
		ApacheKafkaProducer producer = new ApacheKafkaProducer(brokers, topic);
		
		producer.send("key1", "data1");
		
		producer.close();
	}
}
