package net.yeah.kafka;

import net.yeah.kafka.comsumer.high.HighKafkaConsumer;

public class HighApp {
	public static void main(String[] args) {
		String topic = "test";
		String zk = "guojl:2181";
		String groupId = "high_app_0";
		HighKafkaConsumer consumer = new HighKafkaConsumer(topic, zk, groupId);
		consumer.start();
	}
}
