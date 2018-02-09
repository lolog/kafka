package net.yeah.kafka;

import java.util.Arrays;

import net.yeah.kafka.comsumer.lower.LowerKafkaConsumer;

public class LowerApp {
	public static void main(String[] args) {
		String group = "lower_test";
		String broker = "guojl";
		String topic = "test";
		int port = 9092;
		int partition = 0;
		int fetchSize = 2;
		long offset = 1;
		
		LowerKafkaConsumer consumer = new LowerKafkaConsumer(Arrays.asList(broker), port, topic, group);
		consumer.setOption(partition, fetchSize, offset);
		consumer.start();
	}
}
