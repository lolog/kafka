package net.yeah.kafka.producer;


public interface KafkaProducer {
	public void sendAsync(String key, String data);
	public void sendSync(String key, String data);
	public void sendAsync(String key, int partition, String data);
	
	public void close();
}
