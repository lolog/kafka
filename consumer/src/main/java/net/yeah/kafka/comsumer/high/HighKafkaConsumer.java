package net.yeah.kafka.comsumer.high;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class HighKafkaConsumer extends Thread {
	private ConsumerConnector consumer;
	private String topic;
	private ExecutorService service;
	
	private int numThreads = 1;
	
	public HighKafkaConsumer(String topic, String zk, String groupId) {
		ConsumerConfig config = createConsumerConfig(zk, groupId);
		this.consumer = Consumer.createJavaConsumerConnector(config);
		this.topic =  topic;
	}
	
	private ConsumerConfig createConsumerConfig(String zk, String groupId) {
        Properties prop = new Properties();
        prop.put("group.id", groupId);
        prop.put("zookeeper.connect", zk);
        prop.put("auto.offset.reset", "smallest");// smallest,largest
        prop.put("zookeeper.session.timeout.ms", "400");
        prop.put("zookeeper.sync.time.ms", "200");
        prop.put("auto.commit.interval.ms", "1000");
        
        // 构建ConsumerConfig对象
        return new ConsumerConfig(prop);
	}
	
	public void shutdown () {
		if (consumer != null) {
			consumer.shutdown();
		}
		if (service != null) {
			service.shutdown();
		}
	}
	
	@Override
	public void run() {
		// 指定Topic
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(this.topic, this.numThreads);
        
		List<KafkaStream<byte[], byte[]>> streams = consumer.createMessageStreams(topicCountMap).get(this.topic);
		
		service = Executors.newFixedThreadPool(this.numThreads);
		
		int no = 0;
		for (final KafkaStream<byte[], byte[]> stream: streams) {
			service.submit(new ConsumerDeal(stream, no));
			no++;
		}
	}
}

class ConsumerDeal implements Callable<String> {
	// Kafka数据流
	private KafkaStream<byte[], byte[]> stream;
	// 线程ID编号
	private int no;
	
	public ConsumerDeal(KafkaStream<byte[], byte[]> stream, int no) {
		this.stream = stream;
		this.no = no;
		System.out.println("thread no.=" + no);
	}
	
	@Override
	public String call() throws Exception {
		// 获取数据迭代器
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		while(iterator.hasNext()) {
			// 获取数据值
            MessageAndMetadata<byte[], byte[]> value = iterator.next();
			System.out.println("{thread no.=" + this.no
					+ ", clientId="+ iterator.clientId() 
					+ ", topic=" + value.topic()
					+ ", partition=" + value.partition()
					+ ", offset=" + value.offset() 
					+ ", message=" + new String(value.message()) +"}");
		}
		return "end";
	}
}