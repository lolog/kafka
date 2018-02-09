package net.yeah.kafka.comsumer.lower;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

/**
 * 测试不能消费
 * @author lolog
 */
public class LowerKafkaConsumer extends Thread {
	private List<String> brokers;
	private int port;
	
	private String topic;
	private String group;
	
	private SimpleConsumer consumer;
	
	// 一次读取最多的记录数
	private int fetchSize;
	// 开始读取的偏移量
	private long offset;
	// 分区
	private int partition;
	
	private static int soTimeout = 1000000;
	private static int bufferSize = 64 * 1024;
	
	public LowerKafkaConsumer(List<String> brokers, int port, String topic, String group) {
		this.brokers = brokers;
		this.port = port;
		
		this.topic = topic;
		this.group = group;
	}
	
	@Override
	public void run() {
		// 获取分区信息
		PartitionMetadata metadata = findPartitionMetadata(this.partition);
		if (metadata == null) {
			System.out.printf("topic=%s, partition=%d不能找到其Metadata\n", this.topic, this.partition);
			return;
		}
		if (metadata.leader() == null) {
			System.out.printf("topic=%s, partition=%d不能找到Leader\n", this.topic, this.partition);
			return;
		}
		Broker leaderBroker = metadata.leader();
		this.consumer = new SimpleConsumer(leaderBroker.host(), this.port, soTimeout, bufferSize, group);
		
		// 找到其偏移量
		long offset = getOffset(this.partition, kafka.api.OffsetRequest.EarliestTime());
		
		// 获取消息错误的次数
		int errorNums = 0;
		while(true) {
			// 请求信息
			FetchRequest request = new FetchRequestBuilder()
									.clientId(group)
									.addFetch(topic, partition, offset, fetchSize)
									.build();
			// 获取消息
			kafka.javaapi.FetchResponse fetchResponse = consumer.fetch(request);
			
			if(fetchResponse.hasError()) {
				System.out.printf("topic=%s, partition=%d,从LeaderBroker=%s获取消息错误, 错误码=%d\n",
						topic, partition, leaderBroker.host(), fetchResponse.errorCode(topic, partition));
				if (errorNums == 5) {
					break;
				}
				// 如果是偏移量溢出,重新获取偏移量
				if (fetchResponse.errorCode(topic, partition) == ErrorMapping.OffsetOutOfRangeCode()) {
					offset = getOffset(errorNums, kafka.api.OffsetRequest.EarliestTime());
					errorNums++;
					continue;
				}
				// 重新建立连接
				errorNums = 0;
				consumer.close();
				consumer = null;
				// 重新获取leader
				metadata = findPartitionMetadata(this.partition);
				leaderBroker = metadata.leader();
			}
			ByteBufferMessageSet set = fetchResponse.messageSet(topic, partition);
			// 消息处理
			for (MessageAndOffset messageAndOffset: set) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < offset) {
					System.out.printf("消息错误,其中offset=%d, currentOffset=%d%n", this.offset, currentOffset);
					continue;
				}
				// 获取下一个消息偏移量
				this.offset = messageAndOffset.nextOffset();
				// 读取消息
				ByteBuffer payload = messageAndOffset.message().payload();
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				
				try {
					System.out.printf("data offset=%d,data=%s%n", messageAndOffset.offset(), new String(bytes, "utf-8"));
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
			try {
				sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void setOption(int partition, int fetchSize, long offset) {
		this.partition = partition;
		this.fetchSize = fetchSize;
		this.offset = offset;
	}
	
	/**
	 * 获取分区的偏移量
	 * @param partition
	 * @param whichTime
	 */
	public long getOffset(int partition, long whichTime) {
		// 发送请求，获取响应
		int maxNumOffsets = 1;
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(new TopicAndPartition(this.topic, partition), new PartitionOffsetRequestInfo(whichTime, maxNumOffsets));
		OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), group);
		OffsetResponse response = consumer.getOffsetsBefore(request);
		
		if (response.hasError()) {
			System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
	/**
	 * 找到partition分区的Metadata信息
	 * @param partition 分区
	 */
	public PartitionMetadata findPartitionMetadata(int partition) {
		PartitionMetadata metadata = null;
		for (String broker: this.brokers) {
			this.consumer = new SimpleConsumer(broker, port, soTimeout, bufferSize, group);
			
			// 发送请求
			TopicMetadataRequest request = new TopicMetadataRequest(Arrays.asList(this.topic));
			TopicMetadataResponse response = consumer.send(request);
			
			// 获取响应头信息
			List<TopicMetadata> metadatas = response.topicsMetadata();
			for (TopicMetadata item: metadatas) {
				// 循环broker的分区Metadata
				for (PartitionMetadata meta: item.partitionsMetadata()) {
					if(meta.partitionId() == partition) {
						metadata = meta;
						break;
					}
				};
			}
			consumer.close();
			consumer = null;
		}
		return metadata;
	}
}
