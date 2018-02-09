package net.yeah.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.yeah.kafka.comsumer.KafkaConsumer;
import net.yeah.kafka.comsumer.impl.AssignPartitionAndAssignOffsetConsumer;
import net.yeah.kafka.comsumer.impl.AssignPartitionConsumer;
import net.yeah.kafka.comsumer.impl.AutoCommitOffsetConsumer;
import net.yeah.kafka.comsumer.impl.AutoPartitionAndAssignOffsetConsumer;
import net.yeah.kafka.comsumer.impl.CntrlCommitBatchOffsetConsumer;
import net.yeah.kafka.comsumer.impl.CntrlCommitOnlyOffsetConsumer;
import net.yeah.kafka.comsumer.impl.LimitFlowConsumer;
import net.yeah.kafka.comsumer.impl.MutiThreadWakeupConsumer;
import net.yeah.kafka.comsumer.impl.OtherAssignOffsetConsumer;

import org.junit.Test;


/**
 * 提示：订阅者模式不能指定offset
 * @author lolog
 */
public class App {
	private List<String> topics = Arrays.asList(new String[] {"test"});
	// 为了防止服务器问题，可以多指定几个broker
	private String brokers = "guojl:9092";
	
	@Test
	public void autoCommitOffsetConsume() {
		String group = "auto_offset_test";
		KafkaConsumer consumer = new AutoCommitOffsetConsumer(brokers, topics, group);
		consumer.starts();
	}
	
	@Test
	public void cntrlCommitBatchOffsetConsume() {
		String group = "control_offset_test";
		KafkaConsumer consumer = new CntrlCommitBatchOffsetConsumer(brokers, topics, group);
		consumer.starts();
	}
	
	@Test
	public void cntrlCommitOnlyOffsetConsume() {
		String group = "control_offset_test";
		KafkaConsumer consumer = new CntrlCommitOnlyOffsetConsumer(brokers, topics, group);
		consumer.starts();
	}
	
	@Test
	public void assignPartitionConsume() {
		String group = "assign_partition_test";
		List<Map<String, String>> topics = new ArrayList<Map<String,String>>();
		Map<String, String> topic = new HashMap<String, String>();
		topic.put("topic", "test");
		topic.put("partition", "0");
		topics.add(topic);
		
		KafkaConsumer consumer = new AssignPartitionConsumer(brokers, topics, group);
		consumer.starts();
	}
	
	@Test
	public void assignPartitionAndAssignOffsetConsume() {
		String group = "assign_partition_assign_offset_test";
		List<Map<String, String>> topics = new ArrayList<Map<String,String>>();
		Map<String, String> topic = new HashMap<String, String>();
		topic.put("topic", "test");
		topic.put("partition", "0");
		topics.add(topic);
		
		KafkaConsumer consumer = new AssignPartitionAndAssignOffsetConsumer(brokers, topics, group);
		consumer.starts();
	}
	
	@Test
	public void autoPartitionAndAssignOffsetConsume() {
		String group = "auto_partition_assign_offset_test";
		Map<Integer, Integer> partitions = new HashMap<Integer, Integer>();
		partitions.put(0, 1);
		
		// 测试
		// 1. already=false
		// 2. already=true
		KafkaConsumer consumer = new AutoPartitionAndAssignOffsetConsumer(brokers, topics, group, partitions, true);
		consumer.starts();
	}
	
	@Test
	public void otherAssignOffsetConsume() {
		String group = "other_assign_offset_test";
		Map<Integer, Integer> partitions = new HashMap<Integer, Integer>();
		partitions.put(0, 1);
		
		// 测试
		// 1. fromBeginning=false
		// 2. fromBeginning=true
		KafkaConsumer consumer = new OtherAssignOffsetConsumer(brokers, topics, group, false);
		consumer.starts();
	}
	
	@Test
	public void limitFlowConsume() {
		String group = "limit_flow_test";
		
		topics = Arrays.asList(new String[]{"test", "foo"});
		
		// 测试
		// 暂停之后，向foo主题生产消息
		KafkaConsumer consumer = new LimitFlowConsumer(brokers, topics, group);
		consumer.starts();
	}
	
	@Test
	public void mutiThreadWakeupConsume() {
		String group = "muti_thread_wakeup_test";
		
		MutiThreadWakeupConsumer consumer = new MutiThreadWakeupConsumer(brokers, topics, group);
		consumer.starts();
		
		while(consumer.already() == false) {
			
		}
		
		consumer.shut();
	}
}
