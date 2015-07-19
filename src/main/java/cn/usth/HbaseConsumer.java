package cn.usth;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class HbaseConsumer extends Thread{
	String topic_name = "topic1";

	@Override
	public void run() {
		Properties prop = new Properties();
		prop.put("zookeeper.connect", "192.168.80.100:2181,192.168.80.101:2181,192.168.80.102:2181");
		prop.put("group.id", "kinyi");
		ConsumerConfig conf = new ConsumerConfig(prop);
		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(conf);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic_name, 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams = consumerConnector.createMessageStreams(topicCountMap);
		while(true){
			KafkaStream<byte[], byte[]> kafkaStream = createMessageStreams.get(topic_name).get(0);
			ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
			if(iterator.hasNext()){
				String value = new String(iterator.next().message());
				System.out.println(value);
			}
		}
	}
	
	public static void main(String[] args) {
		HbaseConsumer hbaseConsumer = new HbaseConsumer();
		hbaseConsumer.start();
	}
}
