package spider_storm;


import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import org.junit.Test;

public class TestProducer {

	@Test
	public void test() throws Exception {
		Properties prop = new Properties();
		prop.put("metadata.broker.list", "192.168.80.100:9092");
		prop.put("serializer.class", StringEncoder.class.getName());
		ProducerConfig config = new ProducerConfig(prop);
		Producer<String, String> producer = new Producer<String,String>(config);
		KeyedMessage<String, String> message = new KeyedMessage<String, String>("topic1", "I love you");
		producer.send(message);
	}
}
