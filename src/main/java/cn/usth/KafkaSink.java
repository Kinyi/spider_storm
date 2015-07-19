package cn.usth;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

public class KafkaSink extends AbstractSink implements Configurable{
	Producer<String, String> producer;
	
	/**
	 * 首先调用一次该方法
	 */
	@Override
	public void configure(Context context) {
		Properties prop = new Properties();
		prop.put("metadata.broker.list", "192.168.80.100:9092");
		prop.put("serializer.class", StringEncoder.class.getName());
		ProducerConfig config = new ProducerConfig(prop);
		producer = new Producer<String,String>(config);
	}
	
	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		transaction.begin();
		try {
			Event event = channel.take();
			if (event == null) {
				transaction.rollback();
				status = status.BACKOFF;
				return status;
			}
			KeyedMessage<String, String> message = new KeyedMessage<String, String>("topic1", new String(event.getBody()));
			producer.send(message);
			transaction.commit();
			status = status.READY;
		} catch (Exception e) {
			transaction.rollback();
			status = status.BACKOFF;
		}finally{
			transaction.close();
		}
		return status;
	}
}