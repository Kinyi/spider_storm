package cn.usth.storm;

import java.util.UUID;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import cn.usth.bolt.LogFilterBolt;

public class LogProcess {

	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
		String topic = "topic1";
		String zkRoot = "/kafkaSpout";
		String id = UUID.randomUUID().toString();
		SpoutConfig spoutConf = new SpoutConfig(new ZkHosts("192.168.80.100:2181"), topic, zkRoot, id);
		
		String spout_name = KafkaSpout.class.getSimpleName();
		String bolt_name = LogFilterBolt.class.getSimpleName();
		
		builder.setSpout(spout_name, new KafkaSpout(spoutConf));
		builder.setBolt(bolt_name, new LogFilterBolt()).shuffleGrouping(spout_name);
		String simpleName = LogProcess.class.getSimpleName();
		Config conf = new Config();
		
		if (args.length != 0) {
			try {
				StormSubmitter.submitTopology(simpleName, conf, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(simpleName, conf, builder.createTopology());
		}
	}
}
