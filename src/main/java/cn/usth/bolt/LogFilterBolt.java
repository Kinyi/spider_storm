package cn.usth.bolt;

import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.dbutils.DbUtils;

import cn.usth.utils.MyDateUtils;
import cn.usth.utils.MyDbUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class LogFilterBolt extends BaseRichBolt{
	OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	Pattern compile = Pattern.compile("页面下载成功.url:\\[http://[\\w]+\\.(.+)/(.+)\\].耗时\\[(\\d+)\\]毫秒.当前时间戳:\\[(\\d+)\\]");
	@Override
	public void execute(Tuple input) {
		String value = new String(input.getBinaryByField("bytes"));
		Matcher matcher = compile.matcher(value);
		if (matcher.find() && matcher.groupCount() == 4) {
			String topdomain = matcher.group(1);
			String useTime = matcher.group(3);
			String currentTime = matcher.group(4);
			Date date = new Date(Long.parseLong(currentTime));
			MyDbUtils.update(MyDbUtils.INSERT_LOG, topdomain,Integer.parseInt(useTime),MyDateUtils.formatDate2(date));
		}
		this.collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
}