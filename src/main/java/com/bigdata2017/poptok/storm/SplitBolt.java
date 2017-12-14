package com.bigdata2017.poptok.storm;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata2017.poptok.storm.SplitBolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SplitBolt.class);

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String tValue = tuple.getString(0);
		LOGGER.error(tValue);
		String[] receiveData = tValue.split("\\,");

		LOGGER.error(Arrays.toString(receiveData));

	
		collector.emit(new Values(new StringBuffer(receiveData[0]).reverse() + "-" + receiveData[1], receiveData[0],
				receiveData[1]));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("hashtag", "location" ,"date"));
	}

}
