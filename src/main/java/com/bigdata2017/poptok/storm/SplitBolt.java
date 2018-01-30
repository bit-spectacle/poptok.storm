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


// Split Bolt 구현
// Kafka Spout로 전달 받은 tuple을 HBase 컬럼 단위로 분리하기 위한 작업
public class SplitBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SplitBolt.class);

	// KafkaSpout에서 전달한 데이터가 튜플 형식으로 수신.
	// 튜플에 들어 있는 데이터를 콤마로 분리해서 배열에 저장
	public void execute(Tuple tuple, BasicOutputCollector collector) {
				
		String tValue = tuple.getString(0);
		LOGGER.error(tValue);
		
		// - 로 로그 앞쪽의 로그정보 부분과 로그 뒷쪽의 태그 부분으로 구분
		String[] substrData = tValue.split("-");
		// 로그 앞쪽 로그 정보부분을 잘라낸 태그에서 공백을 모두 삭제
		String sValue = substrData[1].replaceAll(" ", "");
		sValue = sValue.replaceAll("#", "");
		LOGGER.error(sValue);
		
		// ,로 각 태그 구분
		//String[] receiveData = sValue.split("\\,");
		//LOGGER.error(Arrays.toString(receiveData));

		// 실시간 주행 정보의 데이터셋을 정의. emit를 통해 다음 Bolt로 넘어감(비정형->정형)
		// 해시태그, 지역 번호, 날짜
		collector.emit(new Values(new StringBuffer(sValue)));
	}

	// 9개 의 데이터셋과 일치하는 필드명을 정의
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("poptok_tag"));
	}

}
