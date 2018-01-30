package com.bigdata2017.poptok.storm;


import java.util.Map;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class EsperBolt extends BaseBasicBolt {
	
	private static final long serialVersionUID = 1L;
	private static final int MAX_SPEED = 30;
	private static final int DURATION_ESTIMATE = 30;
	
	private EPServiceProvider espService;
	private boolean isOverSpeedEvent = false;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {

		Configuration configuration = new Configuration();
		configuration.addEventType( "HashtagInfo", HashtagInfo.class.getName() );

		espService = EPServiceProviderManager.getDefaultProvider( configuration );
		espService.initialize();
		
		// 에스퍼 EPL 쿼리 정의
		String eplOverSpeed =  //해쉬태그 한시간마다 카운팅하기
				"select hashtag, count(*)" + 
				"from table_poptok_hashtag " + 
				"group by hashtag";//win-> epl로 시간 속도 새는것
		
		EPStatement stmtESP = espService.getEPAdministrator().createEPL( eplOverSpeed );
		
		// EPL 쿼리 조건에 일치하는 데이터가 발생했을 때 호출 될 이벤트 함수 등록
		stmtESP.addListener( new UpdateListener(){
			@Override
			public void update( EventBean[] newEvents, EventBean[] oldEvents ) {
				if( newEvents != null ) {
					isOverSpeedEvent = true;
				}
			}
		});
	}
	
	@Override
	public void execute( Tuple tuple, BasicOutputCollector collector ) {

		String tValue = tuple.getString(0); 

		// 볼트로 넘어 오는 튜플을 VO 객체에 담아 에스퍼 엔진에 운행 정보를 등록
		String[] receiveData = tValue.split("\\,");

		HashtagInfo HashtagInfo = new HashtagInfo();
		HashtagInfo.setHashTag( receiveData[0] );
		HashtagInfo.setLocation( receiveData[1] );
		HashtagInfo.setDate( receiveData[2] );

		// 에스퍼는 VO객체를 메모리 상에서 등록된 EPL 쿼리를 실행에 사용하며 
		// 해당 이벤트 발생 시, 리스너가 실행
		espService.getEPRuntime().sendEvent( HashtagInfo ); 

		//LOGGER.error( "sendEvent:" + HashtagInfo.toString() );
		
		// 리스너가 실행 되면 볼트와 공유하고 있는 변수 isOverSpeedEvent 가 true로 설정
		// 다음 볼트(RedisBolt)에 해시태그 데이터(차량번호 + 타임스탬프)를 전송
		if( isOverSpeedEvent ) 
		{	
			//emit으로 다음 bolt로 보내버리는 작업
			collector.emit( new Values( HashtagInfo.getHashTag().substring(0,2), 
										HashtagInfo.getLocation() + "-" + HashtagInfo.getDate() ) );
			
			isOverSpeedEvent = false;
		}		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields( "hashtag", "location" ) );	
	}
	
	public class HashtagInfo {
		private String hashTag;
		private String location;
		private String date;
		
		
		
		public String getHashTag() {
			return hashTag;
		}
		public void setHashTag(String hashTag) {
			this.hashTag = hashTag;
		}
		public String getLocation() {
			return location;
		}
		public void setLocation(String location) {
			this.location = location;
		}
		public String getDate() {
			return date;
		}
		public void setDate(String date) {
			this.date = date;
		}
	

		@Override
		public String toString() {
			return "HashtagInfo [hashtag=" + hashTag + ", location=" + location + ", date=" + date +"]";
		}
	}
}
