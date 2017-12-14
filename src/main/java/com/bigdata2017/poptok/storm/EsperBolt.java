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
		
		String eplOverSpeed =  //해쉬태그 한시간마다 카운팅하기
				"select hashtag, count(*)" + 
				"from table_poptok_hashtag " + 
				"group by hashtag";//win-> epl로 시간 속도 새는것
		EPStatement stmtESP = espService.getEPAdministrator().createEPL( eplOverSpeed );
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

	
		String[] receiveData = tValue.split("\\,");

		HashtagInfo HashtagInfo = new HashtagInfo();
		HashtagInfo.setHashTag( receiveData[0] );
		HashtagInfo.setLocation( receiveData[1] );
		HashtagInfo.setDate( receiveData[2] );

		espService.getEPRuntime().sendEvent( HashtagInfo ); 

		//LOGGER.error( "sendEvent:" + HashtagInfo.toString() );
		
		if( isOverSpeedEvent ) {
			
			collector.emit( new Values( HashtagInfo.getHashTag().substring(0,2), 
										HashtagInfo.getLocation() + "-" + HashtagInfo.getDate() ) );//emit으로 다음 bolt로 보내버리는 작업
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
