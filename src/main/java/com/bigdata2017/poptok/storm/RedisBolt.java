package com.bigdata2017.poptok.storm;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class RedisBolt extends AbstractRedisBolt {
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SplitBolt.class);

	public RedisBolt( JedisClusterConfig  config ) {
		super( config );
	}

	public RedisBolt( JedisPoolConfig config ) {
		super( config );
	}
	
	@Override
	public void execute( Tuple tuple ) {

		JedisCommands jedisCommands = null;
		
		try {
			// EsperBolt에서 전송한 튜플에는 해시태그와 지역 번호가 있다.
			String hashtag = tuple.getStringByField( "poptok_tag" );
			//String location = tuple.getStringByField( "locationNo" );
			
			String[] receiveData = hashtag.split("\\,");
			Double tagCnt = 1.0;
			jedisCommands = getInstance();
				
			// 레디스 클라이언트 라이블러리 Jedis의 JedisCommnads를 이용해 레디스 서버에 두 값을 키와 값으로 적재.
			// 만료 시간을 7일로 설정해서 1주일이 경과하면 과속 운행 데이터는 영구적으로 삭제.
			for(int i=0; i<receiveData.length; i++)
			{
				tagCnt = jedisCommands.zscore("poptok_tag", receiveData[i]);
				jedisCommands.zadd("poptok_tag", (tagCnt + 1.0), receiveData[i]);
				//jedisCommands.sadd( hashtag, location );
				//jedisCommands.expire( hashtag, 60 * 60 * 24 * 7 );
				LOGGER.info("poptok_tag = " + receiveData[i] + ", count = " + tagCnt);
			}
	
		} catch (JedisConnectionException e) {
			throw new RuntimeException( "Exception occurred to JedisConnection", e );
		} catch (JedisException e) {
			System.out.println( "Exception occurred from Jedis/Redis" + e );
		} finally {
			if ( jedisCommands != null ) {
				returnInstance( jedisCommands );
			}
			
			collector.ack( tuple );
		}		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}
}
