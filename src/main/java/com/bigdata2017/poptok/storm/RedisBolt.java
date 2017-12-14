package com.bigdata2017.poptok.storm;

import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class RedisBolt extends AbstractRedisBolt {
	private static final long serialVersionUID = 1L;

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
				
			String hashtag = tuple.getStringByField( "hashtag" );
			String location = tuple.getStringByField( "location" );
	
			jedisCommands = getInstance();
			jedisCommands.sadd( hashtag, location );
			jedisCommands.expire( hashtag, 60 * 60 * 24 * 7 );
	
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
