package com.bigdata2017.poptok.storm;

import java.util.Arrays;
import java.util.UUID;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


// Kafka Spout 구현
// 주요 기능 = 카프카로 부터 수신 받은 데이터를 두 개의 Bolt에게 라우팅하는 것(Split Bolt, Esper Bolt)
public class PoptokTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		// 환경 설정
		Config config = new Config();
		
		config.setDebug( true );
		config.put( Config.NIMBUS_HOST, "hadoop2.poptok.com" );//storm서버
		config.put( Config.NIMBUS_THRIFT_PORT, 6627 );
		config.put( Config.STORM_ZOOKEEPER_PORT, 2181 );
		config.put( Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList( "hadoop2.poptok.com" ) );//zookeeper서버
		// props.put("enable.auto.commit", "false");
		// 토폴로지 등록
		StormSubmitter.submitTopology( args[0], config, makeTopology() );
	}
	
	private static StormTopology makeTopology() {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		// Spout(Kafka) 생성 및 등록
		// zookeeper서버 정보 설정(Kafka와 연결되어 있으므로 Kafka에 접속하기 위해)
		// 카프카에 접속하기 위해 카프카와 연결된 주키퍼의 정보 설정하고 brokerHost를 설정
		BrokerHosts brokerHosts = new ZkHosts( "hadoop2.poptok.com:2181" );	
		String topicName = "Poptok-Topic";
		String zookeeperPathName = "/Poptok-Topic";

		// KafkaSpout 객체 생성 (주키퍼 연결 정보, 카프카 토픽정보, 주키퍼에 설정된 토픽 Path 정보가 필요)
		SpoutConfig spoutConf = new SpoutConfig( brokerHosts, topicName, zookeeperPathName, UUID.randomUUID().toString() );
		spoutConf.scheme = new SchemeAsMultiScheme( new StringScheme() );
		KafkaSpout kafkaSpout = new KafkaSpout( spoutConf );
		
		
		// KafkaSpout를 토폴로지에 설정 (고유ID, KafkaSpout 객체, 병렬처리 힌트를 설정)
		topologyBuilder.setSpout( "kafkaSpout", kafkaSpout, 1 );
		
		// Grouping (kafkaSpout -> splitBolt)
		// Spout 와 SplitBolt를 그룹핑해서 토폴로지를 구성
		topologyBuilder.setBolt( "splitBolt", new SplitBolt(), 1 ).allGrouping( "kafkaSpout" );
		
		// Subgrouping (splitBolt -> hbaseBolt)
		// SplitBolt와 HBaseBolt를 서브그룹핑해서 토폴로지를 구성
		//topologyBuilder.setBolt( "hbaseBolt", new HBaseBolt(), 1 ).shuffleGrouping( "splitBolt" );
		
		
		JedisPoolConfig jedisPoolConfig = new JedisPoolConfig.
				Builder().
				setHost("redis.poptok.com").
				setPort(6379).
				build();
		topologyBuilder.setBolt( "redisBolt", new RedisBolt( jedisPoolConfig ), 1 ).shuffleGrouping( "splitBolt" );
		
		
		// Grouping (kafkaSpout -> esperBolt)
		// Spout 와 EsperBolt를 그룹핑해서 토폴로지를 구성
		// topologyBuilder.setBolt( "esperBolt", new EsperBolt(), 1 ).allGrouping( "kafkaSpout" );		
		// Subgrouping [esperBolt -> redisBolt]
		
		/*
		// EsperBolt와 RedisBold를 서브그룹핑해서 토폴로지를 구성
		JedisPoolConfig jedisPoolConfig =
			new JedisPoolConfig.
			Builder().
			setHost( "redis.poptok.com" ).
			setPort( 6379 ).
			build();
		topologyBuilder.setBolt( "redisBolt", new RedisBolt( jedisPoolConfig ), 1 ).shuffleGrouping( "esperBolt" );
		*/
		
		// 토폴로지 생성
		return topologyBuilder.createTopology();
	}
}
