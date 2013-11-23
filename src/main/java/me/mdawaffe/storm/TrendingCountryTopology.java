package me.mdawaffe.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import me.mdawaffe.storm.bolt.ProjectOneStringField;
import me.mdawaffe.storm.bolt.StatsByteArrayToFieldsBolt;
import me.mdawaffe.storm.bolt.StatsByteArrayToString;
import me.mdawaffe.storm.bolt.WTFBolt;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.PrinterBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;

public class TrendingCountryTopology {

	public static void main( String[] args ) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		ZkHosts zkHosts = new ZkHosts( "localhost:2181" );
		SpoutConfig spoutConf = new SpoutConfig(
						zkHosts,
						"test", // name of topic used by producer & consumer
						"/tmp/zookeeper", // zookeeper root path for offset storing
						"KafkaSpout" );

		builder.setSpout( "stats", new KafkaSpout( spoutConf ) );
		// builder.setBolt( "wtf", new WTFBolt() ).shuffleGrouping( "stats" );
		builder.setBolt( "country", new StatsByteArrayToFieldsBolt( "country" ), 1 ).shuffleGrouping( "stats" );
		// builder.setBolt( "country", new ProjectOneStringField( "country" ), 1 ).localOrShuffleGrouping( "fields" );
		builder.setBolt( "counter", new RollingCountBolt( 30, 5 ),      1 ).fieldsGrouping( "country", new Fields( "country" ) );
		builder.setBolt( "rank",    new IntermediateRankingsBolt( 10 ), 1 ).fieldsGrouping( "counter", new Fields( "obj" ) );
		builder.setBolt( "total",   new TotalRankingsBolt( 10 ),        1 ).globalGrouping( "rank" );

		// builder.setBolt( "json", new StatsByteArrayToString(), 1 ).shuffleGrouping( "stats" );
		// builder.setBolt( "json", new StatsByteArrayToString( "country" ), 1 ).shuffleGrouping( "stats" );
		// builder.setBolt( "printaggregator", new PrinterBolt()).shuffleGrouping( "json" );
		// builder.setBolt( "counter", new RollingCountBolt( 30, 5 ),      4 ).fieldsGrouping(  "logline", new Fields( "country" ) );
		// builder.setBolt( "rank",    new IntermediateRankingsBolt( 10 ), 4 ).fieldsGrouping(  "counter", new Fields( "obj" ) );
		// builder.setBolt( "total",   new TotalRankingsBolt( 10 )           ).globalGrouping(  "rank" );

		Config conf = new Config();
		conf.setDebug( true );

		conf.setNumWorkers( 1 );

		StormSubmitter.submitTopology( "test", conf, builder.createTopology() );
	}
}
