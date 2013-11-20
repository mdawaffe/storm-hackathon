package me.mdawaffe.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.starter.bolt.PrinterBolt;

/**
 * RandomCharacterSpout -> Characters Characters -> WordFinderBolt -> Words Words ->
 * CharacterCountBolt -> Integers Anything? -> TrendingCountBolt -> Trending Counts
 */
public class TrendingCountryTopology {

	public static void main( String[] args ) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		// http://stackoverflow.com/questions/17807292/kafkaspout-is-not-receiving-anything-from-kafka
		ZkHosts zkHosts = new ZkHosts( "localhost:9092" );
		SpoutConfig spoutConf = new SpoutConfig( zkHosts,
						"test", // name of topic used by producer & consumer
						"/tmp/zookeeper", // zookeeper root path for offset storing
						"KafkaSput" );

		builder.setSpout( "spout", new KafkaSpout( spoutConf ) );


		builder.setBolt( "printaggreator", new PrinterBolt() ).shuffleGrouping( "logline" );
		// builder.setBolt( "counter", new RollingCountBolt( 30, 5 ),      4 ).fieldsGrouping(  "logline", new Fields( "country" ) );
		// builder.setBolt( "rank",    new IntermediateRankingsBolt( 10 ), 4 ).fieldsGrouping(  "counter", new Fields( "obj" ) );
		// builder.setBolt( "total",   new TotalRankingsBolt( 10 )           ).globalGrouping(  "rank" );

		Config conf = new Config();
		conf.setDebug( true );

		if ( args != null && args.length > 0 ) {
			conf.setNumWorkers( 1 );

			StormSubmitter.submitTopology( args[0], conf, builder.createTopology() );
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology( "test", conf, builder.createTopology() );
			Utils.sleep( 10000 );
			cluster.killTopology( "test" );
			cluster.shutdown();
		}
	}
}
