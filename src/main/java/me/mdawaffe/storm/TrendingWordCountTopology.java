package me.mdawaffe.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;

import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.TotalRankingsBolt;

import me.mdawaffe.storm.spout.RandomCharacterSpout;
import me.mdawaffe.storm.bolt.WordFinderBolt;
import me.mdawaffe.storm.bolt.CharacterCountBolt;

/**
 * RandomCharacterSpout -> Characters
 * Characters -> WordFinderBolt -> Words
 * Words -> CharacterCountBolt -> Integers
 * Anything? -> TrendingCountBolt -> Trending Counts
 */
public class TrendingWordCountTopology {

	public static void main( String[] args ) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout( "letter", new RandomCharacterSpout(), 1 );

		builder.setBolt( "word",    new WordFinderBolt(),               1 ).shuffleGrouping( "letter" );
		builder.setBolt( "length",  new CharacterCountBolt(),           1 ).shuffleGrouping( "word" );
		builder.setBolt( "counter", new RollingCountBolt( 30, 5 ),      1 ).fieldsGrouping(  "length", new Fields( "length" ) );
		builder.setBolt( "rank",    new IntermediateRankingsBolt( 10 ), 1 ).fieldsGrouping(  "counter", new Fields( "obj" ) );
		builder.setBolt( "total",   new TotalRankingsBolt( 10 ),        1 ).globalGrouping(  "rank" );
	
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
