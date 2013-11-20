package me.mdawaffe.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Values;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.TotalRankingsBolt;

import me.mdawaffe.storm.spout.RandomStatsSpout;
import me.mdawaffe.storm.bolt.ProjectOneStringField;

/**
 * Stats -> Projection by Country -> Trending Ranks
 */
public class TrendingCountriesTopology {

	public static void main( String[] args ) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout( "stats", new RandomStatsSpout(), 1 );

		builder.setBolt( "country", new ProjectOneStringField( "country" ), 1 ).localOrShuffleGrouping( "stats" );

		builder.setBolt( "counter", new RollingCountBolt( 30, 5 ),      1 ).fieldsGrouping( "country", new Fields( "country" ) );
		builder.setBolt( "rank",    new IntermediateRankingsBolt( 10 ), 1 ).fieldsGrouping( "counter", new Fields( "obj" ) );
		builder.setBolt( "total",   new TotalRankingsBolt( 10 ),        1 ).globalGrouping( "rank" );
	
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
