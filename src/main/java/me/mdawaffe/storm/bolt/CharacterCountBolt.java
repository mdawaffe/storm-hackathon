package me.mdawaffe.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * Calculates string length
 */
public class CharacterCountBolt extends BaseBasicBolt {
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.getString( 0 );

		System.out.println( word );

		collector.emit( new Values( word.length() ) );
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare( new Fields( "length" ) );
	}
}
