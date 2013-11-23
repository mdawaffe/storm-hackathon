package me.mdawaffe.storm.bolt;

import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WTFBolt extends BaseBasicBolt {
	String field;

	public WTFBolt() {
		super();
	}

	@Override
	public void execute( Tuple tuple, BasicOutputCollector collector ) {
		Object value = tuple.getValue( 0 );
		collector.emit( new Values( value.getClass().getName() ) );
	}

	@Override
	public void declareOutputFields( OutputFieldsDeclarer ofd ) {
		ofd.declare( new Fields( "bob" ) );
	}
}
