package me.mdawaffe.storm.bolt;

import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/*
 * Project a Multi-Field Tuple stream onto a Single-Field Tuple stream
 */
public class WTFBolt extends BaseBasicBolt {
	String field;

	public WTFBolt() {
		super();
	}

	@Override
	public void execute( Tuple tuple, BasicOutputCollector collector ) {
		String value = tuple.getString( 0 );

		collector.emit( new Values( value.getClass().toString() ) );
	}

	@Override
	public void declareOutputFields( OutputFieldsDeclarer ofd ) {
		ofd.declare( new Fields( this.field ) );
	}
}
