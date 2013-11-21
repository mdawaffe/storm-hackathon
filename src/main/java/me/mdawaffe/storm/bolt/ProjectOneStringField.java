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
public class ProjectOneStringField extends BaseBasicBolt {
	String field;

	public ProjectOneStringField( String field ) {
		super();
		this.field = field;
	}

	@Override
	public void execute( Tuple tuple, BasicOutputCollector collector ) {
		String value = tuple.getStringByField( this.field );

		collector.emit( new Values( value ) );
	}

	@Override
	public void declareOutputFields( OutputFieldsDeclarer ofd ) {
		ofd.declare( new Fields( this.field ) );
	}
}
