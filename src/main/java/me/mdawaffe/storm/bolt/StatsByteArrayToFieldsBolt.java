package me.mdawaffe.storm.bolt;

import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.automattic.ngs.kafka.WpcomStatsLogline;
import java.util.List;
import org.apache.log4j.Logger;

/*
 * Project a Multi-Field Tuple stream onto a Single-Field Tuple stream
 */
public class StatsByteArrayToFieldsBolt extends BaseBasicBolt {

	protected Logger log;

	public StatsByteArrayToFieldsBolt() {
		super();
		log = Logger.getLogger(getClass().getName());
	}

	@Override
	public void execute( Tuple tuple, BasicOutputCollector collector ) {
		Object value = tuple.getValue( 0 );
		String strJson = new String( (byte[])value );

		log.debug( "=====> Got json line, turning into fields [" + strJson + "]" );

		List fields = null;
		try {
			fields = WpcomStatsLogline.asValuesList( strJson );
		} catch ( Exception ex ) {
			log.debug( "=====> Error interpreting log line: [" + strJson + "]" );
			// @todo How to skip emitting?
			return;
		}

		Values out = new Values();
		out.addAll( fields );

		// collector.emit( new Values( value.getClass().getName() ) );
		collector.emit( new Values( out ) );
	}

	@Override
	public void declareOutputFields( OutputFieldsDeclarer ofd ) {
		ofd.declare( new Fields( WpcomStatsLogline.getFieldList() ) );
	}
}
