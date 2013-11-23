package me.mdawaffe.storm.bolt;

import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.automattic.ngs.kafka.WpcomStatsLogline;
import java.util.List;
import org.json.simple.JSONObject;

/*
 * Project a Multi-Field Tuple stream onto a Single-Field Tuple stream
 */
public class StatsByteArrayToFieldsBolt extends BaseBasicBolt {

	private String m_whichField = null;

	public StatsByteArrayToFieldsBolt() {
		super();
	}

	public StatsByteArrayToFieldsBolt( String justOneField ) {
		super();
		m_whichField = justOneField;
	}

	@Override
	public void execute( Tuple tuple, BasicOutputCollector collector ) {
		Object value = tuple.getValue( 0 );
		String logline = new String( (byte[])value );

		Values out = null;
		List fields = null;
		try {
			JSONObject objJson = WpcomStatsLogline.asJsonObject( logline );
			if ( m_whichField != null ) {
				out = new Values( (String)objJson.get( m_whichField ) );
			} else {
				out = new Values();
				fields = WpcomStatsLogline.asValuesList( logline );
				out.addAll( fields );
			}
		} catch ( Exception ex ) {
			// Don't emit anything
			return;
		}

		// collector.emit( new Values( value.getClass().getName() ) );
		collector.emit( out );
	}

	@Override
	public void declareOutputFields( OutputFieldsDeclarer ofd ) {
		if ( m_whichField != null ) {
			ofd.declare( new Fields( m_whichField ) );
		} else {
			ofd.declare( new Fields( WpcomStatsLogline.getFieldList() ) );
		}
	}
}
