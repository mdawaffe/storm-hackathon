package me.mdawaffe.storm.bolt;

import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.automattic.ngs.kafka.WpcomStatsLogline;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;

/*
 * Project a Multi-Field Tuple stream onto a Single-Field Tuple stream
 */
public class StatsByteArrayToString extends BaseBasicBolt {

	private String m_whichField = null;

	public StatsByteArrayToString() {
		super();
	}

	public StatsByteArrayToString( String justOneField ) {
		super();
		m_whichField = justOneField;
	}

	@Override
	public void execute( Tuple tuple, BasicOutputCollector collector ) {
		Object obj = tuple.getValue( 0 );
		String logline = new String( (byte[])obj );
		Values out = null;
		try {
			if ( m_whichField != null ) {
				JSONObject objJson = WpcomStatsLogline.asJsonObject( logline );
				StringBuffer sbufDebug = new StringBuffer();
				sbufDebug.append( "Object thingy: " );
				for ( Object key : objJson.keySet() ) {
					sbufDebug.append( "" + key + ":" + objJson.get( key ) + ", " );
				}
				out = new Values( sbufDebug.toString() );
				// out = new Values( (String)objJson.get( m_whichField ) );
			} else {
				out = new Values( logline );
			}
		} catch ( Exception ex ) {
			out = new Values( "Exception in bolt: " + ex );
			// Nothing to emit
			// @todo Log the error
			// return;
		}
		collector.emit( out );
	}

	@Override
	public void declareOutputFields( OutputFieldsDeclarer ofd ) {
		ofd.declare( new Fields( "json" ) );
	}
}
