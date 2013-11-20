package me.mdawaffe.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LogListenerSpout extends BaseRichSpout implements iLogHandler {

	SpoutOutputCollector _collector;
	LogListenerService m_listener = null;
	// @todo Maybe make this a JsonObject
	Queue<String> m_queue = new ConcurrentLinkedQueue<String>();

	@Override
	// @todo Allow rejection of this line (if queue full or something), so log listener can pass it somewhere else?
	public void hereYouGo( String logLine ) {
		System.out.println( "just got (adding to queue):" + logLine.substring( 0, 30 ) + "..." );
		m_queue.add( logLine );
	}

	@Override
	public void open( Map conf, TopologyContext context, SpoutOutputCollector collector ) {
		_collector = collector;
		System.out.println( "=====> LogListenerSpout::open()" );

		try {
			m_listener = new LogListenerService( 40001, 1, InetAddress.getByName( "localhost" ), this );
			m_listener.serve();
		} catch ( Exception ex ) {
			// @todo log4j
			ex.printStackTrace();
			// Now what
		}
	}

	@Override
	public void nextTuple() {
		// System.out.println( "=====> nextTuple begin" );
		if ( m_queue.isEmpty() ) {
			// System.out.println( "=====> Nothing to emit" );
			// return;
		}
		// String logLine = m_queue.remove();
		// System.out.println( "About to emit:" + logLine.substring( 0, 30 ) + "..." );
		// _collector.emit( new Values( logLine ) );
		_collector.emit( new Values( "US" ) );
	}

	@Override
	public void ack( Object id ) {
		System.out.println( "=====> LogListenerSpout ack" );
	}

	@Override
	public void fail( Object id ) {
		System.out.println( "=====> LogListenerSpout fail" );
	}

	@Override
	public void deactivate() {
		System.out.println( "=====> LogListenerSpout deactivated" );
	}

	@Override
	public void declareOutputFields( OutputFieldsDeclarer declarer ) {
		List<String> fieldList = WpcomStatsLogline.getFieldList();
		/*
		for ( String field : fieldList ) {
			System.out.println( field );
		}
		*/
		// declarer.declare( new Fields( fieldList ) );
		declarer.declare( new Fields( "country" ) );
	}
}
