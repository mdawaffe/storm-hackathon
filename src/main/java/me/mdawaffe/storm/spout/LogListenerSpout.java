package me.mdawaffe.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.automattic.ngs.kafka.LogListenerService;
import com.automattic.ngs.kafka.WpcomStatsLogline;
import com.automattic.ngs.kafka.iLogHandler;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LogListenerSpout extends BaseRichSpout implements iLogHandler {

	private SpoutOutputCollector _collector;
	private LogListenerService m_listener = null;
	// @todo Maybe make this a JsonObject
	private transient Queue<String> m_queue = new ConcurrentLinkedQueue<String>();

	@Override
	// @todo Allow rejection of this line (if queue full or something), so log listener can pass it somewhere else?
	public void hereYouGo( String logLine ) {
		System.out.println( "=====> just got (adding to queue):" + logLine.substring( 0, 30 ) + "..." );
		m_queue.add( logLine );
	}

	@Override
	public void open( Map conf, TopologyContext context, SpoutOutputCollector collector ) {
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		System.out.println( "=====> nextTuple begin, queue size = " + m_queue.size() );
		if ( m_queue.isEmpty() ) {
			System.out.println( "=====> Nothing to emit" );
			return;
		}
		String logLine = m_queue.remove();
		System.out.println( "=====> About to emit:" + logLine.substring( 0, 30 ) + "..." );

		try {
			_collector.emit( new Values( WpcomStatsLogline.asValuesList( logLine ) ) );
		} catch ( Exception ex ) {
			System.err.println( "=====> Exception in _collector.emit(): " + ex );
			ex.printStackTrace();
		}
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
		// Order is important here
		for ( String field : fieldList ) {
			System.out.println( "=====> Declaring " + field );
		}
		declarer.declare( new Fields( fieldList ) );
	}
}
