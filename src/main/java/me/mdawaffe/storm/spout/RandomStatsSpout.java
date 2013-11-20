package me.mdawaffe.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.Date;
import java.text.SimpleDateFormat;

public class RandomStatsSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	Random _rand;

	String[] hosts;
	String[] jetpackVersions;
	String[] countries;
	String[] agents;
	String[] sources;
	String[] refs;

	SimpleDateFormat dateTimeFormat = new SimpleDateFormat( "y-M-d k:m:s" );

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();

		hosts = new String[] {
			"datap2.wordpress.com",
			"upandright.wordpress.com",
			"simperiump2.wordpress.com",
			"elasticsearchp2.wordpress.com",
		};

		jetpackVersions = new String[] {
			null,
			"1:2.4.2",
			"1:2.5",
			"1:2.3.1",
		};

		countries = new String[] {
			"US",
			"IS",
			"PT",
			"CR",
		};

		agents = new String[] {
			"Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36",
			"Opera/9.80 (J2ME/MIDP; Opera Mini/5.0.16823/32.1125; U; en) Presto/2.8.119 Version/11.10",
			"Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)",
			"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0",
			"Mozilla/5.0 (Windows NT 6.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36",
			"Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_2 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Mobile/11A501 Safari/7534.48.3 TeamStream (iPhone5,2 408 2.9.1)"
		};

		sources = new String[] {
			"wpcom",
			"ext",
		};

		refs = new String[] {
			"",
			"",
			"http://updateomattic.wordpress.com/",
			"http://dotcom.wordpress.com/",
			"http://autocooler.wordpress.com/",
		};
	}
	
	@Override
	public void nextTuple() {
		Utils.sleep(100);

		String host = hosts[_rand.nextInt( hosts.length )];
		String jetpackVersion = jetpackVersions[_rand.nextInt( jetpackVersions.length )];
		String agent = agents[_rand.nextInt( agents.length )];
		String country = countries[_rand.nextInt( countries.length )];
		String source = sources[_rand.nextInt( sources.length )];

		String postID = "" + ( _rand.nextInt( 999 ) + 1 );
		String blogID = "" + ( _rand.nextInt( 29999999 ) + 1 );
		String ipAddress = "10." + _rand.nextInt( 255 ) + "." + _rand.nextInt( 255 ) + "." + _rand.nextInt( 255 );
		String timeZone = "" + ( _rand.nextInt( 20 ) - 10 );
		String userID = null;
		if ( 0 == _rand.nextInt( 1 ) ) {
			userID = "" + ( _rand.nextInt( 29999999 ) + 1 );
		}
		String subd = null;
		if ( 0 == _rand.nextInt( 4 ) ) {
			subd = "something" + _rand.nextInt( 99 );
		}

		Date now = new Date();
		String timestamp = dateTimeFormat.format( now );

		String region = null;

		_collector.emit( new Values( host, jetpackVersion, agent, country, source, postID, blogID, ipAddress, timeZone, userID, subd, timestamp, region ) );
	}        
	
	@Override
	public void ack(Object id) {
	}
	
	@Override
	public void fail(Object id) {
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields( "host", "jetpackVersion", "agent", "country", "source", "postID", "blogID", "ipAddress", "timeZone", "userID", "subd", "timestamp", "region" ) );
	}
	
}
