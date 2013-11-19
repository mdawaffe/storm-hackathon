package me.mdawaffe.storm.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.io.*;
import java.util.Map;
import java.util.HashSet;
import java.lang.NullPointerException;


public class WordFinderBolt extends BaseBasicBolt {
	private HashSet<String> words;
	private String recentChars = "";

	@Override
	public void prepare( Map stormConf, TopologyContext context ) {
		BufferedReader wordBuffer = new BufferedReader( new InputStreamReader( getClass().getResourceAsStream( "/me/mdawaffe/words.txt" ) ) );

		this.words = new HashSet();

		try {
			String word;
			do {
				word = wordBuffer.readLine();
				if ( word != null ) {
					this.words.add( word );
				}
			} while ( word != null );
		} catch ( Exception e ) {
			e.printStackTrace();
		}
		try {
			wordBuffer.close();
		} catch( IOException e ) {}
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		int i;
		int recentCharsLength;
		String trialWord = "";

		this.recentChars += tuple.getString( 0 );

		recentCharsLength = this.recentChars.length();
		if ( recentCharsLength > 24 ) {
			this.recentChars = this.recentChars.substring( this.recentChars.length() - 24 );
			recentCharsLength = this.recentChars.length();
		}

		if ( recentCharsLength < 3 ) {
			return;
		}
		
		for ( i = 3; i < recentCharsLength; i++ ) {
			trialWord = this.recentChars.substring( 0, i );
			if ( trialWord == null ) {
				System.out.println( "NULL TRIAL WORD: " + this.recentChars );
				continue;
			}

			try {
				if ( this.words.contains( trialWord ) ) {
					collector.emit( new Values( trialWord ) );
				}
			} catch( NullPointerException e ) {
				System.out.println( "NullPointerException: " + this.recentChars + " i:" + i );
				continue;
			}
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare( new Fields( "word" ) );
	}
}
