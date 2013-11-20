package me.mdawaffe.storm.spout;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.simple.JSONObject;

/**
 * This class has knowledge of the nginx log file format
 * and can transform them to JSON.
 * @author mremy
 */
public class WpcomStatsLogline {

	private static final Pattern s_loglinePattern = Pattern.compile( "^([0-9\\.]+) ([A-Z\\-]+) ([A-Z\\-]+) \\[([^\\]]*?)\\] \"GET /g.gif\\?(\\S+) HTTP/[0-9\\.]+\" \"([^\"]+)\" \"([^\"]+)\"$" );
	private static final HashSet<String> s_pixelParamWhitelist = new HashSet<String>();

	static {
		s_pixelParamWhitelist.add( "ip" );
		s_pixelParamWhitelist.add( "country" );
		s_pixelParamWhitelist.add( "region" );
		s_pixelParamWhitelist.add( "timestamp" );
		s_pixelParamWhitelist.add( "agent" );
		s_pixelParamWhitelist.add( "v" );
		s_pixelParamWhitelist.add( "user_id" );
		s_pixelParamWhitelist.add( "blog" );
		s_pixelParamWhitelist.add( "post" );
		s_pixelParamWhitelist.add( "ref" );
		s_pixelParamWhitelist.add( "tz" );
		s_pixelParamWhitelist.add( "subd" );
		s_pixelParamWhitelist.add( "j" );
		s_pixelParamWhitelist.add( "host" );
	}
	// This is for testing only
	private static boolean s_sendToKafka = false;
	private static final Object s_monitor = new Object();
	private static SimpleDateFormat s_logTimestampFormat = new SimpleDateFormat( "dd/MMM/yyyy:HH:mm:ss Z" );
	private static SimpleDateFormat s_outTimestampFormat = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );

	public static List<String> getFieldList() {
		List<String> retval = new ArrayList<String>();
		retval.addAll( s_pixelParamWhitelist );
		return retval;
	}

	/**
	 *
	 * @param logline A line from the nginx logs
	 * @return A JSON string containing the interesting parts from the log line.
	 * @throws me.mdawaffe.storm.spout.WpcomStatsLogline.MalformedLoglineException in case the line didn't
	 * match the expected pattern.
	 * @throws IOException if there's an error encoding the line as JSON or something.
	 */
	public static String toJson( String logline ) throws IOException, MalformedLoglineException {

		Matcher m = s_loglinePattern.matcher( logline );
		if ( m.find() ) {
			String ip = m.group( 1 );
			String countryCode = m.group( 2 );
			String regionCode = m.group( 3 );
			String timestamp = m.group( 4 );
			String gifQuery = m.group( 5 );
			String referrer = m.group( 6 );
			String userAgent = m.group( 7 );

			/*
			 * Build the object to hold this stuff, don't include empty values to save space
			 */
			JSONObject pv = new JSONObject();

			// IP is always there
			pv.put( "ip", ip );

			// Simplify the timestamp (all timestamps UTC in the stats logs)
			java.util.Date ts = s_logTimestampFormat.parse( timestamp, new ParsePosition( 0 ) );
			pv.put( "timestamp", s_outTimestampFormat.format( ts ) );

			// Country is probably there most of the time
			if ( 0 != countryCode.compareTo( "-" ) ) {
				pv.put( "country", countryCode );
			}

			// Region is only there for U.S. and Canada
			if ( 0 != regionCode.compareTo( "-" ) ) {
				pv.put( "region", regionCode );
			}

			// The original referrer is not intereating, it's the same as the page
			// Below we send the ref param from the pixel query, which is the referrer from the pageview

			// User Agent is always there
			pv.put( "agent", userAgent );

			// Extract the pixel's params
			String[] pixelParamChunks = gifQuery.split( "[\\&\\?]" );

			for ( int i = 0; i < pixelParamChunks.length; i++ ) {
				// @todo What's the current behavior for repeat keys, e.g. x_statsbucket
				int equals = pixelParamChunks[i].indexOf( "=" );
				if ( equals < 0 ) {
					// @todo Log an exception, this is a problem, malformed keyval
				}

				String key = pixelParamChunks[i].substring( 0, equals );

				if ( ! s_pixelParamWhitelist.contains( key ) ) {
					continue;
				}

				String val = URLDecoder.decode( pixelParamChunks[i].substring( equals + 1 ), "UTF-8" );

				// @todo Kinda sucks taking the key names from upstream,
				// should we normalize here?
				// @todo Try to make Avro work.
				pv.put( key, val );
			}

			// Set whitelist items that don't exist to null
			for ( String key : s_pixelParamWhitelist ) // To console as String
			{
				 if ( ! pv.containsKey( key) ) {
					 pv.put( key, null );
				 }
			}

			StringWriter out = new StringWriter();
			pv.writeJSONString( out );
			String jsonText = out.toString();

			return jsonText;
		}

		// @todo Custom exception?
		throw new MalformedLoglineException( "Malformed logline: " + logline );
	}

	static class MalformedLoglineException extends Exception {
		public MalformedLoglineException( String msg ) {
			super( msg );
		}
	}
}
