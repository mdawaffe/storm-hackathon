package me.mdawaffe;

import java.io.*;
import java.util.HashSet;

public class read {
	public static void main(String[] args) {
		BufferedReader wordBuffer;

		String bob = "ABC";
		bob += "DEF";

		System.out.println( bob );
		System.out.println( bob.length() );
		System.out.println( "A".length() );

		System.out.println( "A" + 3 );
		System.out.println( 3 + "!!!" );

		try {
			wordBuffer = new BufferedReader(new FileReader( "./words.txt" ) );
		} catch ( FileNotFoundException e ) {
			System.out.println( "NOT FOUND" );
			return;
		}

		HashSet<String> words = new HashSet();

		try {
			String word;
			do {
				word = wordBuffer.readLine();
				if ( word != null ) {
					words.add( word );
				}
			} while ( word != null );
		} catch ( Exception e ) {
			e.printStackTrace();
		}
		System.out.println( words.size() );
	}
}
