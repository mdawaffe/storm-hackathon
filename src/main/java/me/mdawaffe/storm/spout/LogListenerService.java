package me.mdawaffe.storm.spout;

import java.net.*;
import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// @todo log4j
public class LogListenerService {

	private ServerSocket m_serverSocket = null;
	private ExecutorService m_pool = null;
	private iLogHandler m_logHandler = null;

	public LogListenerService( int port, int poolSize, InetAddress ipAddress, iLogHandler logHandler ) throws IOException {
		m_logHandler = logHandler;
		System.out.println( "Listening on " + ipAddress + ":" + port );
		m_serverSocket = new ServerSocket( port, 5, ipAddress );
		m_pool = Executors.newFixedThreadPool( poolSize );
	}

	public void serve() {
		try {
			Runtime.getRuntime().addShutdownHook( new ShutdownThread( this ) );
			for ( ;; ) {
				m_pool.execute( new ConnectionHandler( m_serverSocket.accept(), m_logHandler ) );
			}
		} catch ( IOException ex ) {
			m_pool.shutdown();
		}
	}

	void shutdownAndAwaitTermination() {
		System.out.println( "=====> Shutting down LogListenerService [D1]" );
		m_pool.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if ( !m_pool.awaitTermination( 60, TimeUnit.SECONDS ) ) {
				m_pool.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if ( !m_pool.awaitTermination( 60, TimeUnit.SECONDS ) ) {
					System.err.println( "Pool did not terminate" );
				} else {
					System.out.println( "So the pool terminated, maybe, probably." );
				}
			}
		} catch ( InterruptedException ie ) {
			// (Re-)Cancel if current thread also interrupted
			m_pool.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

	class ConnectionHandler implements Runnable {

		private final Socket socket;
		iLogHandler m_logHandler = null;

		ConnectionHandler( Socket socket, iLogHandler logHandler ) {
			this.socket = socket;
			m_logHandler = logHandler;
		}

		public void run() {
			try {
				System.out.println( "=====> Reading from socket" );
				BufferedReader in = new BufferedReader( new InputStreamReader( socket.getInputStream() ) );
				String logline;
				while ( ( logline = in.readLine() ) != null ) {
					System.out.println( "=====> Read a line from socket" );

					// is this a line we care about?
					if ( logline.indexOf( "GET /g.gif" ) > 0 ) {
						String json = WpcomStatsLogline.toJson( logline );
						System.out.println( "Sending this to spout: " + json.substring( 0, 30 ) + " ..." );
						m_logHandler.hereYouGo( logline );
					}
				}
			} catch ( Exception ex ) {
				// System.out.println( "Malformed logline: " );
				ex.printStackTrace();
				/* @todo Send error code or something, for now just skipping */
			}
		}
	}

	class ShutdownThread extends Thread {

		LogListenerService m_listenerService = null;

		public ShutdownThread( LogListenerService listenerService ) {
			m_listenerService = listenerService;
		}

		public void run() {
			System.out.println( "=====> Shutting down LogListenerService" );
			m_listenerService.shutdownAndAwaitTermination();
		}
	}

	public static void main( String[] args ) {
		try {
			LogListenerService listener = new LogListenerService( 8888, 1, InetAddress.getByName( "127.0.0.1" ), null );
			listener.serve();
			Thread.sleep( 60000 ); // give us a minute to send some data to test
			// @todo Shutdown and stuff
		} catch ( Exception ex ) {
			ex.printStackTrace();
		}
	}
}
