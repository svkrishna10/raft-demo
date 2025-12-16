package server;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.utils.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author arun
 * <p>
 * <p>
 * This class implements a simple echo server using non-blocking IO.
 * <p>
 * Starting points for student code are methods marked TODO.
 */
public class SingleServer {
	public static final int DEFAULT_PORT = 2000;
	public static final String DEFAULT_ENCODING = "ISO-8859-1";

	protected static final Logger log = Logger.getLogger(SingleServer.class
			.getName());
	protected final MessageNIOTransport<String, String> clientMessenger;

	/**
	 * @param isa      Client-facing socket address
	 * @param isaDB    Socket address of the backend data store
	 * @param keyspace Keyspace in the backend data store
	 * @throws IOException
	 */
	public SingleServer(InetSocketAddress isa, InetSocketAddress isaDB, String
			keyspace) throws IOException {
		this.clientMessenger = new MessageNIOTransport<String, String>(isa
				.getAddress(), isa.getPort(), new
				AbstractBytePacketDemultiplexer() {
			@Override
			public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
				handleMessageFromClient(bytes, nioHeader);
				return true;
			}
		});
		log.log(Level.INFO, "{0} started client messenger at {1}", new
				Object[]{this, isa});
	}

	// TODO: process bytes received from clients here
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		// simple echo server
		try {
			log.log(Level.INFO, "{0} received message from {1}", new
					Object[]{this.clientMessenger.getListeningSocketAddress(),
					header.sndr});
			this.clientMessenger.send(header.sndr, bytes);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args The first argument must be of the form [host:]port with an
	 *             optional host name or IP.
	 */
	public static InetSocketAddress getSocketAddress(String[] args) {
		return args.length > 0 && args[0].contains(":") ? Util
				.getInetSocketAddressFromString(args[0]) : new
				InetSocketAddress("localhost", args.length > 0 ? Integer
				.parseInt(args[0].replaceAll(".*:", "")) : DEFAULT_PORT);
	}

	public void close() {
		this.clientMessenger.stop();
	}

	public static void main(String[] args) throws IOException {
		new SingleServer(getSocketAddress(args), new InetSocketAddress
				("localhost", 9042), "demo");
	}

	;
}