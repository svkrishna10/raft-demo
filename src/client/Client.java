package client;

import edu.umass.cs.nio.interfaces.NodeConfig;
import server.SingleServer;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.nioutils.NIOHeader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * @author arun
 * <p>
 * This class implements a simple client to send requests and receive responses
 * using non-blocking IO.
 * <p>
 * Starting points for student code are methods marked TODO.
 */
public class Client {
	private final MessageNIOTransport<String, byte[]> nio;

	public Client() throws IOException {
		this.nio = new MessageNIOTransport<String, byte[]>(InetAddress.getByName("localhost"), 0, new
                AbstractBytePacketDemultiplexer() {
			@Override
			public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
				handleResponse(bytes, nioHeader);
				return true;
			}
		});
	}

	// TODO: process responses received from server
	protected void handleResponse(byte[] bytes, NIOHeader header) {
		// expect echo reply by default here
		try {
			System.out.println(new String(bytes, SingleServer
                    .DEFAULT_ENCODING));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	/**
	 * A simple interface to invoke a callback method upon a response.
	 */
	public static interface Callback {
		public void handleResponse(byte[] bytes, NIOHeader header);
	}

	/**
	 * This method will simply send the request in a non-blocking, reliable,
	 * in-order manner. This final method can not be overwritten by children.
	 *
	 * @param isa
	 * @param request
	 * @throws IOException
	 */
	public final void send(InetSocketAddress isa, String request) throws
            IOException {
		this.nio.send(isa, request.getBytes(SingleServer.DEFAULT_ENCODING));
	}

	/**
	 * TODO: This method, unlike the simple send above, should invoke the
	 * supplied callback argument's handleResponse method upon receiving the
	 * corresponding response from the remote end.
	 *
	 * @param isa
	 * @param request
	 * @param callback
	 */
	public void callbackSend(InetSocketAddress isa, String request, Callback
            callback) throws IOException {
		throw new RuntimeException("To be implemented");
	}

	public void close() {
		this.nio.stop();
	}

	/**
	 * @param args Default server address is localhost:{@link
	 *             SingleServer#DEFAULT_PORT}
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		new Client().send(SingleServer.getSocketAddress(args), "hello");
	}
}