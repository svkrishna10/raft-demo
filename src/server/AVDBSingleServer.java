package server;

import client.AVDBClient;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;

public class AVDBSingleServer extends SingleServer{
    final private Session session;
    final private Cluster cluster;

    public AVDBSingleServer(InetSocketAddress isa, InetSocketAddress isaDB,
                            String keyspace) throws IOException {
        super(isa, isaDB, keyspace);
        session = (cluster=Cluster.builder().addContactPoint("127.0.0.1")
                .build()).connect("demo");
    }
    // TODO: process bytes received from clients here
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        // simple echo server
        try {
            log.log(Level.INFO, "{0} received message from {1} {2}", new
                    Object[]
                    {this.clientMessenger.getListeningSocketAddress(), header
                            .sndr, new String(bytes)});
            String request = new String(bytes, SingleServer.DEFAULT_ENCODING);
            JSONObject json = null;
            try {
                json = new JSONObject(request);
                request = json.getString(AVDBClient.Keys.REQUEST
                        .toString());
            } catch (JSONException e) {
                //e.printStackTrace();
            }

            ResultSet results = session.execute(request);
            String response="";
            for(Row row : results) {
                response += row.toString();
            }
            if(json!=null) try {
                json.put(AVDBClient.Keys.RESPONSE.toString(),
                        response);
                response = json.toString();
            } catch (JSONException e) {
                e.printStackTrace();
            }
            System.out.println(this.clientMessenger
                    .getListeningSocketAddress() + " executed " +
                    request + " and sending response " +"["+response+"]");
            this.clientMessenger.send(header.sndr, response.getBytes
                    (SingleServer.DEFAULT_ENCODING));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        super.close();
        session.close();
        cluster.close();
    }

    public static void main(String[] args) throws IOException {
        new AVDBSingleServer(new InetSocketAddress
                ("localhost", 1999), new InetSocketAddress("localhost",
                9042), "demo")//.close()
         ;
    }
}
