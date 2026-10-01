import java.net.*;
import java.io.*;

public class UDPServer {
    public static void main(String[] args) {
        DatagramSocket socket = null;
        final int PORT = 6789;
        int L = 0; // last in-order message received

        try {
            socket = new DatagramSocket(PORT);
            System.out.println("UDPServer listening on port " + PORT);

            while (true) {
                byte[] buf = new byte[1000];
                DatagramPacket request = new DatagramPacket(buf, buf.length);

                try {
                    socket.receive(request);
                    String received = new String(request.getData(), 0, request.getLength()).trim();

                    String replyStr;
                    boolean inOrder = false;

                    // parse "N,message"
                    int comma = received.indexOf(',');
                    if (comma > 0) {
                        String nStr = received.substring(0, comma).trim();
                        String payload = received.substring(comma + 1);
                        try {
                            int N = Integer.parseInt(nStr);
                            if (N == L + 1) {
                                inOrder = true;
                                L = N; // accept and update state
                                replyStr = "ok," + L;
                                //replyStr = received; // echo the entire received message
                            } else {
                                // out of order
                                replyStr = "waitingfor," + (L + 1);
                            }
                        } catch (NumberFormatException e) {
                            // malformed N
                            replyStr = "waitingfor," + (L + 1);
                        }
                    } else {
                        // malformed: no comma
                        replyStr = "waitingfor," + (L + 1);
                    }

                    byte[] replyBytes = replyStr.getBytes();
                    DatagramPacket reply = new DatagramPacket(replyBytes, replyBytes.length,
                            request.getAddress(), request.getPort());
                    socket.send(reply);

                    // Logging for demonstration
                    System.out.println("Received: '" + received + "' from " + request.getAddress() + ":" + request.getPort());
                    System.out.println("Replied: '" + replyStr + "' | L=" + L);

                } catch (IOException e) {
                    System.err.println("IO while receiving/sending: " + e.getMessage());
                    // continue serving
                }
            }

        } catch (SocketException e) {
            System.err.println("Socket error: " + e.getMessage());
        } finally {
            if (socket != null && !socket.isClosed()) socket.close();
        }
    }
}
