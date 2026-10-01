import java.net.*;
import java.io.*;

public class UDPClient {
    public static void main(String[] args) {
        DatagramSocket socket = null;
        final int SERVER_PORT = 6789;

        try (BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in))) {
            socket = new DatagramSocket();
            InetAddress server = InetAddress.getByName("localhost");

            int autoSeq = 1;

            while (true) {
                System.out.print("Mode (a=auto, m=manual, q=quit): ");
                String mode = stdin.readLine();
                if (mode == null) break;
                mode = mode.trim().toLowerCase();
                if (mode.equals("q")) break;

                int seq = -1;
                if (mode.equals("a")) {
                    seq = autoSeq;
                } else if (mode.equals("m")) {
                    System.out.print("Sequence number: ");
                    String s = stdin.readLine();
                    try {
                        seq = Integer.parseInt(s.trim());
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid number, try again.");
                        continue;
                    }
                } else {
                    System.out.println("Unknown mode, choose 'a' or 'm'.");
                    continue;
                }

                System.out.print("Message: ");
                String msg = stdin.readLine();
                if (msg == null) break;

                String payload = seq + "," + msg;
                byte[] sendData = payload.getBytes();

                DatagramPacket request = new DatagramPacket(sendData, sendData.length, server, SERVER_PORT);
                socket.send(request);

                byte[] buf = new byte[1000];
                DatagramPacket reply = new DatagramPacket(buf, buf.length);
                socket.receive(reply);
                String replyStr = new String(reply.getData(), 0, reply.getLength()).trim();

                if (replyStr.startsWith("waitingfor,")) {
                    System.out.println("Server: " + replyStr);
                    // do not advance autoSeq
                } else {
                    System.out.println("Echo: " + replyStr);
                    if (mode.equals("a")) autoSeq++; // advance only on successful echo
                }
            }

        } catch (SocketException e) {
            System.err.println("Socket: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("IO: " + e.getMessage());
        } finally {
            // close socket
            try {
                if (socket != null && !socket.isClosed()) socket.close();
            } catch (Exception ignore) {}
        }
    }
}
