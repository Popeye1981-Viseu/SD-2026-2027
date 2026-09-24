import java.net.*;
import java.io.*;

public class UDPServer {

  public static void main(String args[]) {
    DatagramSocket aSocket = null;
    int L = 0; // last in-order message number

    try {
      aSocket = new DatagramSocket(6789);
      System.out.println("UDPServer listening on port 6789");

      while (true) {
        try {
          byte[] buffer = new byte[1000];
          DatagramPacket request = new DatagramPacket(buffer, buffer.length);
          aSocket.receive(request);

          String received = new String(request.getData(), 0, request.getLength(), "UTF-8");
          System.out.println("Received: " + received + " from " + request.getAddress() + ":" + request.getPort());

          String toSend;
          int N = -1;
          boolean malformed = false;

          int comma = received.indexOf(',');
          if (comma == -1) {
            malformed = true;
          } else {
            String ns = received.substring(0, comma).trim();
            try {
              N = Integer.parseInt(ns);
            } catch (NumberFormatException e) {
              malformed = true;
            }
          }

          if (malformed) {
            toSend = "waitingfor," + (L + 1);
            System.out.println("Malformed message, replying: " + toSend);
          } else if (N != L + 1) {
            toSend = "waitingfor," + (L + 1);
            System.out.println("Out of order (got " + N + ", expected " + (L + 1) + "), replying: " + toSend);
          } else {
            toSend = received; // echo
            L = N;
            System.out.println("In-order message accepted. L updated to " + L);
          }

          byte[] replyBytes = toSend.getBytes("UTF-8");
          DatagramPacket reply = new DatagramPacket(replyBytes, replyBytes.length, request.getAddress(), request.getPort());
          aSocket.send(reply);

        } catch (IOException e) {
          System.out.println("IO in loop: " + e.getMessage());
        } catch (Exception e) {
          System.out.println("Unexpected error: " + e.getMessage());
        }
      }

    } catch (SocketException e) {
      System.out.println("Socket: " + e.getMessage());
    } finally {
      if (aSocket != null) aSocket.close();
    }
  }
}
