package tcp01;

import java.io.*;
import java.net.*;

public class TCPServer {
    public static void main(String[] args) {
        try {
            int serverPort = 7896;
            ServerSocket listenSocket = new ServerSocket(serverPort);   // fica à escuta no porto 7896
            System.out.println("Servidor à escuta no porto " + serverPort);
            while (true) {
                Socket clientSocket = listenSocket.accept();            // BLOQUEIA até um cliente se ligar
                Connection c = new Connection(clientSocket);            // trata o cliente noutra thread
            }
        } catch (IOException e) {
            System.out.println("Listen: " + e.getMessage());
        }
    }
}