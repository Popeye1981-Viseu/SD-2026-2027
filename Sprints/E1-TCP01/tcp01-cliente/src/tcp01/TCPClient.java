package tcp01;

import java.io.*;
import java.net.*;

public class TCPClient {
    public static void main(String[] args) {
        Socket s = null;
        try {
            int serverPort = 7896;
            s = new Socket("localhost", serverPort);      // liga ao servidor; falha se não houver servidor
            DataInputStream in = new DataInputStream(s.getInputStream());          // resposta vem como texto
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());  // envio vai como objeto

            Place place = new Place("3500-000", "Viseu");
            Person person = new Person("Xico", place, 2000);

            out.writeObject(person);   // só escreve a Person; o Place vai junto automaticamente
            out.flush();               // garante que os bytes saem já

            String data = in.readUTF();                  // BLOQUEIA à espera da resposta
            System.out.println("Received: " + data);
        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());   // o servidor fechou sem responder
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());    // ex.: Connection refused, NotSerializableException
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    System.out.println("close: " + e.getMessage());
                }
            }
        }
    }
}