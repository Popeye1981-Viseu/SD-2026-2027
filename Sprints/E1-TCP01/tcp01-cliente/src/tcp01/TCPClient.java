package tcp01;

import java.io.*;
import java.net.*;

public class TCPClient {
    public static void main(String[] args) {
        Socket s = null;
        try {
            int serverPort = 7896;
            s = new Socket("localhost", serverPort);

            // 1. construir o objeto (o Place vai dentro, não é escrito em separado)
            Place place = new Place("4900-000", "Viana do Castelo");
            Person person = new Person("Ana", place, 2003);

            // 2. enviar o objeto
            ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
            oos.writeObject(person);
            oos.flush();

            // 3. receber a resposta como texto
            DataInputStream in = new DataInputStream(s.getInputStream());
            String data = in.readUTF();
            System.out.println("Received: " + data);

        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (s != null) {
                try { s.close(); }
                catch (IOException e) { System.out.println("close: " + e.getMessage()); }
            }
        }
    }
}
