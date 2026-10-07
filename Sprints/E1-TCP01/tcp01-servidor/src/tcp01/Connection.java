package tcp01;

import java.io.*;
import java.net.*;

public class Connection extends Thread {
    DataInputStream in;
    DataOutputStream out;
    Socket clientSocket;

    public Connection(Socket aClientSocket) {
        try {
            clientSocket = aClientSocket;
            in = new DataInputStream(clientSocket.getInputStream());
            out = new DataOutputStream(clientSocket.getOutputStream());
            this.start();                                       // executa run() numa thread separada
        } catch (IOException e) {
            System.out.println("Connection: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            ObjectInputStream ois = new ObjectInputStream(clientSocket.getInputStream());
            Person p = (Person) ois.readObject();      // cast obrigatório (CA3)

            // 4.2:
            //out.writeUTF(p.getName());
            // 4.3: responder com a localidade
            out.writeUTF(p.getPlace().getLocality());

        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println("ClassNotFound: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            try { clientSocket.close(); }
            catch (IOException e) { /* falha ao fechar */ }
        }
    }
}
