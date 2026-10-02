package tcp01;

import java.io.*;
import java.net.*;

public class Connection extends Thread {
    DataOutputStream out;
    Socket clientSocket;

    public Connection(Socket aClientSocket) {
        try {
            clientSocket = aClientSocket;
            out = new DataOutputStream(clientSocket.getOutputStream());  // resposta vai como texto
            this.start();                                                // cria a thread e chama run()
        } catch (IOException e) {
            System.out.println("Connection: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            // criado aqui e não no construtor: este construtor BLOQUEIA até chegar o cabeçalho do cliente
            ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());

            Object obj = in.readObject();                    // BLOQUEIA até chegar o objeto; devolve Object

            if (obj instanceof Person) {
                Person p = (Person) obj;                     // cast para a classe esperada
                String localidade = p.getPlace().getLocality();   // o Place veio junto com a Person
                System.out.println("Recebido: " + p.getName() + " de " + localidade);
                out.writeUTF(localidade);                    // envia a localidade ao cliente
            } else {
                out.writeUTF("Erro: objeto recebido não é uma Person");
            }
        } catch (ClassNotFoundException e) {
            System.out.println("Classe não encontrada: " + e.getMessage());  // classe não existe deste lado
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());    // o cliente fechou a ligação antes do tempo
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());     // ex.: InvalidClassException (serialVersionUID diferente)
        } finally {
            try {
                clientSocket.close();                        // fecha a ligação deste cliente
            } catch (IOException e) {
                /* falha ao fechar */
            }
        }
    }
}