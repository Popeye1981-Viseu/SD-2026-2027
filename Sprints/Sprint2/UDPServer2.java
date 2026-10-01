import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDPServer2 {

    // Estado partilhado (limitação reconhecida para CA5: não separado por cliente)
    static List<String> finalMessages = new ArrayList<>();
    static Map<Integer, String> outOfOrderMessages = new HashMap<>();
    static List<String> deliveredThisStep = new ArrayList<>();

    public static int processDeliveredMessages(int nLastMessageInOrder,
                                               int nCurrentMessage,
                                               String currentMessage) {
        // Limpeza defensiva (o main também limpa, mas mantém o método auto-contido)
        deliveredThisStep.clear();

        int L = nLastMessageInOrder;

        if (nCurrentMessage == L + 1) {
            finalMessages.add(currentMessage);
            deliveredThisStep.add(currentMessage);
            L = nCurrentMessage;

            while (outOfOrderMessages.containsKey(L + 1)) {
                String nextPayload = outOfOrderMessages.remove(L + 1);
                finalMessages.add(nextPayload);
                deliveredThisStep.add(nextPayload);
                L++;
            }
        } else if (nCurrentMessage > L + 1) {
            // Só guarda se ainda não existir (duplicado na temporária é ignorado)
            if (!outOfOrderMessages.containsKey(nCurrentMessage)) {
                outOfOrderMessages.put(nCurrentMessage, currentMessage);
            }
        }
        // nCurrentMessage <= L -> duplicado já entregue -> ignorar

        return L;
    }

    public static void main(String[] args) {
        DatagramSocket socket = null;
        final int PORT = 6789;
        int L = 0;

        try {
            socket = new DatagramSocket(PORT);
            System.out.println("UDPServer listening on port " + PORT);

            while (true) {
                byte[] buf = new byte[1000];
                DatagramPacket request = new DatagramPacket(buf, buf.length);

                try {
                    socket.receive(request);
                    String received = new String(request.getData(), 0, request.getLength()).trim();

                    // Ponto 1: garantir que o log de cada datagrama começa vazio
                    deliveredThisStep.clear();

                    String replyStr = "";
                    int oldL = L;

                    int comma = received.indexOf(',');
                    if (comma > 0) {
                        String nStr = received.substring(0, comma).trim();
                        String payload = received.substring(comma + 1);
                        try {
                            int N = Integer.parseInt(nStr);

                            L = processDeliveredMessages(L, N, payload);

                            // Ponto 2: L != oldL <=> houve pelo menos uma entrega
                            if (L != oldL) {
                                // Houve pelo menos uma entrega neste passo -> echo do datagrama recebido
                                replyStr = received;
                            } else {
                                // Não houve entrega (fora de ordem, duplicado, malformado) -> waitingfor
                                replyStr = "waitingfor," + (L + 1);
                            }

                        } catch (NumberFormatException e) {
                            replyStr = "waitingfor," + (L + 1);
                        }
                    } else {
                        replyStr = "waitingfor," + (L + 1);
                    }

                    byte[] replyBytes = replyStr.getBytes();
                    DatagramPacket reply = new DatagramPacket(replyBytes, replyBytes.length,
                            request.getAddress(), request.getPort());
                    socket.send(reply);

                    System.out.println("Received: '" + received + "' from "
                            + request.getAddress() + ":" + request.getPort());
                    System.out.println("Replied: '" + replyStr + "'");
                    System.out.println("L = " + L);
                    System.out.println("Estrutura temporária: " + outOfOrderMessages);
                    System.out.println("Mensagens entregues neste passo: " + deliveredThisStep);
                    System.out.println("Lista de receção: " + finalMessages);
                    System.out.println("--------------------------------------------------");

                } catch (IOException e) {
                    System.err.println("IO while receiving/sending: " + e.getMessage());
                }
            }

        } catch (SocketException e) {
            System.err.println("Socket error: " + e.getMessage());
        } finally {
            if (socket != null && !socket.isClosed()) socket.close();
        }
    }
}