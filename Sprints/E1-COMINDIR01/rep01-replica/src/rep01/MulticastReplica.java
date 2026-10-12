package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        TreeMap<Long, SensorRecord> pending = new TreeMap<>();

        // ===== Estado da medição da rajada =====
        boolean rajadaAtiva = false;
        long rajadaT0 = 0;              // instante do primeiro SIM recebido
        long rajadaT1 = 0;              // instante do último SIM recebido
        int rajadaRecebidos = 0;        // registos SIM recebidos (bem-formados)
        int rajadaEscritos = 0;         // registos SIM efetivamente gravados no ficheiro
        // =======================================

        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);
            socket.setSoTimeout(1000);   // receive() lança SocketTimeoutException após 1s de silêncio

            long last = file.lastSeq();
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT
                    + " (último seq = " + last + ")");

            byte[] buffer = new byte[1000];

            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                String line;
                try {
                    socket.receive(p);   // bloqueia até chegar datagrama ou timeout (1s)
                    line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);
                } catch (SocketTimeoutException e) {
                    // Silêncio de 1s: assume (heurística) que a rajada terminou
                    if (rajadaAtiva) {
                        System.out.println("=== FIM DA RAJADA (heurística de silêncio) ===");
                        System.out.println("t0=" + rajadaT0 + " | t1=" + rajadaT1
                                + " | duração/janela de receção=" + (rajadaT1 - rajadaT0) + " ms");
                        System.out.println("Recebidos=" + rajadaRecebidos
                                + " | Escritos=" + rajadaEscritos
                                + " | Retidos em pending=" + pending.size());
                        rajadaAtiva = false;
                        rajadaT0 = rajadaT1 = 0;
                        rajadaRecebidos = rajadaEscritos = 0;
                    }
                    continue;   // continua à escuta
                }

                // Parsing isolado: datagrama malformado não termina a réplica
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(line);
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                    continue;
                }

                long seq = r.getSeq();

                // ===== Deteção de registos da rajada (sensor SIM) =====
                boolean isRajada = "SIM".equals(r.getSensorId());
                if (isRajada) {
                    if (!rajadaAtiva) {
                        // primeiro registo SIM desta rajada
                        rajadaAtiva = true;
                        rajadaT0 = System.currentTimeMillis();
                    }
                    rajadaT1 = System.currentTimeMillis();
                    rajadaRecebidos++;
                }
                // =====================================================

                // ===== Lógica de sequência (mantida do ponto 6) =====
                if (seq <= last || pending.containsKey(seq)) {
                    System.out.println("DUPLICADO: " + seq);
                } else if (seq == last + 1) {
                    file.append(r);
                    last = seq;
                    if (isRajada) rajadaEscritos++;
                    System.out.println("Aplicado: " + r.toLine());

                    // Drena em cascata os retidos que passaram a estar em sequência
                    while (pending.containsKey(last + 1)) {
                        SensorRecord next = pending.remove(last + 1);
                        file.append(next);
                        last = next.getSeq();
                        if ("SIM".equals(next.getSensorId())) rajadaEscritos++;
                        System.out.println("Aplicado (retido): " + next.toLine());
                    }
                } else {
                    pending.put(seq, r);
                    System.out.println("EM ESPERA: " + seq + " (falta o registo " + (last + 1) + ")");
                }
                // ====================================================
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}