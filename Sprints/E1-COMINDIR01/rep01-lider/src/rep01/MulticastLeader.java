package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class MulticastLeader {
    // Endereço de grupo multicast e porto usados por líder e réplicas
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        // try-with-resources: fecha automaticamente o socket e o Scanner no fim
        try (MulticastSocket socket = new MulticastSocket();
             Scanner sc = new Scanner(System.in)) {

            // Resolve o endereço de grupo a partir da string "230.0.0.1"
            InetAddress group = InetAddress.getByName(GROUP);

            // TTL = 1: o datagrama não sai da rede local
            socket.setTimeToLive(1);

            // Ficheiro onde o líder guarda os registos localmente
            RecordFile file = new RecordFile("lider.txt");

            // Lê o último número de sequência já gravado (0 se o ficheiro não existir),
            // para continuar a numeração caso o líder seja reiniciado
            long seq = file.lastSeq();

            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'rajada <n>' | 'sair'");

            // Ciclo principal: lê comandos/registos da consola até o utilizador sair
            while (true) {
                System.out.print("> ");

                // Bloqueia à espera de uma linha do utilizador; sai se o stdin fechar (EOF)
                if (!sc.hasNextLine()) break;

                // Lê a linha e remove espaços em branco a mais nas extremidades
                String line = sc.nextLine().trim();

                // Comando de saída
                if (line.equalsIgnoreCase("sair")) break;

                // ===== COMANDO: rajada <n> =====
                // Gera e envia n registos o mais depressa possível, sem pausas nem prints
                // por registo. Serve para medir débito, latência e provocar perdas.

                // Divide a linha primeiro para verificar o comando exato
                String[] parts = line.split("\\s+");

                // ===== COMANDO: rajada <n> =====
                if (parts.length == 2 && parts[0].equals("rajada")) {
                    // Converte o segundo token para inteiro; valida o formato
                    int n;
                    try {
                        n = Integer.parseInt(parts[1]);
                    } catch (NumberFormatException e) {
                        System.out.println("Número inválido: " + parts[1]);
                        continue;
                    }

                    // NOVO: rejeita rajadas de tamanho zero ou negativo
                    if (n <= 0) {
                        System.out.println("Número inválido (tem de ser > 0): " + n);
                        continue;
                    }

                    // t0 e t1 marcam o instante do primeiro e do último envio (em ms)
                    long t0 = 0, t1 = 0;

                    // Ciclo interno da rajada: sem pausas, sem I/O de consola
                    for (int i = 0; i < n; i++) {
                        // Gera um registo com o sensor "SIM" e temperatura aleatória [15, 30)
                        SensorRecord r = SensorRecord.now(++seq, "SIM", 15 + Math.random() * 15);

                        // 1) Escreve primeiro localmente (o líder é a fonte da verdade)
                        file.append(r);

                        // Converte a linha de texto em bytes UTF-8 para envio
                        byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);

                        // Marca o instante do primeiro envio
                        if (i == 0) t0 = System.currentTimeMillis();

                        // 2) Envia para o grupo multicast (um único datagrama por registo)
                        socket.send(new DatagramPacket(m, m.length, group, PORT));

                        // Marca o instante do último envio
                        if (i == n - 1) t1 = System.currentTimeMillis();
                    }

                    // Reporta só o resultado agregado no fim da rajada
                    System.out.println("Rajada de " + n + " | t0=" + t0 + " | t1=" + t1 + " | duração=" + (t1 - t0) + " ms");
                    continue;   // não cai na validação normal de <sensor> <temperatura>
                }
                // ===== FIM DO COMANDO rajada =====

                // ===== REGISTO NORMAL: <sensor> <temperatura> =====
                // Divide a linha por espaços em branco
                String[] p = line.split("\\s+");

                // Tem de ter exatamente 2 campos: sensor e temperatura
                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }

                // Converte a temperatura para double; valida o formato
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }

                // Cria o registo com o próximo número de sequência
                SensorRecord r = SensorRecord.now(++seq, p[0], temp);

                // 1) Escreve localmente antes de propagar
                file.append(r);

                // Converte para bytes UTF-8
                byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);

                // 2) Envia para o grupo multicast
                socket.send(new DatagramPacket(m, m.length, group, PORT));

                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}