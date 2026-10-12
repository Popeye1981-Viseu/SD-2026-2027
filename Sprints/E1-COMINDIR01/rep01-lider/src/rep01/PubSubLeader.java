package rep01;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class PubSubLeader {
    static final String TOPIC = "rep01-temperaturas";
    static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;
        IpfsPubSub pubsub = new IpfsPubSub(api);

        try (Scanner sc = new Scanner(System.in)) {
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();
            System.out.println("PubSubLeader pronto (API: " + api + "). Formato: <sensor> <temperatura> | 'rajada <n>' | 'sair'");

            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;

                String[] parts = line.split("\\s+");

                // ===== COMANDO: rajada <n> =====
                if (parts.length == 2 && parts[0].equals("rajada")) {
                    int n;
                    try {
                        n = Integer.parseInt(parts[1]);
                    } catch (NumberFormatException e) {
                        System.out.println("Número inválido: " + parts[1]);
                        continue;
                    }
                    if (n <= 0) {
                        System.out.println("Número inválido (tem de ser > 0): " + n);
                        continue;
                    }

                    long t0 = 0, t1 = 0;
                    for (int i = 0; i < n; i++) {
                        SensorRecord r = SensorRecord.now(++seq, "SIM", 15 + Math.random() * 15);
                        file.append(r);
                        if (i == 0) t0 = System.currentTimeMillis();
                        pubsub.publish(TOPIC, r.toLine());
                        if (i == n - 1) t1 = System.currentTimeMillis();
                    }
                    System.out.println("Rajada de " + n + " | t0=" + t0 + " | t1=" + t1 + " | duração=" + (t1 - t0) + " ms");
                    continue;
                }

                // ===== REGISTO NORMAL =====
                if (parts.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(parts[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + parts[1]);
                    continue;
                }
                SensorRecord r = SensorRecord.now(++seq, parts[0], temp);
                file.append(r);
                pubsub.publish(TOPIC, r.toLine());
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("Erro: " + e.getMessage());
        }
    }
}
