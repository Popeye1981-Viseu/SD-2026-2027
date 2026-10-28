package rep04;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Producer {

    // NOVO: Monitores de heartbeat
    private static HeartbeatMonitor[] monitores;

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        Partitioner part = new Partitioner(n);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        IpfsPubSub ps = new IpfsPubSub(api);
        Random rnd = new Random();
        Scanner sc = new Scanner(System.in);

        // NOVO: Inicializar os monitores
        monitores = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            final int kk = k;
            monitores[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                    msg -> registarMonitor(kk, msg));

            // NOVO: Subscrever os LOTES apenas para atualizar o sinal de vida
            final int partition = k;
            Thread t = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(partition), msg -> {
                        try {
                            Batch batch = Batch.decode(msg, n);
                            monitores[partition].onHeartbeat(batch.inc, batch.hb);

                            // ADICIONAR ESTA LINHA:
                            registarMonitor(partition, "RECEBIDO;p" + partition + ";inc=" +
                                    batch.inc + ",hb=" + batch.hb + ",ultimoSeq=" + batch.lastSeq +
                                    ",n=" + batch.records.size() + ",seqs=vazio");
                        } catch (IllegalArgumentException e) {
                            System.out.println("ERRO: LOTE malformado no produtor para p" + partition + ": " + e.getMessage());
                        }
                    });
                    // Aviso se a subscrição retornar normalmente (stream fechado)
                    System.out.println("AVISO: A subscrição de heartbeats para p" + partition + " terminou inesperadamente (stream fechado).");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.println("ERRO: Subscrição de p" + partition + " interrompida: " + e.getMessage());
                } catch (UncheckedIOException e) {
                    System.out.println("ERRO: Falha de rede/IO na subscrição de p" + partition + ": " + e.getMessage());
                } catch (IOException e) {
                    System.out.println("ERRO: Falha de IO ao subscrever p" + partition + ": " + e.getMessage());
                }
            });
            t.start();
        }

        // NOVO: Agendador para verificar timeouts
        ScheduledExecutorService verificador = Executors.newSingleThreadScheduledExecutor();
        verificador.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitores) {
                m.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        System.out.println("N=" + part.count() + " | Comandos: <sensor> <temperatura> | "
                + "rajada <n> <s> | rajada-quente <n> <s> | sair");

        while (sc.hasNextLine()) {
            String[] p = sc.nextLine().trim().split("\\s+");
            if (p.length == 0 || p[0].isEmpty()) continue;

            try {
                if (p[0].equals("sair")) {
                    System.out.println("A sair do produtor...");
                    System.exit(0);

                } else if (p[0].equals("rajada") && p.length == 3) {
                    rajada(ps, part, rnd, Integer.parseInt(p[1]), Integer.parseInt(p[2]), false);

                } else if (p[0].equals("rajada-quente") && p.length == 3) {
                    int s = Integer.parseInt(p[2]);
                    if (s < 2) {
                        System.out.println("rajada-quente precisa de s >= 2");
                    } else {
                        rajada(ps, part, rnd, Integer.parseInt(p[1]), s, true);
                    }

                } else if (p.length == 2) {
                    publicar(ps, part, p[0], Double.parseDouble(p[1]));

                } else {
                    System.out.println("Comando inválido");
                }
            } catch (NumberFormatException e) {
                System.out.println("Número inválido: " + e.getMessage());
            } catch (Exception e) {
                System.out.println("Erro ao publicar: " + e.getMessage());
            }
        }
    }

    private static int publicar(IpfsPubSub ps, Partitioner part, String sensor, double temp)
            throws Exception {
        int k = part.of(sensor);

        // NOVO: Aviso se o líder estiver suspeito
        if (monitores[k].state() == HeartbeatMonitor.State.SUSPEITO) {
            registarMonitor(k, "AVISO: líder p" + k + " suspeito; leitura " + sensor + " pode perder-se");
        }

        VectorClock vetorZero = new VectorClock(part.count());
        ps.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0L, vetorZero));

        System.out.println(sensor + " " + temp + " → p" + k);
        return k;
    }

    private static void rajada(IpfsPubSub ps, Partitioner part, Random rnd,
                               int n, int s, boolean quente) throws Exception {
        int[] porParticao = new int[part.count()];
        long primeira = 0, ultima = 0;

        for (int i = 0; i < n; i++) {
            String sensor;
            if (!quente) {
                sensor = String.format("S%02d", 1 + rnd.nextInt(s));
            } else if (rnd.nextDouble() < 0.8) {
                sensor = "S01";
            } else {
                sensor = String.format("S%02d", 2 + rnd.nextInt(s - 1));
            }
            double temp = 15 + rnd.nextDouble() * 15;

            int k = publicar(ps, part, sensor, temp);
            porParticao[k]++;

            long agora = System.currentTimeMillis();
            if (i == 0) primeira = agora;
            ultima = agora;

            Thread.sleep(5); // Pausa de 5ms original
        }

        System.out.println("Rajada: " + n + " leituras | primeira=" + primeira
                + " ultima=" + ultima + " (" + (ultima - primeira) + " ms)");
        for (int k = 0; k < porParticao.length; k++) {
            System.out.println("  p" + k + ": " + porParticao[k] + " leituras");
        }
    }

    // NOVO: Método para escrever no ficheiro monitor-produtor.txt
    private static synchronized void registarMonitor(int k, String evento) {
        String linha = System.currentTimeMillis() + ";" + evento;
        System.out.println(linha);
        try {
            Files.writeString(Paths.get("monitor-produtor.txt"), linha + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("Erro a escrever no monitor do produtor: " + e.getMessage());
        }
    }
}
