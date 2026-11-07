package raft04;

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

/**
 * Producer = o PRODUTOR (a "interface" por onde se injetam leituras à mão).
 *
 * Lê comandos do teclado:
 *   <sensor> <temperatura>   ex.:  S01 21.0        (1 leitura)
 *   rajada <n> <s>           ex.:  rajada 300 10   (n leituras de s sensores distintos S01..Ss)
 *   rajada-quente <n> <s>    80 % das leituras para S01 (cria uma partição "quente")
 *   sair
 * Cada leitura é publicada em raft04-in-p<k>, onde k = partição do sensor (Partitioner).
 *
 * Além disso, também ouve os lotes de todas as partições, só para vigiar os heartbeats
 * e AVISAR quando uma leitura vai para uma partição sem líder conhecido.
 *
 * NOVO nesta ficha: escreve em produtor.txt UMA LINHA por cada IN enviado
 * (instante;p<k>;sensor;temperatura). O RaftCheck usa-o para saber que leituras se perderam.
 */
public class Producer {

    // Monitores de heartbeat (um por partição)
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
        Scanner sc = new Scanner(System.in);   // leitura de comandos do teclado

        // Inicializar os monitores
        monitores = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            final int kk = k;
            monitores[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                    msg -> registarMonitor(kk, msg));

            // Subscrever os LOTES apenas para atualizar o sinal de vida
            final int partition = k;
            Thread t = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(partition), msg -> {
                        try {
                            Batch batch = Batch.decode(msg, n);
                            // ALTERADO: o detetor usa o termo e o líder (já não há incarnação)
                            monitores[partition].onHeartbeat(batch.term, batch.leader, batch.hb);
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

        // Agendador para verificar timeouts
        ScheduledExecutorService verificador = Executors.newSingleThreadScheduledExecutor();
        verificador.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitores) {
                m.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        System.out.println("N=" + part.count() + " | Comandos: <sensor> <temperatura> | "
                + "rajada <n> <s> | rajada-quente <n> <s> | sair");

        // Ciclo principal: lê uma linha, interpreta o comando e executa.
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
                        // precisa de pelo menos 2 sensores: S01 (quente) e outro
                        System.out.println("rajada-quente precisa de s >= 2");
                    } else {
                        rajada(ps, part, rnd, Integer.parseInt(p[1]), s, true);
                    }

                } else if (p.length == 2) {
                    // "<sensor> <temperatura>": uma leitura avulsa
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

    // Publica UMA leitura: escolhe a partição pelo sensor e envia IN para o tópico dessa partição.
    // Devolve a partição escolhida.
    private static int publicar(IpfsPubSub ps, Partitioner part, String sensor, double temp)
            throws Exception {
        int k = part.of(sensor);

        // Aviso se a partição estiver sem líder conhecido (a leitura é enviada na mesma)
        if (monitores[k].state() == HeartbeatMonitor.State.SUSPEITO) {
            registarMonitor(k, "AVISO: partição p" + k + " sem líder conhecido; leitura " + sensor + " pode perder-se");
        }

        // O produtor não tem histórico causal: envia L=0 e V a zeros.
        VectorClock vetorZero = new VectorClock(part.count());
        long instante = System.currentTimeMillis();   // instante do envio
        ps.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0L, vetorZero));
        registarEnvio(instante, k, sensor, temp);     // só chega aqui se o envio não falhou

        System.out.println(sensor + " " + temp + " → p" + k);
        return k;
    }

    // Envia n leituras seguidas com s sensores distintos.
    // quente=true -> 80 % para S01 (partição "quente").
    private static void rajada(IpfsPubSub ps, Partitioner part, Random rnd,
                               int n, int s, boolean quente) throws Exception {
        int[] porParticao = new int[part.count()];   // quantas leituras foram para cada partição
        long primeira = 0, ultima = 0;               // instantes da primeira e da última publicação
        int falhadas = 0;                            // leituras que não conseguiram ser publicadas

        for (int i = 0; i < n; i++) {
            String sensor;
            if (!quente) {
                // sensor uniforme entre S01..S<s>
                sensor = String.format("S%02d", 1 + rnd.nextInt(s));
            } else if (rnd.nextDouble() < 0.8) {
                sensor = "S01";   // 80 % das vezes o sensor quente
            } else {
                sensor = String.format("S%02d", 2 + rnd.nextInt(s - 1));
            }
            double temp = 15 + rnd.nextDouble() * 15;   // temperatura aleatória entre 15 e 30

            try {
                int k = publicar(ps, part, sensor, temp);
                porParticao[k]++;
            } catch (IOException e) {
                // uma falha pontual (ex.: ligação ao Kubo recusada) não interrompe a rajada
                falhadas++;
                System.out.println("Erro ao publicar: " + e.getMessage());
            }

            long agora = System.currentTimeMillis();
            if (i == 0) primeira = agora;
            ultima = agora;

            Thread.sleep(10); // pausa entre publicações
        }

        System.out.println("Rajada: " + n + " leituras | primeira=" + primeira
                + " ultima=" + ultima + " (" + (ultima - primeira) + " ms) | não publicadas=" + falhadas);
        for (int k = 0; k < porParticao.length; k++) {
            System.out.println("  p" + k + ": " + porParticao[k] + " leituras");
        }
    }

    // NOVO: acrescenta uma linha a produtor.txt por cada IN enviado.
    // Formato: <instante>;p<k>;<sensor>;<temperatura>
    private static synchronized void registarEnvio(long instante, int k, String sensor, double temp) {
        try {
            Files.writeString(Paths.get("produtor.txt"), instante + ";p" + k + ";" + sensor + ";" + temp + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("Erro a escrever produtor.txt: " + e.getMessage());
        }
    }

    // Método para escrever no ficheiro monitor-produtor.txt
    // Formato: <instante>;<EVENTO>;p<k>;<detalhe>   (o parâmetro k não é usado no corpo)
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
