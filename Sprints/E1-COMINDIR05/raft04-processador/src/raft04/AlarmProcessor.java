package raft04;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * AlarmProcessor = o PROCESSADOR DE ALARMES.
 *
 * É ao mesmo tempo CONSUMIDOR (ouve os lotes de todas as partições) e PRODUTOR
 * (quando uma temperatura é >= 28 °C, gera uma nova leitura "ALARM-<sensor>" e envia-a
 * a um líder). Isto cria dependências causais ENTRE partições, que é o que o relógio
 * vetorial permite observar.
 *
 * Mantém o seu próprio L e V: vai fundindo (merge) os carimbos de tudo o que lê,
 * e envia esses carimbos no alarme -> o líder de destino sabe que o alarme "veio depois" das leituras.
 */
public class AlarmProcessor {

    private final int n;                       // nº de partições
    private final Object lock = new Object();  // protege lamport e vector
    private long lamport = 0;                  // Lamport do processador
    private final VectorClock vector;          // vetor do processador
    private final Partitioner part;
    private final IpfsPubSub pubsub;

    // Para cada partição, os pares (seq, termo) já tratados. Um registo com o mesmo seq mas
    // outro termo é OUTRO registo (de outro líder) e é tratado de novo.
    private final Set<String>[] seenPares;

    // Para cada partição, o maior termo já observado: lotes de termo inferior são de líderes
    // obsoletos e os seus registos não são tratados.
    private final long[] maiorTermo;

    // Monitores de heartbeat por partição
    private final HeartbeatMonitor[] monitores;

    @SuppressWarnings("unchecked")
    public AlarmProcessor(int n, String api) {
        this.n = n;
        this.vector = new VectorClock(n);
        this.part = new Partitioner(n);
        this.pubsub = new IpfsPubSub(api);

        this.seenPares = new Set[n];
        this.maiorTermo = new long[n];
        this.monitores = new HeartbeatMonitor[n];
        for (int i = 0; i < n; i++) {
            this.seenPares[i] = ConcurrentHashMap.newKeySet();   // conjunto seguro para várias threads
            // um monitor por partição; os eventos vão para registarMonitor
            this.monitores[i] = new HeartbeatMonitor(i, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, this::registarMonitor);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Uso: AlarmProcessor <N> [api]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        AlarmProcessor processor = new AlarmProcessor(n, api);
        processor.run();
    }

    // Arranca o verificador de heartbeats e uma thread de subscrição por partição.
    private void run() {
        // Agendador para verificar os timeouts
        ScheduledExecutorService verificador = Executors.newSingleThreadScheduledExecutor();
        verificador.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitores) {
                m.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        // uma thread por partição, cada uma subscrita ao tópico de dados dessa partição
        List<Thread> threads = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            final int kk = k;
            Thread t = new Thread(() -> {
                try {
                    pubsub.subscribe(Wire.topicData(kk), msg -> receber(kk, msg));
                } catch (IOException | InterruptedException e) {
                    System.out.println("Erro na subscrição p" + kk + ": " + e.getMessage());
                }
            }, "proc-p" + k);
            t.start();
            threads.add(t);
        }

        // a thread main espera pelas outras (ficam a correr para sempre)
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // Recebe e descodifica o LOTE
    // Chamado por cada lote da partição k.
    private void receber(int k, String msg) {
        Batch batch;
        try {
            batch = Batch.decode(msg, n);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO no processador: " + e.getMessage());
            return;
        }

        // 1. Atualiza o monitor de heartbeat (ele próprio ignora termos inferiores)
        monitores[k].onHeartbeat(batch.term, batch.leader, batch.hb);

        // Só trata lotes de termo maior ou igual ao maior termo já observado nesta partição.
        // (o método é chamado por uma thread por partição, por isso maiorTermo[k] só é tocado por uma thread)
        if (batch.term < maiorTermo[k]) {
            System.out.println("IGNORADO no processador: lote do termo " + batch.term
                    + " < " + maiorTermo[k] + " (p" + k + ")");
            return;
        }
        maiorTermo[k] = batch.term;

        // 2. Processa os registos contidos no lote
        for (SensorRecord r : batch.records) {
            if (r.getSensorId().startsWith("ALARM-")) {
                continue; // Ignora registos que já são alarmes (evita alarmes de alarmes, em ciclo)
            }

            // Ignora pares (seq, termo) já tratados
            if (!seenPares[k].add(r.getSeq() + ":" + r.getTermo())) {
                continue;
            }

            String alarmMsg = null;
            int targetPartition = -1;

            synchronized (lock) {
                // fica a "saber" tudo o que este registo sabia: funde os carimbos
                lamport = Math.max(lamport, r.lamport);
                vector.merge(r.vector);

                // se a temperatura passa o limiar, prepara um alarme
                if (r.getTemperature() >= Wire.LIMIAR_ALARME) {
                    String alarmSensor = "ALARM-" + r.getSensorId();
                    targetPartition = part.of(alarmSensor);   // o alarme pode cair noutra partição

                    // o alarme leva os carimbos acumulados (cópia do vetor)
                    alarmMsg = Wire.in(alarmSensor, r.getTemperature(), lamport, vector.copy());

                    System.out.println("ALARME: " + r.getSensorId() + " p" + k + "#" + r.getSeq() + " → p" + targetPartition);
                }
            }

            // Publica fora da secção crítica
            if (alarmMsg != null) {
                // Aviso se a partição de destino estiver sem líder conhecido (publica na mesma)
                if (monitores[targetPartition].state() == HeartbeatMonitor.State.SUSPEITO) {
                    registarMonitor("AVISO: partição p" + targetPartition + " sem líder conhecido; alarme "
                            + r.getSensorId() + " pode perder-se");
                }
                try {
                    pubsub.publish(Wire.topicIn(targetPartition), alarmMsg);
                } catch (IOException | InterruptedException e) {
                    System.out.println("Erro ao publicar alarme: " + e.getMessage());
                }
            }
        }
    }

    // Método para escrever no ficheiro monitor-processador.txt
    // Formato: <instante>;<EVENTO>;p<k>;<detalhe>
    private synchronized void registarMonitor(String evento) {
        String linha = System.currentTimeMillis() + ";" + evento;
        System.out.println(linha);
        try {
            Files.writeString(Paths.get("monitor-processador.txt"), linha + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("Erro a escrever no monitor do processador: " + e.getMessage());
        }
    }
}
