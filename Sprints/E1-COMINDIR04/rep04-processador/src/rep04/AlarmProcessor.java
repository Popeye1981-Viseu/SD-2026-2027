package rep04;

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

public class AlarmProcessor {

    private final int n;
    private final Object lock = new Object();
    private long lamport = 0;
    private final VectorClock vector;
    private final Partitioner part;
    private final IpfsPubSub pubsub;

    private final Set<Long>[] seenSeqs;

    // NOVO: Monitores de heartbeat por partição
    private final HeartbeatMonitor[] monitores;

    @SuppressWarnings("unchecked")
    public AlarmProcessor(int n, String api) {
        this.n = n;
        this.vector = new VectorClock(n);
        this.part = new Partitioner(n);
        this.pubsub = new IpfsPubSub(api);

        this.seenSeqs = new Set[n];
        this.monitores = new HeartbeatMonitor[n];
        for (int i = 0; i < n; i++) {
            this.seenSeqs[i] = ConcurrentHashMap.newKeySet();
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

    private void run() {
        // NOVO: Agendador para verificar os timeouts
        ScheduledExecutorService verificador = Executors.newSingleThreadScheduledExecutor();
        verificador.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitores) {
                m.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

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

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ALTERADO: Agora recebe e descodifica o LOTE
    private void receber(int k, String msg) {
        Batch batch;
        try {
            batch = Batch.decode(msg, n);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO no processador: " + e.getMessage());
            return;
        }

        // 1. Atualiza o monitor de heartbeat
        monitores[k].onHeartbeat(batch.inc, batch.hb);

        // ADICIONAR ESTAS LINHAS:
        String seqs = batch.records.isEmpty() ? "vazio" : batch.records.get(0).seq + ".." +
                batch.records.get(batch.records.size() - 1).seq;
        registarMonitor("RECEBIDO;p" + k + ";inc=" + batch.inc + ",hb=" + batch.hb +
                ",ultimoSeq=" + batch.lastSeq + ",n=" + batch.records.size() + ",seqs=" + seqs);

        // 2. Processa os registos contidos no lote
        for (SensorRecord r : batch.records) {
            if (r.getSensorId().startsWith("ALARM-")) {
                continue; // Ignora registos que já são alarmes
            }

            // Ignora duplicados (mesma partição e mesmo seq)
            if (!seenSeqs[k].add(r.getSeq())) {
                continue;
            }

            String alarmMsg = null;
            int targetPartition = -1;

            synchronized (lock) {
                lamport = Math.max(lamport, r.lamport);
                vector.merge(r.vector);

                if (r.getTemperature() >= Wire.LIMIAR_ALARME) {
                    String alarmSensor = "ALARM-" + r.getSensorId();
                    targetPartition = part.of(alarmSensor);

                    alarmMsg = Wire.in(alarmSensor, r.getTemperature(), lamport, vector.copy());

                    System.out.println("ALARME: " + r.getSensorId() + " p" + k + "#" + r.getSeq() + " → p" + targetPartition);
                }
            }

            // Publica fora da secção crítica
            if (alarmMsg != null) {
                // NOVO: Aviso se o líder da partição de destino estiver suspeito
                if (monitores[targetPartition].state() == HeartbeatMonitor.State.SUSPEITO) {
                    registarMonitor("AVISO: líder p" + targetPartition + " suspeito; alarme " + r.getSensorId() + " pode perder-se");
                }
                try {
                    pubsub.publish(Wire.topicIn(targetPartition), alarmMsg);
                } catch (IOException | InterruptedException e) {
                    System.out.println("Erro ao publicar alarme: " + e.getMessage());
                }
            }
        }
    }

    // NOVO: Método para escrever no ficheiro monitor-processador.txt
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