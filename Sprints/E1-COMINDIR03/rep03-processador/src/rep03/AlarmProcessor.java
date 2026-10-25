package rep03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AlarmProcessor {

    private final int n;
    private final Object lock = new Object();
    private long lamport = 0;
    private final VectorClock vector;
    private final Partitioner part;
    private final IpfsPubSub pubsub;

    // CORRIGIDO: Apenas a declaração, sem inicialização com 'n'
    private final Set<Long>[] seenSeqs;

    @SuppressWarnings("unchecked")
    public AlarmProcessor(int n, String api) {
        this.n = n;
        this.vector = new VectorClock(n);
        this.part = new Partitioner(n);
        this.pubsub = new IpfsPubSub(api);

        // CORRIGIDO: Inicialização do array dentro do construtor, onde o 'n' já é conhecido
        this.seenSeqs = new Set[n];
        for (int i = 0; i < n; i++) {
            this.seenSeqs[i] = ConcurrentHashMap.newKeySet();
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

    private void receber(int k, String msg) {
        if (!Wire.type(msg).equals("REG")) {
            return;
        }

        SensorRecord r;
        try {
            String[] partes = Wire.regParts(msg);
            r = SensorRecord.fromLine(partes[2], n);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO no processador: " + msg);
            return;
        }

        // Ignora registos que já são alarmes
        if (r.getSensorId().startsWith("ALARM-")) {
            return;
        }

        // Ignora duplicados (mesma partição e mesmo seq)
        if (!seenSeqs[k].add(r.getSeq())) {
            return;
        }

        String alarmMsg = null;
        int targetPartition = -1;

        synchronized (lock) {
            // Atualiza com o máximo de todos os registos recebidos
            lamport = Math.max(lamport, r.lamport);
            vector.merge(r.vector);

            // Verifica se dispara alarme
            if (r.getTemperature() >= Wire.LIMIAR_ALARME) {
                String alarmSensor = "ALARM-" + r.getSensorId();
                targetPartition = part.of(alarmSensor);

                // Constrói a mensagem com os carimbos atuais (V usa copy() para não sofrer mutações futuras)
                alarmMsg = Wire.in(alarmSensor, r.getTemperature(), lamport, vector.copy());

                System.out.println("ALARME: " + r.getSensorId() + " p" + k + "#" + r.getSeq() + " → p" + targetPartition);
            }
        }

        // Publica fora da secção crítica para não bloquear as outras threads
        if (alarmMsg != null) {
            try {
                pubsub.publish(Wire.topicIn(targetPartition), alarmMsg);
            } catch (IOException | InterruptedException e) {
                System.out.println("Erro ao publicar alarme: " + e.getMessage());
            }
        }
    }
}