package rep01;

import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";

    private static final Object LOCK = new Object();

    private static RecordFile file;
    private static final TreeMap<Long, SensorRecord> pending = new TreeMap<>();

    private static final ScheduledExecutorService SCHED =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fim-rajada");
                t.setDaemon(true);
                return t;
            });
    private static ScheduledFuture<?> tarefaFim = null;

    // ===== DOIS tokens distintos =====
    // versaoTarefa: muda em CADA rearm → invalida tarefas obsoletas dentro da mesma rajada
    // geracao:     muda APENAS quando começa uma nova rajada → distingue períodos para reset de métricas
    private static long versaoTarefa = 0;
    private static long geracao = 0;

    // ===== Estado da medição (protegido por LOCK) =====
    private static boolean rajadaAtiva = false;
    private static long rajadaT0 = 0;
    private static long rajadaT1 = 0;
    private static int rajadaRecebidos = 0;
    private static int rajadaEscritos = 0;
    // ======================================================================

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String port = args.length > 1 ? args[1] : "5001";
        String api = "http://127.0.0.1:" + port + "/api/v0";

        IpfsPubSub pubsub = new IpfsPubSub(api);
        file = new RecordFile("replica-" + id + ".txt");

        try {
            long last = file.lastSeq();
            System.out.println("Réplica PubSub " + id + " subscrita ao tópico " + TOPIC
                    + " (API: " + api + ", último seq = " + last + ")");

            final long[] lastRef = {last};

            pubsub.subscribe(TOPIC, line -> {
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(line);
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                    return;
                }

                synchronized (LOCK) {
                    long seq = r.getSeq();
                    long lastNow = lastRef[0];

                    // ===== Medição da rajada =====
                    boolean isRajada = "SIM".equals(r.getSensorId());
                    if (isRajada) {
                        if (!rajadaAtiva) {
                            // Nova janela de rajada: só aqui se incrementa a geração
                            // e se reiniciam as métricas
                            rajadaAtiva = true;
                            geracao++;
                            rajadaT0 = System.currentTimeMillis();
                            rajadaRecebidos = 0;
                            rajadaEscritos = 0;
                        }
                        rajadaT1 = System.currentTimeMillis();
                        rajadaRecebidos++;
                        rearmarFimRajada();
                    }

                    // ===== Lógica de sequência =====
                    if (seq <= lastNow || pending.containsKey(seq)) {
                        System.out.println("DUPLICADO: " + seq);
                    } else if (seq == lastNow + 1) {
                        try {
                            file.append(r);
                            lastRef[0] = seq;
                            if (isRajada) rajadaEscritos++;
                            System.out.println("Aplicado: " + r.toLine());

                            while (pending.containsKey(lastRef[0] + 1)) {
                                SensorRecord next = pending.remove(lastRef[0] + 1);
                                file.append(next);
                                lastRef[0] = next.getSeq();
                                if ("SIM".equals(next.getSensorId())) rajadaEscritos++;
                                System.out.println("Aplicado (retido): " + next.toLine());
                            }
                        } catch (IOException e) {
                            System.out.println("IO no append: " + e.getMessage());
                        }
                    } else {
                        pending.put(seq, r);
                        System.out.println("EM ESPERA: " + seq + " (falta o registo " + (lastNow + 1) + ")");
                    }
                }
            });
        } catch (IOException | InterruptedException e) {
            System.out.println("Erro: " + e.getMessage());
        }
    }

    // A cada rearm:
    //  - cancela a tarefa anterior (se existir)
    //  - incrementa versaoTarefa  → invalida qualquer tarefa que ainda esteja a executar
    //  - agenda nova tarefa com versaoAlvo = versaoTarefa atual
    private static void rearmarFimRajada() {
        if (tarefaFim != null) {
            tarefaFim.cancel(false);
        }
        versaoTarefa++;                       // muda em CADA rearm
        final long versaoAlvo = versaoTarefa;
        final long geracaoAlvo = geracao;     // capturada apenas para diagnóstico
        tarefaFim = SCHED.schedule(
                () -> imprimirResumoRajada(versaoAlvo, geracaoAlvo),
                1, TimeUnit.SECONDS);
    }

    // Executada na thread "fim-rajada".
    // Valida que ainda é a ÚLTIMA tarefa agendada (versaoAlvo == versaoTarefa);
    // se houver outra rearm entretanto, esta é obsoleta e sai sem tocar no estado.
    private static void imprimirResumoRajada(long versaoAlvo, long geracaoAlvo) {
        synchronized (LOCK) {
            // Token de versão: qualquer rearm entretanto invalida esta tarefa
            if (versaoAlvo != versaoTarefa) {
                return;
            }
            if (!rajadaAtiva) {
                return;
            }
            System.out.println("=== FIM DA RAJADA (heurística de silêncio) ==="
                    + " [geração=" + geracaoAlvo + "]");
            System.out.println("t0=" + rajadaT0 + " | t1=" + rajadaT1
                    + " | janela de receção=" + (rajadaT1 - rajadaT0) + " ms");
            System.out.println("Recebidos=" + rajadaRecebidos
                    + " | Escritos=" + rajadaEscritos
                    + " | Retidos em pending=" + pending.size());
            rajadaAtiva = false;
            rajadaT0 = rajadaT1 = 0;
            rajadaRecebidos = rajadaEscritos = 0;
            tarefaFim = null;
        }
    }
}