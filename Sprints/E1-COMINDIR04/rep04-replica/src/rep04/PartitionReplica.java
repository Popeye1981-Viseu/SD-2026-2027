package rep04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartitionReplica {

    private static double perda;
    private static long descartar;
    private static final AtomicBoolean jaDescartou = new AtomicBoolean(false);
    private static final Random RND = new Random();

    private static final long SILENCIO_MS = 1000;
    private static final ScheduledExecutorService SCHED =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fim-rajada");
                t.setDaemon(true);
                return t;
            });

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Uso: PartitionReplica <id> <N> <partições> [portoApi]");
            return;
        }
        String id = args[0];
        int n = Integer.parseInt(args[1]);
        String[] particoes = args[2].split(",");
        String portoApi = args.length > 3 ? args[3] : "5001";
        String api = "http://127.0.0.1:" + portoApi + "/api/v0";

        perda = Double.parseDouble(System.getProperty("perda", "0"));
        descartar = Long.parseLong(System.getProperty("descartar", "-1"));

        IpfsPubSub ps = new IpfsPubSub(api);
        ScheduledExecutorService verificador = Executors.newSingleThreadScheduledExecutor();

        // NOVO: Garantir que os agendadores são encerrados quando a JVM termina
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("A encerrar réplica e a fechar os agendadores...");
            SCHED.shutdownNow();
            verificador.shutdownNow();
        }));

        for (String s : particoes) {
            int k = Integer.parseInt(s.trim());
            Particao p = new Particao(id, k, new RecordFile("replica-" + id + "-p" + k + ".txt", n), n);

            System.out.println("Réplica PubSub " + id + " subscrita ao tópico " + Wire.topicData(k)
                    + " (API: " + api + ", último seq = " + p.ultimo + ")");

            SCHED.scheduleAtFixedRate(p::verificarFimRajada, 200, 200, TimeUnit.MILLISECONDS);
            verificador.scheduleAtFixedRate(p::verificarMonitor, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

            Thread t = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(k), p::receber);
                } catch (IOException | InterruptedException e) {
                    System.out.println("Erro: " + e.getMessage());
                }
            });
            t.start();
        }
    }

    private static class Particao {
        final String id;
        final int k;
        final RecordFile file;
        final int n;
        long ultimo;
        final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        String hostLider;
        int portoLider;

        final HeartbeatMonitor monitor;

        // NOVO: Flag para evitar recuperações concorrentes sem bloquear a receção
        boolean emRecuperacao = false;

        boolean rajadaAtiva = false;
        long t0;
        long tUltimaEscrita;
        long ultimaAtividade;
        int recebidos;
        int descartados;
        int escritos;
        int ligacoes;
        int ligacoesFalhadas;
        int recuperados;

        Particao(String id, int k, RecordFile file, int n) throws IOException {
            this.id = id;
            this.k = k;
            this.file = file;
            this.n = n;
            this.ultimo = file.lastSeq();
            this.monitor = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, this::registarMonitor);
        }

        synchronized void verificarMonitor() {
            monitor.check();
        }

        synchronized void receber(String msg) {
            Batch batch;
            try {
                batch = Batch.decode(msg, n);
            } catch (IllegalArgumentException e) {
                System.out.println("LOTE REJEITADO: " + msg + " | " + e.getMessage());
                return;
            }

            monitor.onHeartbeat(batch.inc, batch.hb);

            String seqs = batch.records.isEmpty() ? "vazio" :
                    batch.records.get(0).seq + ".." + batch.records.get(batch.records.size() - 1).seq;
            registarMonitor("RECEBIDO;p" + k + ";inc=" + batch.inc + ",hb=" + batch.hb +
                    ",ultimoSeq=" + batch.lastSeq + ",n=" + batch.records.size() + ",seqs=" + seqs);

            marcarAtividade();
            hostLider = batch.address;
            portoLider = batch.port;

            for (SensorRecord r : batch.records) {
                recebidos++;
                if (r.getSeq() == descartar && jaDescartou.compareAndSet(false, true)) {
                    descartados++;
                    System.out.println("DESCARTADO (simulação): " + r.getSeq());
                    continue;
                }
                if (perda > 0 && RND.nextDouble() < perda) {
                    descartados++;
                    System.out.println("DESCARTADO (simulação): " + r.getSeq());
                    continue;
                }
                aplicar(r);
            }

            // ATUALIZADO: Verifica lacuna e lança thread sem bloquear o trinco
            if ((ultimo < batch.lastSeq || retidos.size() > Wire.LIMITE_RETIDOS) && !emRecuperacao) {
                long de = ultimo + 1;
                long ate = batch.lastSeq;
                if (!retidos.isEmpty() && retidos.firstKey() > de) {
                    ate = Math.min(ate, retidos.firstKey() - 1);
                }
                if (de <= ate) {
                    emRecuperacao = true;
                    registarMonitor("RECUPERACAO;p" + k + ";de=" + de + ";ate=" + ate);
                    final long fDe = de;
                    final long fAte = ate;
                    new Thread(() -> executarRecuperacao(fDe, fAte)).start();
                }
            }
        }

        void aplicar(SensorRecord r) {
            long seq = r.getSeq();
            if (seq <= ultimo || retidos.containsKey(seq)) {
                System.out.println("DUPLICADO: " + seq);
            } else if (seq == ultimo + 1) {
                if (escrever(r, false)) {
                    escreverRetidosEmSequencia();
                }
            } else {
                retidos.put(seq, r);
                System.out.println("EM ESPERA: " + seq + " (falta o registo " + (ultimo + 1) + ")");
            }
        }

        boolean escrever(SensorRecord r, boolean retido) {
            try {
                file.append(r);
                ultimo = r.getSeq();
                tUltimaEscrita = System.currentTimeMillis();
                ultimaAtividade = tUltimaEscrita;
                escritos++;
                System.out.println((retido ? "Aplicado (retido): " : "Aplicado: ") + r.toLine());
                return true;
            } catch (IOException e) {
                System.out.println("IO no append: " + e.getMessage());
                return false;
            }
        }

        void escreverRetidosEmSequencia() {
            while (!retidos.isEmpty() && retidos.firstKey() == ultimo + 1) {
                SensorRecord prox = retidos.pollFirstEntry().getValue();
                if (!escrever(prox, true)) {
                    retidos.put(prox.getSeq(), prox);
                    return;
                }
            }
        }

        // NOVO: Recuperação assíncrona que não bloqueia a receção de heartbeats
        void executarRecuperacao(long de, long ate) {
            List<SensorRecord> temporarios = new ArrayList<>();
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(hostLider, portoLider), Wire.T_TCP_MS);
                s.setSoTimeout(Wire.T_TCP_MS);

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(
                        new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);

                out.println("PEDIDO " + de + " " + ate);

                boolean fim = false;
                String linha;
                while ((linha = in.readLine()) != null) {
                    if (linha.equals("FIM")) {
                        fim = true;
                        break;
                    }
                    if (linha.startsWith("ERRO")) {
                        throw new IOException("líder respondeu " + linha);
                    }
                    temporarios.add(SensorRecord.fromLine(linha, n));
                }
                if (!fim) {
                    throw new IOException("resposta interrompida antes de FIM");
                }

                // Só aplica se recebeu tudo com sucesso
                synchronized (this) {
                    for (SensorRecord r : temporarios) {
                        retidos.put(r.getSeq(), r);
                        recuperados++;
                    }
                    escreverRetidosEmSequencia();
                }
            } catch (IOException | IllegalArgumentException e) {
                ligacoesFalhadas++;
                System.out.println("RECUPERAÇÃO FALHOU: " + e.getMessage());
            } finally {
                // Liberta a flag para futuras tentativas
                synchronized (this) {
                    emRecuperacao = false;
                }
            }
        }

        void marcarAtividade() {
            long agora = System.currentTimeMillis();
            if (!rajadaAtiva) {
                rajadaAtiva = true;
                t0 = agora;
                tUltimaEscrita = 0;
                recebidos = descartados = escritos = 0;
                ligacoes = ligacoesFalhadas = recuperados = 0;
            }
            ultimaAtividade = agora;
        }

        synchronized void verificarFimRajada() {
            if (!rajadaAtiva || System.currentTimeMillis() - ultimaAtividade < SILENCIO_MS) {
                return;
            }
            System.out.println("=== FIM DA RAJADA " + Wire.topicData(k) + " (heurística de silêncio) ===");
            System.out.println("t0=" + t0 + " | tUltimaEscrita=" + tUltimaEscrita
                    + " | janela=" + (tUltimaEscrita > 0 ? (tUltimaEscrita - t0) : 0) + " ms");
            System.out.println("Recebidos=" + recebidos + " | Descartados=" + descartados
                    + " | Escritos=" + escritos);
            System.out.println("Ligações de recuperação=" + ligacoes + " (falhadas=" + ligacoesFalhadas + ")"
                    + " | Registos obtidos por TCP=" + recuperados);
            System.out.println("Retidos em memória=" + retidos.size() + " | último seq=" + ultimo);
            rajadaAtiva = false;
        }

        synchronized void registarMonitor(String evento) {
            String linha = System.currentTimeMillis() + ";" + evento;
            System.out.println(linha);
            try {
                Files.writeString(Paths.get("monitor-replica" + id + ".txt"), linha + "\n",
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.out.println("Erro a escrever no monitor: " + e.getMessage());
            }
        }
    }
}