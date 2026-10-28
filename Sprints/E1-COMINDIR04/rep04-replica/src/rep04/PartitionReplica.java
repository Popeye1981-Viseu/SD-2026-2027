package rep04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
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
        // ALTERADO: Agora exige pelo menos 3 argumentos: id, N e as partições
        if (args.length < 3) {
            System.out.println("Uso: PartitionReplica <id> <N> <partições> [portoApi]");
            return;
        }
        String id = args[0];
        int n = Integer.parseInt(args[1]);                // LÊ O N DOS ARGUMENTOS
        String[] particoes = args[2].split(",");          // ex.: "0,1"
        String portoApi = args.length > 3 ? args[3] : "5001";
        String api = "http://127.0.0.1:" + portoApi + "/api/v0";

        perda = Double.parseDouble(System.getProperty("perda", "0"));
        descartar = Long.parseLong(System.getProperty("descartar", "-1"));

        IpfsPubSub ps = new IpfsPubSub(api);

        for (String s : particoes) {
            int k = Integer.parseInt(s.trim());
            // Passa o N (lido dos args) para o RecordFile e para a Particao
            Particao p = new Particao(k, new RecordFile("replica-" + id + "-p" + k + ".txt", n), n);

            System.out.println("Réplica PubSub " + id + " subscrita ao tópico " + Wire.topicData(k)
                    + " (API: " + api + ", último seq = " + p.ultimo + ")");

            SCHED.scheduleAtFixedRate(p::verificarFimRajada, 200, 200, TimeUnit.MILLISECONDS);

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
        final int k;
        final RecordFile file;
        final int n;                    // Número de partições
        long ultimo;
        final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        String hostLider;
        int portoLider;

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

        Particao(int k, RecordFile file, int n) throws IOException {
            this.k = k;
            this.file = file;
            this.n = n;
            this.ultimo = file.lastSeq();
        }

        synchronized void receber(String msg) {
            SensorRecord r;
            String host;
            int porto;
            try {
                if (!Wire.type(msg).equals("REG")) {
                    throw new IllegalArgumentException("tipo não é REG");
                }
                String[] partes = Wire.regParts(msg);
                host = partes[0];
                porto = Integer.parseInt(partes[1]);
                r = SensorRecord.fromLine(partes[2], n);
            } catch (IllegalArgumentException e) {
                System.out.println("REJEITADO: " + msg);
                return;
            }

            marcarAtividade();
            recebidos++;

            if (r.getSeq() == descartar && jaDescartou.compareAndSet(false, true)) {
                descartados++;
                System.out.println("DESCARTADO (simulação): " + r.getSeq());
                return;
            }
            if (perda > 0 && RND.nextDouble() < perda) {
                descartados++;
                System.out.println("DESCARTADO (simulação): " + r.getSeq());
                return;
            }

            hostLider = host;
            portoLider = porto;

            aplicar(r);

            if (retidos.size() > Wire.LIMITE_RETIDOS) {
                recuperar();
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

        void recuperar() {
            long de = ultimo + 1;
            long ate = retidos.firstKey() - 1;
            ligacoes++;
            System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " pedida a " + hostLider + ":" + portoLider);

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
                    SensorRecord r = SensorRecord.fromLine(linha, n);
                    recuperados++;
                    System.out.println("RECUPERADO: " + r.getSeq());
                    aplicar(r);
                }
                if (!fim) {
                    throw new IOException("resposta interrompida antes de FIM");
                }
            } catch (IOException | IllegalArgumentException e) {
                ligacoesFalhadas++;
                System.out.println("RECUPERAÇÃO FALHOU: " + e.getMessage());
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
    }
}