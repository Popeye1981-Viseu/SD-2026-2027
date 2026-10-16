package rep02;

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

// Réplica: subscreve o tópico de dados de cada partição que guarda, aplica os registos
// pela ordem do seq e, quando acumula mais de LIMITE_RETIDOS retidos, pede ao líder
// os registos em falta por TCP.
// Execução: PartitionReplica <id> <partições> [portoApi]   ex.: PartitionReplica 1 0,1
public class PartitionReplica {

    // simulação de perdas, aplicada APENAS às mensagens recebidas pelo PubSub
    private static double perda;          // -Dperda=<p>: descarta cada REG com probabilidade p
    private static long descartar;        // -Ddescartar=<seq>: descarta só a 1.ª receção desse seq
    // partilhado pelas threads das várias partições: só a primeira receção é descartada
    private static final AtomicBoolean jaDescartou = new AtomicBoolean(false);
    // java.util.Random pode ser usado por várias threads em simultâneo
    private static final Random RND = new Random();

    // MEDIÇÃO (como na COMINDIR01): 1 s sem atividade numa partição = fim da rajada
    private static final long SILENCIO_MS = 1000;
    // thread "fim-rajada": verifica periodicamente o silêncio de cada partição.
    // É daemon para não impedir o processo de terminar.
    private static final ScheduledExecutorService SCHED =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "fim-rajada");
                t.setDaemon(true);
                return t;
            });

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: PartitionReplica <id> <partições> [portoApi]");
            return;
        }
        String id = args[0];
        String[] particoes = args[1].split(",");          // lista separada por vírgulas, ex.: "0,1"
        // porto da API do daemon IPFS (5001 por omissão; outro porto no cenário de dois nós)
        String portoApi = args.length > 2 ? args[2] : "5001";
        String api = "http://127.0.0.1:" + portoApi + "/api/v0";

        // propriedades de sistema (por omissão: nenhuma perda simulada)
        perda = Double.parseDouble(System.getProperty("perda", "0"));
        descartar = Long.parseLong(System.getProperty("descartar", "-1"));

        IpfsPubSub ps = new IpfsPubSub(api);

        // por cada partição: um estado próprio, um ficheiro próprio e uma thread de subscrição
        for (String s : particoes) {
            int k = Integer.parseInt(s.trim());
            Particao p = new Particao(k, new RecordFile("replica-" + id + "-p" + k + ".txt"));
            // mensagem de arranque no formato da COMINDIR01
            System.out.println("Réplica PubSub " + id + " subscrita ao tópico " + Wire.topicData(k)
                    + " (API: " + api + ", último seq = " + p.ultimo + ")");

            // verificação do fim da rajada desta partição, de 200 em 200 ms
            SCHED.scheduleAtFixedRate(p::verificarFimRajada, 200, 200, TimeUnit.MILLISECONDS);

            Thread t = new Thread(() -> {
                try {
                    // bloqueia aqui: cada mensagem de rep02-p<k> é tratada por p.receber
                    ps.subscribe(Wire.topicData(k), p::receber);
                } catch (IOException | InterruptedException e) {
                    System.out.println("Erro: " + e.getMessage());
                }
            });
            t.start();
        }
        // o main termina aqui; o processo continua vivo enquanto houver threads de subscrição
    }

    // Estado de UMA partição nesta réplica.
    // É acedido por duas threads: a de subscrição desta partição (receber, incluindo a
    // recuperação) e a thread "fim-rajada" (verificarFimRajada). Por isso os dois métodos
    // de entrada são synchronized neste objeto. Cada partição tem o seu objeto, logo o seu
    // trinco: a recuperação de uma partição não bloqueia as outras.
    private static class Particao {
        final int k;
        final RecordFile file;                                       // replica-<id>-p<k>.txt
        long ultimo;                                                 // último seq escrito
        final TreeMap<Long, SensorRecord> retidos = new TreeMap<>(); // registos retidos, por seq
        String hostLider;                                            // endereço anunciado no último REG
        int portoLider;                                              // porto anunciado no último REG

        // ===== estado da medição da rajada (pontos 6 e 16) =====
        boolean rajadaAtiva = false;
        long t0;                  // instante (ms) do primeiro REG recebido na rajada
        long tUltimaEscrita;      // instante (ms) em que a réplica escreveu o último registo
        long ultimaAtividade;     // última receção ou escrita (para detetar o silêncio)
        int recebidos;            // REG bem formados recebidos pelo PubSub
        int descartados;          // REG descartados pela simulação (-Dperda / -Ddescartar)
        int escritos;             // registos escritos no ficheiro (por PubSub ou por TCP)
        int ligacoes;             // ligações TCP de recuperação abertas
        int ligacoesFalhadas;     // ligações de recuperação que falharam
        int recuperados;          // registos obtidos por TCP
        // =======================================================

        Particao(int k, RecordFile file) throws IOException {
            this.k = k;
            this.file = file;
            this.ultimo = file.lastSeq();   // último seq inicializado com lastSeq()
        }

        // Chamado por cada mensagem recebida pelo PubSub no tópico desta partição
        synchronized void receber(String msg) {
            SensorRecord r;
            String host;
            int porto;
            try {
                // REG;<endereço>;<porto>;<seq>;<sensor>;<temperatura>;<instante>
                if (!Wire.type(msg).equals("REG")) {
                    throw new IllegalArgumentException("tipo não é REG");
                }
                String[] partes = Wire.regParts(msg);          // { endereço, porto, linha do registo }
                host = partes[0];
                porto = Integer.parseInt(partes[1]);
                r = SensorRecord.fromLine(partes[2]);
            } catch (IllegalArgumentException e) {
                // mensagem mal formada: rejeitada, a réplica continua (formato da COMINDIR01)
                System.out.println("REJEITADO: " + msg);
                return;
            }

            marcarAtividade();     // medição: início da rajada e última atividade
            recebidos++;

            // SIMULAÇÃO: descarta antes de aplicar, como se a mensagem não tivesse chegado
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

            // endereço e porto do líder atualizados com CADA mensagem REG recebida
            hostLider = host;
            portoLider = porto;

            aplicar(r);

            // mais de LIMITE_RETIDOS retidos: recupera. Como é verificado a cada REG recebido,
            // uma recuperação falhada é tentada de novo na receção do registo seguinte.
            if (retidos.size() > Wire.LIMITE_RETIDOS) {
                recuperar();
            }
        }

        // Regra de aplicação (a mesma para registos do PubSub e registos recuperados)
        void aplicar(SensorRecord r) {
            long seq = r.getSeq();
            if (seq <= ultimo || retidos.containsKey(seq)) {
                // seq <= último, ou já retido: duplicado, ignora-o
                System.out.println("DUPLICADO: " + seq);
            } else if (seq == ultimo + 1) {
                // seq = último + 1: escreve-o e escreve os retidos que passaram a estar em sequência
                if (escrever(r, false)) {
                    escreverRetidosEmSequencia();
                }
            } else {
                // seq > último + 1: retém-no em memória (formato da COMINDIR01)
                retidos.put(seq, r);
                System.out.println("EM ESPERA: " + seq + " (falta o registo " + (ultimo + 1) + ")");
            }
        }

        // escreve um registo no ficheiro e avança 'ultimo'; devolve false se a escrita falhar.
        // 'retido' só serve para a mensagem: "Aplicado:" ou "Aplicado (retido):", como na COMINDIR01
        boolean escrever(SensorRecord r, boolean retido) {
            try {
                file.append(r);
                ultimo = r.getSeq();
                // medição: instante da última escrita
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

        // escreve, por ordem, os retidos cujo seq passou a ser último + 1
        void escreverRetidosEmSequencia() {
            while (!retidos.isEmpty() && retidos.firstKey() == ultimo + 1) {
                SensorRecord prox = retidos.pollFirstEntry().getValue();  // retira o menor retido
                if (!escrever(prox, true)) {
                    retidos.put(prox.getSeq(), prox);   // escrita falhou: continua retido
                    return;
                }
            }
        }

        // Pede ao líder, por TCP, o intervalo de último + 1 até ao menor seq retido menos 1.
        // Corre na própria thread de subscrição: enquanto dura, esta partição não lê o PubSub.
        void recuperar() {
            long de = ultimo + 1;
            long ate = retidos.firstKey() - 1;
            ligacoes++;   // medição: ligações de recuperação
            System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " pedida a " + hostLider + ":" + portoLider);

            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(hostLider, portoLider), Wire.T_TCP_MS);   // bloqueia até a ligação ser aceite
                s.setSoTimeout(Wire.T_TCP_MS);                                           // readLine deixa de bloquear indefinidamente

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter out = new PrintWriter(
                        new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);

                out.println("PEDIDO " + de + " " + ate);      // envia o pedido

                boolean fim = false;
                String linha;
                while ((linha = in.readLine()) != null) {      // lê os registos até FIM
                    if (linha.equals("FIM")) {
                        fim = true;
                        break;
                    }
                    if (linha.startsWith("ERRO")) {
                        throw new IOException("líder respondeu " + linha);
                    }
                    SensorRecord r = SensorRecord.fromLine(linha);   // seq;sensor;temperatura;instante
                    recuperados++;   // medição: registos obtidos por TCP
                    System.out.println("RECUPERADO: " + r.getSeq());
                    aplicar(r);   // mesma regra; os retidos em sequência são escritos de seguida
                }
                if (!fim) {
                    // ligação fechada antes de FIM: resposta interrompida
                    throw new IOException("resposta interrompida antes de FIM");
                }
            } catch (IOException | IllegalArgumentException e) {
                // ligação recusada, tempo esgotado ou resposta interrompida:
                // os registos já escritos mantêm-se e os retidos continuam em memória
                ligacoesFalhadas++;
                System.out.println("RECUPERAÇÃO FALHOU: " + e.getMessage());
            }
        }

        // MEDIÇÃO: na primeira atividade de uma rajada, reinicia os contadores
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

        // MEDIÇÃO: corre na thread "fim-rajada"; ao fim de 1 s sem atividade escreve o resumo
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