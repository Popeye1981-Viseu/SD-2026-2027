package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * PartitionNode = UM NÓ do grupo de UMA partição. Substitui o PartitionLeader e a Replica.
 * Arranque:  PartitionNode <k> <i> [N]     (k = partição, i = nó 1..3, N = nº de partições, por omissão 3)
 *
 * Cada nó guarda uma cópia completa dos registos da partição e está, em cada instante,
 * num de três papéis (quem decide é o RaftState):
 *   SEGUIDOR  - aplica os lotes do líder e espera heartbeats;
 *   CANDIDATO - pediu votos para ser líder;
 *   LIDER     - recebe os IN, atribui seq e publica os lotes (como o PartitionLeader da ficha anterior).
 *
 * Threads em jogo:
 *   main (servidor TCP de recuperação) | 3 subscrições (IN, LOTE, eleição) |
 *   verificação (tick, a cada VERIFICACAO_MS) | agendador do líder (enviarLote) | recuperação (TCP, assíncrona).
 *
 * LOCKS (ordem de aquisição, nunca ao contrário):
 *   monitor do nó (this)  ->  monitor do RaftState  ->  trinco do registo de eventos (logLock).
 * O RaftState nunca chama o nó, e o registo de eventos nunca pede nenhum dos outros dois,
 * por isso não há ordem inversa possível e não há deadlock.
 * TODA a chamada que muda o RaftState é feita DENTRO do monitor do nó. É isto que garante que um IN
 * que chega no instante em que o nó se demite ou recebe o termo novo é tratado ANTES (como líder do
 * termo antigo) ou DEPOIS (termIfLeader() = -1, IGNORADO), nunca "a meio".
 */
public class PartitionNode {

    // ---------------------------------------------------------------- identificação
    private final int k;                  // partição de dados
    private final int i;                  // identificador deste nó dentro do grupo (1..G)
    private final int n;                  // número de partições (tamanho do vetor V)
    private final Partitioner part;
    private final IpfsPubSub ps;
    private final String endereco = "127.0.0.1";   // onde este nó atende recuperações
    private final int porto;                        // porto TCP deste nó

    private RaftState raft;               // termo, voto, papel, prazo de eleição
    private final RecordFile ficheiro;    // no-p<k>-n<i>.txt

    // ---------------------------------------------------------------- estado protegido pelo monitor do nó
    // Cópia em memória do ficheiro: registos.get(s-1) é o registo com seq = s (sem buracos).
    private final List<SensorRecord> registos = new ArrayList<>();

    // estado do LÍDER (só tem significado enquanto o nó é líder)
    private long seq;                     // último seq atribuído
    private long lamport;                 // relógio de Lamport atual
    private VectorClock vector;           // relógio vetorial atual
    private long hb;                      // nº do último heartbeat enviado (recomeça em 1 em cada liderança)
    private List<SensorRecord> pendentes = new ArrayList<>();   // registos gravados mas ainda não publicados
    private ScheduledFuture<?> agendamento;                     // tarefa periódica de enviarLote (null se não é líder)

    // estado do SEGUIDOR
    private long ultimoTermoLote = 0;     // termo do último lote aplicado ...
    private long ultimoHbLote = 0;        // ... e o seu hb (para descartar lotes repetidos ou atrasados)
    private boolean emRecuperacao = false; // há uma recuperação TCP em curso (evita lançar outra)

    // agendadores
    private final ScheduledExecutorService agendador = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService verificador = Executors.newSingleThreadScheduledExecutor();

    // registo de eventos: trinco próprio, o último na ordem de aquisição
    private final Object logLock = new Object();

    public PartitionNode(int k, int i, int n, String api) {
        this.k = k;
        this.i = i;
        this.n = n;
        this.part = new Partitioner(n);
        this.ps = new IpfsPubSub(api);
        this.porto = Wire.nodePort(k, i);
        this.ficheiro = new RecordFile("no-p" + k + "-n" + i + ".txt", n);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionNode <k> <i> [N]");
            return;
        }
        int k = Integer.parseInt(args[0]);
        int i = Integer.parseInt(args[1]);
        int n = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        if (i < 1 || i > Wire.GRUPO) {
            System.out.println("i tem de estar entre 1 e " + Wire.GRUPO);
            return;
        }
        PartitionNode no = new PartitionNode(k, i, n, "http://127.0.0.1:5001/api/v0");
        no.arrancar();
    }

    // ================================================================ ARRANQUE

    private void arrancar() throws IOException {
        // 1. lê o ficheiro, se existir: dá o ultimoSeq e o ultimoTermo locais
        List<SensorRecord> doFicheiro = ficheiro.readAll();
        for (int idx = 0; idx < doFicheiro.size(); idx++) {
            // o ficheiro tem de ter os seq 1, 2, 3, ... sem buracos
            if (doFicheiro.get(idx).seq != idx + 1) {
                throw new IOException("Ficheiro " + "no-p" + k + "-n" + i + ".txt com buraco no seq " + (idx + 1));
            }
        }
        registos.addAll(doFicheiro);

        // 2. cria o RaftState como seguidor, com o termo gravado em estado-p<k>-n<i>.txt
        raft = new RaftState(k, i, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS,
                Paths.get("estado-p" + k + "-n" + i + ".txt"), this::evento);

        // Evento extra (não é da lista da ficha): marca o instante de arranque, que o RaftCheck
        // usa para medir o "tempo até ao primeiro líder desde o arranque do primeiro nó".
        evento("ARRANQUE;p" + k + ";termo=" + raft.term() + ";ultimoSeq=" + ultimoSeq()
                + ";ultimoTermo=" + ultimoTermo());

        // 3. subscreve os três tópicos (cada subscrição bloqueia uma thread)
        subscrever(Wire.topicIn(k), this::tratarEntrada);
        subscrever(Wire.topicData(k), this::receberLote);
        subscrever(Wire.topicElection(k), this::receberEleicao);

        // 4. thread de verificação: a cada VERIFICACAO_MS vê se o prazo de eleição expirou
        verificador.scheduleAtFixedRate(this::verificar, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS,
                TimeUnit.MILLISECONDS);

        // 5. servidor TCP de recuperação (corre na thread main, para sempre). Serve registos
        //    a qualquer nó, seja ou não líder. Cada ligação é tratada numa thread própria.
        System.out.println("Nó n" + i + " da partição p" + k + " | termo=" + raft.term() + " | ultimoSeq="
                + ultimoSeq() + " | ultimoTermo=" + ultimoTermo() + " | recuperação em " + endereco + ":" + porto);
        try (ServerSocket server = new ServerSocket(porto)) {
            while (true) {
                Socket s = server.accept();
                new Thread(() -> atenderLigacao(s)).start();
            }
        }
    }

    // Lança uma thread que subscreve um tópico e entrega cada mensagem ao tratador.
    // O tratador nunca deixa escapar exceções: se escapasse, a subscrição terminava em silêncio.
    private void subscrever(String topico, java.util.function.Consumer<String> tratador) {
        Thread t = new Thread(() -> {
            try {
                ps.subscribe(topico, msg -> {
                    try {
                        tratador.accept(msg);
                    } catch (Exception e) {
                        evento("ERRO;p" + k + ";" + topico + ": " + e);
                    }
                });
                evento("ERRO;p" + k + ";subscrição de " + topico + " terminou (stream fechado)");
            } catch (IOException | InterruptedException e) {
                evento("ERRO;p" + k + ";subscrição de " + topico + " falhou: " + e.getMessage());
            }
        }, "sub-" + topico);
        t.start();
    }

    // ================================================================ AUXILIARES

    // último seq e termo do último registo local (chamar com o monitor do nó)
    private long ultimoSeq()   { return registos.size(); }
    private long ultimoTermo() { return registos.isEmpty() ? 0 : registos.get(registos.size() - 1).termo; }

    // acrescenta um registo ao FICHEIRO e à memória (chamar com o monitor do nó)
    private void acrescentar(SensorRecord r) throws IOException {
        ficheiro.append(r);      // grava primeiro (durabilidade) ...
        registos.add(r);         // ... e só depois atualiza a memória
    }

    // Escreve um evento, precedido do instante, na consola e em eventos-p<k>-n<i>.txt.
    // Formato:  <instante>;<EVENTO>;p<k>;<detalhe>
    private void evento(String texto) {
        String linha = System.currentTimeMillis() + ";" + texto;
        synchronized (logLock) {
            System.out.println(linha);
            try {
                Files.writeString(Paths.get("eventos-p" + k + "-n" + i + ".txt"), linha + "\n",
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.out.println("Erro a escrever eventos: " + e.getMessage());
            }
        }
    }

    // Se o nó já não é líder, pára o agendamento dos lotes e deita fora os pendentes.
    // (chamar com o monitor do nó, depois de qualquer chamada que possa ter demitido o nó)
    private void limparSeNaoLider() {
        if (agendamento != null && raft.termIfLeader() == -1) {
            agendamento.cancel(false);
            agendamento = null;
            pendentes = new ArrayList<>();
        }
    }

    // publica num tópico, a não ser que o nó esteja "isolado" (partição de rede simulada)
    private void publicar(String topico, String msg) {
        if (Wire.isolated(k, i)) return;          // isolado: nada sai
        try {
            ps.publish(topico, msg);
        } catch (IOException | InterruptedException e) {
            evento("ERRO;p" + k + ";falha ao publicar em " + topico + ": " + e.getMessage());
        }
    }

    // ================================================================ ELEIÇÃO

    // Thread de verificação, a cada VERIFICACAO_MS: se o prazo expirou, o RaftState inicia uma
    // candidatura (já gravou o estado) e devolve o termo; então publicamos o PEDIDO_VOTO.
    private void verificar() {
        try {
            long termo;
            long us;
            long ut;
            synchronized (this) {
                termo = raft.tick();
                us = ultimoSeq();
                ut = ultimoTermo();
            }
            if (termo != -1) {
                publicar(Wire.topicElection(k), new VoteRequest(termo, i, us, ut).encode());
            }
        } catch (Exception e) {
            evento("ERRO;p" + k + ";verificação: " + e);
        }
    }

    // Mensagens do tópico de eleição: PEDIDO_VOTO e VOTO.
    private void receberEleicao(String msg) {
        if (Wire.isolated(k, i)) return;          // isolado: não processa
        String tipo = Wire.type(msg);

        if (tipo.equals("PEDIDO_VOTO")) {
            VoteRequest vr = VoteRequest.decode(msg);
            if (vr.candidate == i) return;        // eco do meu próprio pedido
            RaftState.Vote voto;
            synchronized (this) {
                // decide com o MEU último registo (restrição de voto)
                voto = raft.onVoteRequest(vr.term, vr.candidate, vr.lastSeq, vr.lastTerm,
                        ultimoSeq(), ultimoTermo());
                limparSeNaoLider();               // pode ter-me demitido (termo superior)
            }
            // responde SEMPRE, mesmo quando recusa: o candidato fica a saber o termo atual do eleitor
            publicar(Wire.topicElection(k), new VoteReply(voto.term, i, vr.candidate, voto.granted).encode());

        } else if (tipo.equals("VOTO")) {
            VoteReply vr = VoteReply.decode(msg);
            if (vr.voter == i || vr.candidate != i) return;   // só interessam os votos dirigidos a mim
            synchronized (this) {
                boolean ganhei = raft.onVote(vr.term, vr.voter, vr.candidate, vr.granted);
                limparSeNaoLider();
                if (ganhei) tornarLider();
            }
        }
    }

    // ================================================================ LÍDER

    // Passagem a líder (chamar com o monitor do nó). Inicializa seq, L, V, hb e pendentes
    // e inicia o agendador que publica um lote de imediato e depois a cada PERIODO_HB_MS.
    private void tornarLider() {
        if (raft.termIfLeader() == -1) return;    // demitido entretanto: não faz nada

        seq = ultimoSeq();                        // continua a numeração do MEU último registo
        if (registos.isEmpty()) {
            lamport = 0;                          // sem registos: começa do zero
            vector = new VectorClock(n);
        } else {
            // L e V a partir do último registo: sem isto o V[k] deixaria de ser igual ao seq
            SensorRecord ultimo = registos.get(registos.size() - 1);
            lamport = ultimo.lamport;
            vector = ultimo.vector.copy();
        }
        hb = 0;
        pendentes = new ArrayList<>();

        if (agendamento != null) agendamento.cancel(false);   // nunca dois agendamentos ao mesmo tempo
        agendamento = agendador.scheduleAtFixedRate(this::enviarLote, 0, Wire.PERIODO_HB_MS,
                TimeUnit.MILLISECONDS);
        System.out.println("Sou LÍDER de p" + k + " no termo " + raft.term() + " | seq=" + seq
                + " | L=" + lamport + " | V=" + vector);
    }

    // Mensagens IN (leituras). Só o líder as aceita; os outros nós assinalam IGNORADO.
    private void tratarEntrada(String msg) {
        String sensor;
        double temp;
        long lMsg;
        VectorClock vMsg;

        try {
            // 1) validação: tem de ser do tipo IN com 4 campos (sensor;temp;L;V)
            if (!Wire.type(msg).equals("IN")) {
                throw new IllegalArgumentException("tipo não é IN");
            }
            String[] f = Wire.body(msg).split(";");
            if (f.length != 4 || f[0].isBlank()) {
                throw new IllegalArgumentException("esperado IN;<sensor>;<temperatura>;<L>;<V>");
            }
            sensor = f[0].trim();
            temp = Double.parseDouble(f[1].trim());
            lMsg = Long.parseLong(f[2].trim());
            vMsg = VectorClock.parse(f[3].trim(), part.count());

            // 2) o sensor pertence a esta partição?
            int j = part.of(sensor);
            if (j != k) {
                System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                return;
            }

            // 3) secção crítica do nó
            synchronized (this) {
                long t = raft.termIfLeader();
                if (t == -1) {
                    // não sou líder (ou acabei de me demitir): não atribui seq
                    evento("IGNORADO;p" + k + ";IN " + sensor + " " + temp + " (sem liderança)");
                    return;
                }
                long novoL = Math.max(lamport, lMsg) + 1;   // Lamport: max(local, recebido) + 1
                VectorClock novoV = vector.copy();
                novoV.merge(vMsg);                          // vetor: máximo componente a componente
                novoV.tick(k);                              // evento local: incrementa a componente k
                long novoSeq = seq + 1;

                // invariante: a componente k do vetor TEM de ser igual ao seq
                if (novoV.get(k) != novoSeq) {
                    throw new IllegalStateException("Invariante quebrado: V[k]=" + novoV.get(k)
                            + " != seq=" + novoSeq);
                }

                SensorRecord r = SensorRecord.now(novoSeq, t, sensor, temp, novoL, novoV);
                acrescentar(r);                 // grava PRIMEIRO (ficheiro + memória) ...
                lamport = novoL;                // ... e só depois atualiza o estado do líder
                vector = novoV;
                seq = novoSeq;
                pendentes.add(r);               // fica à espera do próximo lote

                System.out.println("EVENTO: seq=" + novoSeq + " termo=" + t + " L=" + novoL
                        + " V=" + novoV + " (em espera para lote)");
            }
        } catch (Exception e) {
            evento("ERRO;p" + k + ";IN rejeitado: " + msg.trim() + " | " + e.getMessage());
        }
    }

    // Fecha e publica o lote. Corre no agendador, de Δ em Δ, enquanto o nó for líder.
    private void enviarLote() {
        try {
            List<SensorRecord> lote;
            long termo;
            long us;
            long ut;
            long hbAtual;

            // 1. secção sincronizada: confirma que ainda sou líder e tira o "retrato" do estado
            synchronized (this) {
                termo = raft.termIfLeader();
                if (termo == -1) {
                    // já não sou líder: cancela o agendamento e termina
                    if (agendamento != null) agendamento.cancel(false);
                    agendamento = null;
                    return;
                }
                lote = pendentes;
                pendentes = new ArrayList<>();      // os IN seguintes vão para a lista nova
                us = ultimoSeq();
                ut = ultimoTermo();
                hbAtual = ++hb;
            }

            // 2. FORA da secção: publica (não bloqueia a receção de IN enquanto se fala com o IPFS)
            if (Wire.isolated(k, i)) return;        // isolado: nada sai e não há linha em hb-*
            Batch batch = new Batch(termo, i, endereco, porto, hbAtual, us, ut, lote);
            String msg = batch.encode();
            long bytes = msg.getBytes(StandardCharsets.UTF_8).length;
            ps.publish(Wire.topicData(k), msg);

            // 3. linha em hb-p<k>-n<i>.txt:  termo;hb;instante;ultimoSeq;ultimoTermo;n;bytes
            String linha = termo + ";" + hbAtual + ";" + System.currentTimeMillis() + ";" + us + ";" + ut
                    + ";" + lote.size() + ";" + bytes + "\n";
            Files.writeString(Paths.get("hb-p" + k + "-n" + i + ".txt"), linha,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            System.out.println("HEARTBEAT: termo=" + termo + " hb=" + hbAtual + " n=" + lote.size()
                    + " ultimoSeq=" + us);
        } catch (Exception e) {
            // apanha TUDO: uma exceção não apanhada faria o agendador parar para sempre
            evento("ERRO;p" + k + ";enviarLote: " + e);
        }
    }

    // ================================================================ SEGUIDOR

    // Cada LOTE recebido (heartbeat + registos).
    private void receberLote(String msg) {
        if (Wire.isolated(k, i)) return;            // isolado: não processa
        Batch b;
        try {
            b = Batch.decode(msg, n);
        } catch (IllegalArgumentException e) {
            System.out.println("LOTE REJEITADO: " + e.getMessage());
            return;
        }
        if (b.leader == i) return;                  // eco do meu próprio lote

        boolean lancar = false;           // vou lançar uma recuperação?
        boolean divergencia = false;      // ... e é por divergência?
        long de = 0;
        long ate = 0;

        synchronized (this) {
            // 1. o RaftState decide se o lote é de um líder válido (termo igual ou superior ao meu)
            boolean aplicar = raft.onBatch(b.term, b.leader);
            limparSeNaoLider();                     // se eu era líder, já não sou
            if (!aplicar) {
                evento("IGNORADO;p" + k + ";termo=" + b.term + ";lider=n" + b.leader
                        + ";atual=" + raft.term());
                return;
            }
            // lote repetido ou atrasado dentro do mesmo termo: ignora (evita falsas divergências)
            if (b.term == ultimoTermoLote && b.hb <= ultimoHbLote) {
                evento("IGNORADO;p" + k + ";termo=" + b.term + ";hb=" + b.hb + " (repetido ou atrasado)");
                return;
            }
            ultimoTermoLote = b.term;
            ultimoHbLote = b.hb;

            String seqs = b.records.isEmpty() ? "vazio"
                    : b.records.get(0).seq + ".." + b.records.get(b.records.size() - 1).seq;
            evento("RECEBIDO;p" + k + ";termo=" + b.term + ",lider=n" + b.leader + ",hb=" + b.hb
                    + ",ultimoSeq=" + b.lastSeq + ",ultimoTermo=" + b.lastTerm
                    + ",n=" + b.records.size() + ",seqs=" + seqs);

            if (emRecuperacao) return;              // a recuperação em curso trata de tudo

            // 2. aplica os registos pela ordem
            boolean lacuna = false;
            try {
                for (SensorRecord r : b.records) {
                    long us = ultimoSeq();
                    if (r.seq <= us) {
                        // já tenho este seq: mesmo termo = duplicado; termo diferente = divergência
                        if (registos.get((int) r.seq - 1).termo == r.termo) {
                            System.out.println("DUPLICADO: " + r.seq);
                        } else {
                            divergencia = true;
                            break;
                        }
                    } else if (r.seq == us + 1) {
                        acrescentar(r);             // o próximo da sequência
                        System.out.println("Aplicado: " + r.toLine());
                    } else {
                        lacuna = true;              // falta algum antes deste
                        break;
                    }
                }
            } catch (IOException e) {
                evento("ERRO;p" + k + ";IO ao aplicar registos: " + e.getMessage());
                return;
            }

            // 3. no fim do lote, compara o meu último registo com ultimoSeq/ultimoTermo do cabeçalho
            if (!divergencia) {
                long us = ultimoSeq();
                if (us > b.lastSeq) {
                    divergencia = true;             // tenho registos para além de ultimoSeq
                } else if (us == b.lastSeq && b.lastSeq > 0
                        && registos.get((int) b.lastSeq - 1).termo != b.lastTerm) {
                    divergencia = true;             // o meu registo ultimoSeq tem outro termo
                } else if (us < b.lastSeq) {
                    lacuna = true;                  // tenho menos registos: faltam alguns
                }
            }

            // 4. decide a recuperação (a divergência tem prioridade sobre a lacuna)
            if (divergencia) {
                lancar = true;
                ate = b.lastSeq;
            } else if (lacuna) {
                lancar = true;
                de = ultimoSeq() + 1;
                ate = b.lastSeq;
                evento("RECUPERACAO;p" + k + ";de=" + de + ";ate=" + ate);
            }
            if (lancar) emRecuperacao = true;       // impede lançar outra em paralelo
        }

        if (lancar) {
            // a recuperação (TCP) corre noutra thread para NÃO bloquear a receção de lotes
            final boolean fDiv = divergencia;
            final long fAte = ate;
            final Batch fb = b;
            new Thread(() -> executarRecuperacao(fDiv, fAte, fb.leader, fb.term, fb.address, fb.port),
                    "recuperacao-p" + k + "-n" + i).start();
        }
    }

    // Primeiro seq do bloco de termo que está 'recuo' blocos antes do último.
    // Um "bloco de termo" é uma sequência contígua de registos com o mesmo termo.
    // recuo = 0 -> o último bloco; recuo = 1 -> o bloco anterior; ... (chamar com o monitor do nó)
    private long inicioBloco(int recuo) {
        long fim = ultimoSeq();
        long inicio = 1;
        for (int b = 0; b <= recuo; b++) {
            if (fim == 0) return 1;                 // já não há mais blocos para trás
            long termo = registos.get((int) fim - 1).termo;
            inicio = fim;
            while (inicio > 1 && registos.get((int) inicio - 2).termo == termo) inicio--;
            fim = inicio - 1;
        }
        return inicio;
    }

    // O líder ainda é o mesmo e o termo não mudou? (chamar com o monitor do nó)
    private boolean aindaValido(int liderId, long termoLider) {
        return raft.termIfLeader() == -1 && raft.leader() == liderId && raft.term() == termoLider;
    }

    /**
     * Recuperação assíncrona (thread própria). Dois casos:
     *  - LACUNA (divergencia=false): pede por TCP os registos que faltam e acrescenta-os;
     *  - DIVERGÊNCIA (divergencia=true): pede os registos a partir do primeiro seq do meu último bloco
     *    de termo; se o primeiro registo recebido não coincidir com o meu, recua UM bloco de termo de
     *    cada vez; quando coincide, TRUNCA o meu ficheiro a partir da primeira diferença e aplica os
     *    registos do líder.
     */
    private void executarRecuperacao(boolean divergencia, long lastSeqLider, int liderId,
                                     long termoLider, String host, int portoLider) {
        try {
            int recuo = 0;                          // quantos blocos de termo já recuámos
            while (true) {
                long s;                             // primeiro seq a pedir
                long snapshot;                      // o meu ultimoSeq no momento do pedido
                synchronized (this) {
                    if (!aindaValido(liderId, termoLider)) return;
                    snapshot = ultimoSeq();
                    s = divergencia ? inicioBloco(recuo) : snapshot + 1;
                }

                // pedido TCP FORA do monitor do nó (pode demorar até T_TCP_MS)
                List<SensorRecord> recebidos = (lastSeqLider >= s)
                        ? pedir(host, portoLider, s, lastSeqLider)
                        : new ArrayList<>();        // o líder nem tem o seq s

                synchronized (this) {
                    // se entretanto o líder ou o termo mudaram, ou o meu registo mudou, desiste
                    // (o próximo lote volta a desencadear a recuperação, se ainda for preciso)
                    if (!aindaValido(liderId, termoLider) || ultimoSeq() != snapshot) return;

                    if (!divergencia) {
                        // LACUNA: acrescenta os que continuam a sequência
                        int aplicados = 0;
                        for (SensorRecord r : recebidos) {
                            if (r.seq == ultimoSeq() + 1) {
                                acrescentar(r);
                                aplicados++;
                                System.out.println("Aplicado (recuperado): " + r.toLine());
                            }
                        }
                        System.out.println("Registos obtidos por TCP: " + aplicados);
                        return;
                    }

                    // DIVERGÊNCIA: o primeiro registo recebido coincide com o meu registo de seq s?
                    boolean coincide = !recebidos.isEmpty() && recebidos.get(0).seq == s
                            && registos.get((int) s - 1).termo == recebidos.get(0).termo;
                    if (!coincide && s > 1) {
                        recuo++;                    // não coincide: recua um bloco de termo e repete
                        continue;
                    }

                    // Determina o primeiro seq divergente (d)
                    long d;
                    if (!coincide) {
                        d = 1;                      // chegámos ao início e nada coincide: tudo diverge
                    } else {
                        d = -1;
                        for (SensorRecord r : recebidos) {
                            if (r.seq <= ultimoSeq() && registos.get((int) r.seq - 1).termo == r.termo) continue;
                            d = r.seq;              // primeiro registo do líder diferente do meu
                            break;
                        }
                        if (d == -1) {
                            // tudo o que recebi coincide: o que eu tenha a mais é divergente
                            d = recebidos.get(recebidos.size() - 1).seq + 1;
                        }
                    }

                    // TRUNCA a partir de d
                    if (d <= ultimoSeq()) {
                        List<SensorRecord> cauda = new ArrayList<>(registos.subList((int) d - 1, registos.size()));
                        Set<Long> termos = new LinkedHashSet<>();    // termos distintos descartados
                        List<String> listaTermos = new ArrayList<>();
                        for (SensorRecord r : cauda) {
                            if (termos.add(r.termo)) listaTermos.add(String.valueOf(r.termo));
                        }
                        guardarTruncados(d, cauda);                       // cópia dos descartados (para o RaftCheck)
                        registos.subList((int) d - 1, registos.size()).clear();
                        ficheiro.rewrite(registos);                        // reescrita atómica do ficheiro
                        evento("TRUNCADO;p" + k + ";de=" + d + ";descartados=" + cauda.size()
                                + ";termos=" + String.join(",", listaTermos));
                    }

                    // aplica os registos do líder a partir de d
                    int aplicados = 0;
                    for (SensorRecord r : recebidos) {
                        if (r.seq >= d && r.seq == ultimoSeq() + 1) {
                            acrescentar(r);
                            aplicados++;
                            System.out.println("Aplicado (divergência): " + r.toLine());
                        }
                    }
                    if (aplicados > 0 && d > snapshot) {
                        evento("RECUPERACAO;p" + k + ";de=" + d + ";ate=" + lastSeqLider);
                    }
                    return;
                }
            }
        } catch (Exception e) {
            evento("ERRO;p" + k + ";recuperação falhou: " + e.getMessage());
        } finally {
            synchronized (this) {
                emRecuperacao = false;              // liberta a flag para futuras tentativas
            }
        }
    }

    // Guarda os registos descartados numa truncagem em truncados-p<k>-n<i>.txt (um bloco por
    // truncagem, com um cabeçalho "#de=<seq>"). Serve só para o RaftCheck saber QUE leituras se
    // descartaram, já que o TRUNCADO só diz quantos foram.
    private void guardarTruncados(long de, List<SensorRecord> descartados) throws IOException {
        StringBuilder sb = new StringBuilder("#de=" + de + "\n");
        for (SensorRecord r : descartados) sb.append(r.toLine()).append("\n");
        Files.writeString(Paths.get("truncados-p" + k + "-n" + i + ".txt"), sb.toString(),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // Cliente TCP de recuperação: "PEDIDO <de> <até>" -> linhas de registo ... "FIM".
    // Só devolve algo se vier TUDO até ao FIM (nunca aplica uma resposta parcial).
    private List<SensorRecord> pedir(String host, int portoLider, long de, long ate) throws IOException {
        if (Wire.isolated(k, i)) throw new IOException("nó isolado: ligação TCP falhou");
        List<SensorRecord> recebidos = new ArrayList<>();
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, portoLider), Wire.T_TCP_MS);
            s.setSoTimeout(Wire.T_TCP_MS);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
            out.println("PEDIDO " + de + " " + ate);

            boolean fim = false;
            String linha;
            while ((linha = in.readLine()) != null) {
                if (linha.equals("FIM")) { fim = true; break; }
                if (linha.startsWith("ERRO")) throw new IOException("nó respondeu " + linha);
                recebidos.add(SensorRecord.fromLine(linha, n));
            }
            if (!fim) throw new IOException("resposta interrompida antes de FIM");
        }
        return recebidos;
    }

    // ================================================================ SERVIDOR TCP

    // Atende UMA ligação de recuperação: lê "PEDIDO <de> <até>", responde com registos e "FIM".
    // Qualquer nó (líder ou não) responde; só um nó isolado deixa a ligação falhar.
    private void atenderLigacao(Socket s) {
        try (Socket sock = s;
             BufferedReader in = new BufferedReader(new InputStreamReader(sock.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(sock.getOutputStream(), StandardCharsets.UTF_8), true)) {
            if (Wire.isolated(k, i)) return;        // isolado: fecha sem responder
            sock.setSoTimeout(Wire.T_TCP_MS);
            String pedido = in.readLine();
            if (pedido == null) return;

            long de;
            long ate;
            String[] p = pedido.trim().split("\\s+");
            try {
                if (p.length != 3 || !p[0].equals("PEDIDO")) throw new IllegalArgumentException();
                de = Long.parseLong(p[1]);
                ate = Long.parseLong(p[2]);
            } catch (IllegalArgumentException e) {
                out.println("ERRO esperado PEDIDO <de> <até>");
                return;
            }
            if (de < 1 || de > ate) {
                out.println("ERRO intervalo inválido " + de + ".." + ate);
                return;
            }
            // copia o intervalo pedido (nunca além do que existe) com o monitor do nó, e responde fora dele
            List<SensorRecord> resposta;
            synchronized (this) {
                long ateEfetivo = Math.min(ate, ultimoSeq());
                resposta = de > ateEfetivo ? new ArrayList<>()
                        : new ArrayList<>(registos.subList((int) de - 1, (int) ateEfetivo));
            }
            for (SensorRecord r : resposta) out.println(r.toLine());
            out.println("FIM");
            System.out.println("RECUPERAÇÃO servida: pedido " + de + ".." + ate + " (" + resposta.size() + " registos)");
        } catch (IOException e) {
            System.out.println("Recuperação interrompida: " + e.getMessage());
        }
    }
}
