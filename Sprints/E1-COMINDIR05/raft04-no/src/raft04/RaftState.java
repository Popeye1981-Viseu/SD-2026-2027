package raft04;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * RaftState = o "cérebro" da eleição de UM nó de UMA partição (código dado pela ficha).
 *
 * Guarda e decide tudo o que tem a ver com termos e votos:
 *   - o TERMO atual e em quem se votou nele (votedFor)  -> estado PERSISTENTE, gravado em disco;
 *   - o PAPEL do nó (seguidor, candidato ou líder), quem é o líder, os votos recebidos e o
 *     PRAZO DE ELEIÇÃO -> estado volátil.
 * Não sabe nada de rede nem de registos: o PartitionNode chama estes métodos quando
 * chega uma mensagem ou quando passa o tempo, e age conforme o que eles devolvem.
 *
 * Todos os métodos públicos são synchronized (monitor desta instância).
 */
public final class RaftState {
    public enum Role { SEGUIDOR, CANDIDATO, LIDER }

    // resposta a um PEDIDO_VOTO: termo em que é dada e se o voto foi concedido
    public static final class Vote {
        public final long term;
        public final boolean granted;
        Vote(long term, boolean granted) { this.term = term; this.granted = granted; }
    }

    private final int partition;       // partição de dados deste grupo
    private final int self;            // identificador deste nó (1..G)
    private final int groupSize;       // G = tamanho do grupo
    private final long minNanos;       // limite inferior do prazo de eleição
    private final long maxNanos;       // limite superior do prazo de eleição
    private final Path stateFile;      // ficheiro do estado persistente (termo;votouEm)
    private final Consumer<String> log; // para onde vão os eventos (o PartitionNode passa o seu)

    // estado persistente: gravado antes de qualquer mensagem que dele dependa
    private long term = 0;
    private int votedFor = 0;              // 0 = ainda não votou neste termo

    // estado volátil
    private Role role = Role.SEGUIDOR;
    private int leader = 0;                // 0 = desconhecido
    private final Set<Integer> votes = new HashSet<>();   // quem votou em mim neste termo (inclui-me)
    private long deadline;                 // System.nanoTime() a partir do qual há eleição

    public RaftState(int partition, int self, int groupSize, long minMs, long maxMs,
                     Path stateFile, Consumer<String> log) throws IOException {
        this.partition = partition;
        this.self = self;
        this.groupSize = groupSize;
        this.minNanos = minMs * 1_000_000L;
        this.maxNanos = maxMs * 1_000_000L;
        this.stateFile = stateFile;
        this.log = log;
        // se o nó já correu antes, retoma o termo e o voto gravados (não pode votar duas vezes no mesmo termo)
        if (Files.exists(stateFile)) {
            String[] f = Files.readString(stateFile, StandardCharsets.UTF_8).trim().split(";");
            term = Long.parseLong(f[0]);
            votedFor = Integer.parseInt(f[1]);
        }
        resetTimer();
    }

    // sorteia um novo prazo de eleição, uniforme em [min, max], contado a partir de agora
    private void resetTimer() {
        long t = (maxNanos > minNanos)
                ? ThreadLocalRandom.current().nextLong(minNanos, maxNanos + 1)
                : minNanos;                       // min == max: prazo FIXO (ensaio 9(b))
        deadline = System.nanoTime() + t;
    }

    // grava "termo;votouEm" de forma atómica: ficheiro temporário + ATOMIC_MOVE
    private void persist() {
        try {
            Path tmp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            Files.writeString(tmp, term + ";" + votedFor + "\n", StandardCharsets.UTF_8);
            Files.move(tmp, stateFile,
                    StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // termo superior observado: adota-o e passa a seguidor
    private void adoptTerm(long newTerm) {
        if (role == Role.LIDER) log.accept("DEMISSAO;p" + partition + ";termo=" + term);
        log.accept("TERMO;p" + partition + ";" + term + "->" + newTerm);
        term = newTerm;
        votedFor = 0;                 // termo novo: ainda não votei nele
        role = Role.SEGUIDOR;
        leader = 0;
        persist();
    }

    // thread de verificação, a cada VERIFICACAO_MS: devolve o termo da candidatura ou -1
    public synchronized long tick() {
        if (role == Role.LIDER || System.nanoTime() < deadline) return -1;
        term++;                       // novo termo
        votedFor = self;              // vota em si próprio
        persist();                    // grava ANTES de pedir votos
        role = Role.CANDIDATO;
        leader = 0;
        votes.clear();
        votes.add(self);
        resetTimer();                 // novo prazo: se não houver maioria, nova eleição
        log.accept("CANDIDATO;p" + partition + ";termo=" + term);
        return term;
    }

    // PEDIDO_VOTO de outro nó
    public synchronized Vote onVoteRequest(long cTerm, int candidate, long cLastSeq, long cLastTerm,
                                           long myLastSeq, long myLastTerm) {
        if (cTerm > term) adoptTerm(cTerm);
        // restrição de voto: o registo do candidato tem de estar pelo menos tão atualizado como o meu
        boolean upToDate = cLastTerm > myLastTerm
                || (cLastTerm == myLastTerm && cLastSeq >= myLastSeq);
        boolean granted = cTerm == term
                && (votedFor == 0 || votedFor == candidate)
                && upToDate;
        if (granted) {
            votedFor = candidate;
            persist();                // grava ANTES de o VOTO ser publicado
            resetTimer();             // conceder um voto também renova o prazo
        }
        log.accept("VOTO;p" + partition + ";termo=" + term + ";candidato=n" + candidate
                + ";" + (granted ? "concedido" : "recusado"));
        return new Vote(term, granted);
    }

    // VOTO recebido: devolve true apenas no instante em que este nó se torna líder
    public synchronized boolean onVote(long vTerm, int voter, int candidate, boolean granted) {
        if (vTerm > term) { adoptTerm(vTerm); return false; }
        if (role != Role.CANDIDATO || vTerm != term || candidate != self || !granted) return false;
        votes.add(voter);
        if (votes.size() <= groupSize / 2) return false;      // ainda sem maioria
        role = Role.LIDER;
        leader = self;
        log.accept("LIDER;p" + partition + ";termo=" + term + ";votos=" + votes);
        return true;
    }

    // LOTE de outro nó: devolve true se o lote deve ser aplicado
    public synchronized boolean onBatch(long bTerm, int bLeader) {
        if (bTerm < term) return false;                    // líder obsoleto
        if (bTerm > term) adoptTerm(bTerm);
        if (role == Role.LIDER) {                          // nunca deveria acontecer
            log.accept("ERRO;p" + partition + ";dois lideres no termo " + term);
            return false;
        }
        role = Role.SEGUIDOR;                              // um candidato reconhece o líder
        if (leader != bLeader) {
            leader = bLeader;
            log.accept("NOVO_LIDER;p" + partition + ";termo=" + term + ";lider=n" + bLeader);
        }
        resetTimer();                                      // ouvi o líder: renova o prazo
        return true;
    }

    public synchronized long termIfLeader() { return role == Role.LIDER ? term : -1; }
    public synchronized long term()         { return term; }
    public synchronized Role role()         { return role; }
    public synchronized int leader()        { return leader; }
}
