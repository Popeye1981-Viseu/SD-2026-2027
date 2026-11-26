package tpf;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Registo das réplicas conhecidas pelo líder (E2-03, objetivo 5).
 *
 * Por réplica guarda o estado ({@link Estado}), a quota e os bytes usados
 * reportados no último heartbeat, os bytes reservados por colocações ainda
 * sem ACK e o instante do último heartbeat (num {@link HeartbeatMonitor}).
 *
 * Invariantes:
 *   livre(r) = quota − usados − reservados
 *   usados  e reservados são sempre >= 0
 *   só réplicas não MORTA são candidatas à colocação
 *
 * A operação crítica é {@link #reserveAndChoose}: a escolha e a soma das
 * reservas acontecem **na mesma secção sincronizada**. Sem isso, dois chunks
 * selados ao mesmo tempo poderiam escolher a mesma réplica com base no mesmo
 * espaço livre e exceder a quota.
 */
public final class ReplicaRegistry {

    /** Instantâneo imutável de uma réplica, para leitura por /estado e testes. */
    public record ReplicaInfo(String id, Estado estado, long quota, long usados,
                              long reservados, long ultimoHb) {
        public long livre() {
            return quota - usados - reservados;
        }
    }

    private static final class Replica {
        final String id;
        final HeartbeatMonitor monitor;
        Estado estado;
        long quota;
        long usados;
        long reservados;

        Replica(String id, Estado estado, HeartbeatMonitor monitor) {
            this.id = id;
            this.estado = estado;
            this.monitor = monitor;
        }

        long livre() {
            return quota - usados - reservados;
        }
    }

    private final Map<String, Replica> replicas = new HashMap<>();
    private final Eventos eventos;
    private final Placement placement;
    private final LongSupplier relogio;

    public ReplicaRegistry(Eventos eventos) {
        this(eventos, new Placement(Wire.ALFA, Wire.MARGEM_BYTES), System::currentTimeMillis);
    }

    ReplicaRegistry(Eventos eventos, Placement placement) {
        this(eventos, placement, System::currentTimeMillis);
    }

    ReplicaRegistry(Eventos eventos, Placement placement, LongSupplier relogio) {
        if (eventos == null) throw new IllegalArgumentException("eventos nulo");
        if (placement == null) throw new IllegalArgumentException("placement nulo");
        if (relogio == null) throw new IllegalArgumentException("relógio nulo");
        this.eventos = eventos;
        this.placement = placement;
        this.relogio = relogio;
    }

    // ------------------------------------------------------------------
    // Entrada e saída de réplicas
    // ------------------------------------------------------------------

    /**
     * Regista uma réplica que está em replicas.txt mas ainda não deu sinal de
     * vida neste arranque. Fica MORTA e fora da colocação até ao primeiro
     * heartbeat. Não emite evento (não é uma transição observada).
     */
    public synchronized void conhecida(String id) {
        if (id == null || id.isEmpty()) throw new IllegalArgumentException("id vazio");
        replicas.computeIfAbsent(id,
                k -> new Replica(k, Estado.MORTA, new HeartbeatMonitor()));
    }

    /**
     * Marca a réplica como autenticada no hub (OLA + PROVA válida). Uma
     * ligação autenticada prova que a réplica está viva agora, por isso passa
     * a VIVA e o temporizador de heartbeat é reiniciado.
     */
    public synchronized void autenticada(String id) {
        Replica r = obterOuCriar(id);
        r.monitor.onHeartbeat(relogio.getAsLong());
        transitar(r, Estado.VIVA);
    }

    /** Atualiza quota/usados reportados no heartbeat e passa a réplica a VIVA. */
    public synchronized void onHeartbeat(Wire.Hb hb) {
        onHeartbeat(hb, relogio.getAsLong());
    }

    /** Variante com instante explícito (testes determinísticos). */
    synchronized void onHeartbeat(Wire.Hb hb, long agora) {
        if (hb == null) throw new IllegalArgumentException("heartbeat nulo");
        Replica r = obterOuCriar(hb.id());
        r.quota = hb.quota();
        r.usados = hb.usados();
        r.monitor.onHeartbeat(agora);
        transitar(r, Estado.VIVA);
    }

    /** Remove a réplica do registo (usado por /replica-remover). */
    public synchronized boolean remover(String id) {
        return replicas.remove(id) != null;
    }

    // ------------------------------------------------------------------
    // Estados
    // ------------------------------------------------------------------

    /**
     * Aplica SUSPEITA_MS e MORTA_MS a todas as réplicas. Deve ser chamado
     * periodicamente pelo líder; {@link #reserveAndChoose} também o chama,
     * para nunca escolher uma réplica com estado desatualizado.
     */
    public synchronized void atualizarEstados(long agora) {
        for (Replica r : replicas.values()) {
            transitar(r, r.monitor.estado(agora));
        }
    }

    private void transitar(Replica r, Estado novo) {
        Estado antigo = r.estado;
        if (antigo == novo) return;
        r.estado = novo;
        // Formato exigido: ESTADO <id> <antigo> -> <novo>
        eventos.registar("ESTADO", r.id + " " + antigo + " -> " + novo);
    }

    private Replica obterOuCriar(String id) {
        if (id == null || id.isEmpty()) throw new IllegalArgumentException("id vazio");
        return replicas.computeIfAbsent(id,
                k -> new Replica(k, Estado.MORTA, new HeartbeatMonitor()));
    }

    // ------------------------------------------------------------------
    // Colocação e reservas
    // ------------------------------------------------------------------

    /**
     * Escolhe até {@code count} réplicas e reserva {@code tamanho} bytes em
     * cada uma, tudo dentro da mesma secção sincronizada.
     *
     * Os candidatos são as réplicas que não estão MORTA (VIVA e SUSPEITA
     * contam: uma réplica SUSPEITA pode voltar a qualquer momento e os seus
     * chunks continuam válidos) e que têm
     * {@code livre >= tamanho + MARGEM_BYTES}. As réplicas em {@code exclude}
     * não são consideradas.
     */
    public synchronized List<String> reserveAndChoose(String chunkId, long tamanho,
                                                      int count, Set<String> exclude) {
        if (chunkId == null || chunkId.isEmpty()) throw new IllegalArgumentException("chunkId vazio");
        if (tamanho < 0) throw new IllegalArgumentException("tamanho negativo");
        if (count < 0) throw new IllegalArgumentException("count negativo");

        // Um estado desatualizado escolheria uma réplica já morta.
        atualizarEstados(relogio.getAsLong());

        List<Placement.Candidate> candidatos = new ArrayList<>();
        for (Replica r : replicas.values()) {
            if (r.estado != Estado.MORTA) {
                candidatos.add(new Placement.Candidate(r.id, r.livre()));
            }
        }

        List<String> escolhidas = placement.choose(chunkId, tamanho, count,
                candidatos, exclude == null ? Set.of() : exclude);

        // A reserva acontece ANTES de devolver: quem chama não pode escolher
        // e reservar em passos separados sem perder a atomicidade.
        for (String id : escolhidas) {
            Replica r = replicas.get(id);
            if (r != null) r.reservados += tamanho;
        }
        return escolhidas;
    }

    /**
     * Converte uma reserva confirmada por ACK em espaço usado. Se a reserva
     * já tiver sido libertada por timeout (ACK atrasado), o valor é limitado
     * a zero — os usados passam a refletir o chunk, que a réplica tem.
     */
    public synchronized boolean confirmarReserva(String id, long tamanho) {
        Replica r = replicas.get(id);
        if (r == null) return false;
        if (tamanho < 0) throw new IllegalArgumentException("tamanho negativo");
        r.reservados = Math.max(0, r.reservados - tamanho);
        r.usados += tamanho;
        return true;
    }

    /** Liberta uma reserva que não foi confirmada (fim de ACK_TIMEOUT_MS). */
    public synchronized boolean libertar(String id, long tamanho) {
        Replica r = replicas.get(id);
        if (r == null) return false;
        if (tamanho < 0) throw new IllegalArgumentException("tamanho negativo");
        r.reservados = Math.max(0, r.reservados - tamanho);
        return true;
    }

    // ------------------------------------------------------------------
    // Leitura
    // ------------------------------------------------------------------

    /** Estado atual (null se a réplica não é conhecida). */
    public synchronized Estado estado(String id) {
        Replica r = replicas.get(id);
        return r == null ? null : r.estado;
    }

    /** Espaço livre efetivo (null se a réplica não é conhecida). */
    public synchronized Long livre(String id) {
        Replica r = replicas.get(id);
        return r == null ? null : r.livre();
    }

    public synchronized int total() {
        return replicas.size();
    }

    public synchronized ReplicaInfo info(String id) {
        Replica r = replicas.get(id);
        return r == null ? null : new ReplicaInfo(r.id, r.estado, r.quota,
                r.usados, r.reservados, r.monitor.ultimoHb());
    }

    /** Instantâneo de todas as réplicas, ordenado por id. */
    public synchronized List<ReplicaInfo> listar() {
        List<ReplicaInfo> out = new ArrayList<>(replicas.size());
        for (Replica r : replicas.values()) {
            out.add(new ReplicaInfo(r.id, r.estado, r.quota, r.usados,
                    r.reservados, r.monitor.ultimoHb()));
        }
        out.sort(Comparator.comparing(ReplicaInfo::id));
        return out;
    }

    /** Identificadores das réplicas não MORTA (usado nos testes). */
    synchronized Set<String> vivasOuSuspeitas() {
        Set<String> out = new HashSet<>();
        for (Replica r : replicas.values()) {
            if (r.estado != Estado.MORTA) out.add(r.id);
        }
        return out;
    }
}
