package tpf;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Colocação ponderada pelo espaço livre com rendezvous hashing (E2-03,
 * objetivo 6).
 *
 * Para cada par (chunk, réplica):
 *
 *   u(c, r) = ((primeiros 53 bits de SHA-256(c + "|" + r)) + 0.5) / 2^53
 *   pontuação(c, r) = livre(r)^ALFA / -ln(u(c, r))
 *
 * As {@code count} réplicas com maior pontuação recebem o chunk. O sorteio é
 * determinístico para o mesmo chunk e o mesmo estado, o que torna os ensaios
 * reproduzíveis. Duas chamadas concorrentes com o mesmo estado devolvem a
 * mesma lista; o que impede a sobrelotação é a reserva atómica feita pelo
 * {@link ReplicaRegistry}.
 */
public final class Placement {

    /** Vista de uma réplica no instante da decisão. */
    public static final class Candidate {
        public final String id;
        public final long free;          // quota − usados − reservados, em bytes

        public Candidate(String id, long free) {
            if (id == null || id.isEmpty()) throw new IllegalArgumentException("id vazio");
            this.id = id;
            this.free = free;
        }
    }

    private final double alpha;
    private final long margin;

    public Placement(double alpha, long margin) {
        if (alpha < 0) throw new IllegalArgumentException("alpha tem de ser >= 0");
        if (margin < 0) throw new IllegalArgumentException("margin tem de ser >= 0");
        this.alpha = alpha;
        this.margin = margin;
    }

    public double alpha() { return alpha; }

    public long margin() { return margin; }

    /**
     * u(c, r) uniforme em ]0, 1[, a partir dos primeiros 53 bits de
     * SHA-256(chunkId + "|" + replicaId). Os 53 bits são os que um double
     * representa exatamente; o +0.5 e o deslocamento de 11 bits garantem que
     * o valor nunca é 0 nem 1 (o que faria -ln(u) explodir ou dar 0).
     */
    static double unit(String chunkId, String replicaId) {
        try {
            byte[] h = MessageDigest.getInstance("SHA-256")
                    .digest((chunkId + "|" + replicaId).getBytes(StandardCharsets.UTF_8));
            long v = 0;
            for (int i = 0; i < 8; i++) v = (v << 8) | (h[i] & 0xff);
            v >>>= 11;
            return (v + 0.5) / (double) (1L << 53);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponível nesta JVM", e);
        }
    }

    double score(String chunkId, Candidate c) {
        return Math.pow((double) c.free, alpha) / -Math.log(unit(chunkId, c.id));
    }

    /**
     * Devolve até {@code count} réplicas, por ordem decrescente de pontuação.
     * Pode devolver menos do que {@code count} (ou nenhuma) — quem chama tem
     * de tratar o caso SUBREPLICADO. O desempate por id garante que a ordem
     * não depende da ordem de iteração da coleção de entrada.
     */
    public List<String> choose(String chunkId, long size, int count,
                               Collection<Candidate> alive, Set<String> exclude) {
        if (chunkId == null || chunkId.isEmpty()) throw new IllegalArgumentException("chunkId vazio");
        if (count <= 0) return List.of();
        if (alive == null) return List.of();
        Set<String> excluidas = exclude == null ? Set.of() : exclude;

        List<Candidate> ok = new ArrayList<>();
        for (Candidate c : alive) {
            if (!excluidas.contains(c.id) && c.free >= size + margin) ok.add(c);
        }
        ok.sort(Comparator.comparingDouble((Candidate c) -> score(chunkId, c))
                .reversed()
                .thenComparing(c -> c.id));

        List<String> out = new ArrayList<>();
        for (int i = 0; i < Math.min(count, ok.size()); i++) out.add(ok.get(i).id);
        return out;
    }
}
