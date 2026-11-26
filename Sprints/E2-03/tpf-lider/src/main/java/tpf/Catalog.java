package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Catálogo do líder (E2-03, objetivo 7): onde está cada chunk.
 *
 * Formato de {@code catalogo.txt}, uma linha por chunk:
 *
 *   chunkId;lsnDe;lsnAte;tamanho;replicas;estado
 *
 * {@code replicas} é a lista das réplicas com cópia CONFIRMADA, separada por
 * vírgulas (pode ser vazia). {@code estado} é SELADO (ainda não cumpre as
 * restrições de durabilidade) ou DURAVEL (cumpre-as). Um chunk diz-se
 * SUBREPLICADO quando tem menos de R cópias; isso é derivado do número de
 * cópias e não é um estado — um chunk pode ser DURAVEL e subreplicado.
 *
 * O ficheiro é reescrito de forma atómica (temporário + ATOMIC_MOVE) a cada
 * alteração, e é carregado no arranque do líder. Todos os métodos são
 * sincronizados: o catálogo é tocado por threads do Discord, do hub e do
 * agendador de ACK.
 */
public final class Catalog {

    public static final String FICHEIRO = "catalogo.txt";

    public enum Estado {
        SELADO,
        DURAVEL
    }

    /** Uma linha do catálogo. */
    public static final class EntradaCatalogo {
        private final String chunkId;
        private final long lsnDe;
        private final long lsnAte;
        private final long tamanho;
        private final Set<String> replicas = new LinkedHashSet<>();
        private Estado estado;

        EntradaCatalogo(String chunkId, long lsnDe, long lsnAte, long tamanho, Estado estado) {
            if (chunkId == null || chunkId.isEmpty()) throw new IllegalArgumentException("chunkId vazio");
            if (lsnDe < 1 || lsnAte < lsnDe) {
                throw new IllegalArgumentException("intervalo de LSN inválido: " + lsnDe + "-" + lsnAte);
            }
            if (tamanho < 0) throw new IllegalArgumentException("tamanho negativo");
            this.chunkId = chunkId;
            this.lsnDe = lsnDe;
            this.lsnAte = lsnAte;
            this.tamanho = tamanho;
            this.estado = estado;
        }

        public String chunkId() { return chunkId; }
        public long lsnDe() { return lsnDe; }
        public long lsnAte() { return lsnAte; }
        public long tamanho() { return tamanho; }
        public Estado estado() { return estado; }
        public Set<String> replicas() { return Set.copyOf(replicas); }
        public int numeroCopias() { return replicas.size(); }

        /** Cópia imutável para leitura fora do lock do catálogo. */
        EntradaCatalogo copia() {
            EntradaCatalogo c = new EntradaCatalogo(chunkId, lsnDe, lsnAte, tamanho, estado);
            c.replicas.addAll(replicas);
            return c;
        }
    }

    private final Path pasta;
    private final Path ficheiro;
    private final int r;
    private final int wMin;
    private final Map<String, EntradaCatalogo> chunks = new LinkedHashMap<>();

    private Catalog(Path pasta, int r, int wMin) {
        if (pasta == null) throw new IllegalArgumentException("pasta nula");
        this.pasta = pasta;
        this.ficheiro = pasta.resolve(FICHEIRO);
        this.r = r;
        this.wMin = wMin;
    }

    /** Catálogo vazio (primeiro arranque ou testes). */
    public static Catalog vazio(Path pasta) {
        return vazio(pasta, Wire.R, Wire.W_MIN);
    }

    static Catalog vazio(Path pasta, int r, int wMin) {
        return new Catalog(pasta, r, wMin);
    }

    /** Carrega {@code catalogo.txt}, se existir; um ficheiro inexistente dá um catálogo vazio. */
    public static Catalog carregar(Path pasta) throws IOException {
        return carregar(pasta, Wire.R, Wire.W_MIN);
    }

    static Catalog carregar(Path pasta, int r, int wMin) throws IOException {
        Catalog c = new Catalog(pasta, r, wMin);
        if (!Files.exists(c.ficheiro)) return c;

        List<String> linhas = Files.readAllLines(c.ficheiro, StandardCharsets.UTF_8);
        int numero = 0;
        for (String linha : linhas) {
            numero++;
            if (linha.isEmpty()) {
                // Só a última linha vazia (do '\n' final) é tolerada.
                if (numero == linhas.size()) continue;
                throw new IOException("catalogo.txt, linha " + numero + ": linha vazia");
            }
            EntradaCatalogo e = c.analisarLinha(linha, numero);
            if (c.chunks.putIfAbsent(e.chunkId, e) != null) {
                throw new IOException("catalogo.txt, linha " + numero
                        + ": chunkId repetido (" + e.chunkId + ")");
            }
        }
        return c;
    }

    private EntradaCatalogo analisarLinha(String linha, int numero) throws IOException {
        String[] p = linha.split(";", -1);
        if (p.length != 6) {
            throw new IOException("catalogo.txt, linha " + numero
                    + ": esperava 6 campos (chunkId;lsnDe;lsnAte;tamanho;replicas;estado), encontrei "
                    + p.length);
        }
        try {
            String chunkId = p[0];
            if (chunkId.isEmpty()) throw new IllegalArgumentException("chunkId vazio");
            long lsnDe = Long.parseLong(p[1]);
            long lsnAte = Long.parseLong(p[2]);
            long tamanho = Long.parseLong(p[3]);
            Estado estado = Estado.valueOf(p[5]);

            EntradaCatalogo e = new EntradaCatalogo(chunkId, lsnDe, lsnAte, tamanho, estado);
            if (!p[4].isEmpty()) {
                for (String id : p[4].split(",", -1)) {
                    if (id.isEmpty()) throw new IllegalArgumentException("réplica vazia na lista");
                    e.replicas.add(id);
                }
            }
            return e;
        } catch (IllegalArgumentException e) {
            throw new IOException("catalogo.txt, linha " + numero + ": " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------
    // Alterações
    // ------------------------------------------------------------------

    /**
     * Insere o chunk no catálogo com estado SELADO e sem cópias. Se o chunk
     * já existir (mesmo id = mesmo conteúdo), não é duplicado: um chunk é
     * imutável e endereçado pelo hash.
     */
    public synchronized EntradaCatalogo adicionar(Sealed selado, long lsnDe, long lsnAte)
            throws IOException {
        if (selado == null) throw new IllegalArgumentException("chunk nulo");
        EntradaCatalogo existente = chunks.get(selado.chunkId());
        if (existente != null) return existente.copia();

        EntradaCatalogo e = new EntradaCatalogo(selado.chunkId(), lsnDe, lsnAte,
                selado.tamanhoCifrado(), Estado.SELADO);
        chunks.put(e.chunkId, e);
        gravarAtomico();
        return e.copia();
    }

    /**
     * Acrescenta uma cópia confirmada por ACK e atualiza o estado para
     * DURAVEL quando o número de cópias atinge
     * {@code max(W_MIN, ⌊R/2⌋+1)}. Devolve false se o chunk não existir.
     */
    public synchronized boolean confirmar(String chunkId, String replica) throws IOException {
        if (chunkId == null || replica == null || replica.isEmpty()) return false;
        EntradaCatalogo e = chunks.get(chunkId);
        if (e == null) return false;

        boolean mudou = e.replicas.add(replica);
        if (e.estado == Estado.SELADO
                && e.replicas.size() >= Wire.quorumDurabilidade(r, wMin)) {
            e.estado = Estado.DURAVEL;
            mudou = true;
        }
        if (mudou) gravarAtomico();
        return true;
    }

    // ------------------------------------------------------------------
    // Leitura
    // ------------------------------------------------------------------

    public synchronized Set<String> copias(String chunkId) {
        EntradaCatalogo e = chunks.get(chunkId);
        return e == null ? Set.of() : Set.copyOf(e.replicas);
    }

    public synchronized Estado estado(String chunkId) {
        EntradaCatalogo e = chunks.get(chunkId);
        return e == null ? null : e.estado;
    }

    public synchronized EntradaCatalogo entrada(String chunkId) {
        EntradaCatalogo e = chunks.get(chunkId);
        return e == null ? null : e.copia();
    }

    public synchronized List<EntradaCatalogo> entradas() {
        List<EntradaCatalogo> out = new ArrayList<>(chunks.size());
        for (EntradaCatalogo e : chunks.values()) out.add(e.copia());
        out.sort(Comparator.comparing(EntradaCatalogo::chunkId));
        return out;
    }

    public synchronized int total() {
        return chunks.size();
    }

    public synchronized int contar(Estado estado) {
        int n = 0;
        for (EntradaCatalogo e : chunks.values()) if (e.estado == estado) n++;
        return n;
    }

    /** Chunks com menos de R cópias confirmadas (condição derivada, não um estado). */
    public synchronized int subreplicados() {
        int n = 0;
        for (EntradaCatalogo e : chunks.values()) if (e.replicas.size() < r) n++;
        return n;
    }

    /** Quantas entradas do catálogo têm cópia confirmada na réplica indicada. */
    public synchronized int chunksPorReplica(String replica) {
        int n = 0;
        for (EntradaCatalogo e : chunks.values()) if (e.replicas.contains(replica)) n++;
        return n;
    }

    public int r() { return r; }

    public int wMin() { return wMin; }

    // ------------------------------------------------------------------
    // Persistência
    // ------------------------------------------------------------------

    /**
     * Reescreve o catálogo atomicamente: escreve um temporário na mesma pasta,
     * força-o para o disco e só depois faz ATOMIC_MOVE sobre o ficheiro final.
     * Um leitor nunca vê um catálogo a meio; um crash deixa o anterior.
     */
    public synchronized void gravarAtomico() throws IOException {
        Files.createDirectories(pasta);

        List<EntradaCatalogo> ordenadas = new ArrayList<>(chunks.values());
        ordenadas.sort(Comparator.comparing(EntradaCatalogo::chunkId));

        StringBuilder sb = new StringBuilder();
        for (EntradaCatalogo e : ordenadas) {
            List<String> reps = new ArrayList<>(e.replicas);
            reps.sort(String::compareTo);
            sb.append(e.chunkId).append(';')
                    .append(e.lsnDe).append(';')
                    .append(e.lsnAte).append(';')
                    .append(e.tamanho).append(';')
                    .append(String.join(",", reps)).append(';')
                    .append(e.estado.name()).append('\n');
        }

        Path temporario = pasta.resolve("." + FICHEIRO + "." + UUID.randomUUID() + ".tmp");
        try {
            try (FileChannel ch = FileChannel.open(temporario, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                ByteBuffer buf = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
                while (buf.hasRemaining()) ch.write(buf);
                ch.force(true);
            }
            try {
                Files.move(temporario, ficheiro, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                // Fallback documentado: nunca acontece em sistemas de ficheiros locais.
                Files.move(temporario, ficheiro, StandardCopyOption.REPLACE_EXISTING);
            }
            LogRegistos.sincronizarPasta(pasta);   // entrada de diretório durável
        } finally {
            Files.deleteIfExists(temporario);
        }
    }
}
