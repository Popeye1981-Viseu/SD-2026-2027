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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Armazenamento cego de uma réplica (E2-03, objetivo 4).
 *
 * A réplica guarda apenas bytes cifrados:
 *
 *   <chunkId>.bin   o texto cifrado, incluindo a tag GCM
 *   <chunkId>.hdr   o cabeçalho público (keyVersion;salt;nonce)
 *
 * Invariantes:
 *  - o nome do `.bin` é o SHA-256 do seu conteúdo;
 *  - `usados` é a soma dos `.bin` da pasta;
 *  - o ACK só é publicado depois de os dois ficheiros estarem duráveis;
 *  - um STORE de um chunk já existente responde com ACK sem reescrever.
 *
 * O armazém não conhece a master key, o catálogo nem o conteúdo dos registos.
 */
public final class Armazem {

    public enum Resultado { OK, QUOTA, HASH, ERRO }

    public record Rececao(Resultado resultado, String sha256, String detalhe) {}

    public static final String EXT_BIN = ".bin";
    public static final String EXT_HDR = ".hdr";

    private final Path pasta;
    private final long quota;
    private final EventosReplica eventos;

    private long usados;
    private int escritas;

    public Armazem(Path pasta, long quota, EventosReplica eventos) throws IOException {
        if (pasta == null) throw new IllegalArgumentException("pasta nula");
        if (quota < 0) throw new IllegalArgumentException("quota negativa");
        if (eventos == null) throw new IllegalArgumentException("eventos nulos");
        this.pasta = pasta;
        this.quota = quota;
        this.eventos = eventos;
        Files.createDirectories(pasta);
        this.usados = calcularUsados(pasta);
    }

    /** Soma dos tamanhos dos `.bin` da pasta (fonte de verdade dos usados). */
    public static long calcularUsados(Path pasta) throws IOException {
        if (!Files.isDirectory(pasta)) return 0;
        long total = 0;
        try (Stream<Path> ficheiros = Files.list(pasta)) {
            for (Path p : (Iterable<Path>) ficheiros::iterator) {
                if (Files.isRegularFile(p) && p.getFileName().toString().endsWith(EXT_BIN)) {
                    total += Files.size(p);
                }
            }
        }
        return total;
    }

    public synchronized long usados() { return usados; }

    public long quota() { return quota; }

    /** Número de escritas novas (não conta ACK de chunks já existentes). */
    synchronized int escritas() { return escritas; }

    /** Chunks presentes na pasta, por ordem (para o INVENTARIO). */
    public synchronized List<String> inventario() throws IOException {
        List<String> ids = new ArrayList<>();
        try (Stream<Path> ficheiros = Files.list(pasta)) {
            ficheiros.filter(p -> Files.isRegularFile(p)
                            && p.getFileName().toString().endsWith(EXT_BIN))
                    .forEach(p -> {
                        String nome = p.getFileName().toString();
                        ids.add(nome.substring(0, nome.length() - EXT_BIN.length()));
                    });
        }
        ids.sort(Comparator.naturalOrder());
        return ids;
    }

    /**
     * Recebe um STORE. A ordem reflete duas regras do enunciado:
     *
     *   1. valida os campos e o SHA-256 do bin; se não for o chunkId, recusa
     *      (HASH) — é barato e não depende do disco;
     *   2. se o `.bin` já existe (e o seu conteúdo continua a ter o hash do
     *      nome), responde OK sem reescrever; um duplicado é um ACK idempotente
     *      e NÃO consome espaço novo, por isso nunca é recusado por quota;
     *   3. só quando o `.bin` não existe é que verifica a quota (QUOTA);
     *   4. grava `.hdr` e `.bin` com temporário + ATOMIC_MOVE, atualiza usados;
     *   5. só depois disto o chamador pode publicar o ACK.
     */
    public synchronized Rececao receber(String chunkId, byte[] hdr, byte[] bin) {
        if (chunkId == null || chunkId.isEmpty() || hdr == null || bin == null) {
            return new Rececao(Resultado.ERRO, null, "STORE com campos em falta");
        }

        // 1. Endereçamento por conteúdo: o nome tem de ser o hash do conteúdo.
        String sha = sha256Hex(bin);
        if (!sha.equalsIgnoreCase(chunkId)) {
            String detalhe = "INTEGRIDADE: SHA-256 do bin (" + sha
                    + ") diferente do chunkId (" + chunkId + ")";
            eventos.registarEImprimir("ERRO", detalhe);
            return new Rececao(Resultado.HASH, sha, detalhe);
        }

        Path ficheiroBin = pasta.resolve(chunkId + EXT_BIN);
        Path ficheiroHdr = pasta.resolve(chunkId + EXT_HDR);

        try {
            // 2. Um chunk já gravado responde OK sem reescrever, mesmo que a
            // quota esteja cheia: não ocupa espaço novo e o ACK é idempotente.
            if (Files.exists(ficheiroBin)) {
                // Confirma que o conteúdo continua a ser o chunk endereçado;
                // chunks são imutáveis.
                String existente = sha256Hex(Files.readAllBytes(ficheiroBin));
                if (!existente.equalsIgnoreCase(chunkId)) {
                    String detalhe = "INTEGRIDADE: o .bin existente de " + chunkId
                            + " tem SHA-256 " + existente;
                    eventos.registarEImprimir("ERRO", detalhe);
                    return new Rececao(Resultado.HASH, existente, detalhe);
                }
                if (Files.exists(ficheiroHdr)) {
                    eventos.registarEImprimir("STORE",
                            "chunkId=" + chunkId + " bytes=" + bin.length + " (já existia)");
                    return new Rececao(Resultado.OK, sha, "já existia");
                }
                // .bin válido mas .hdr em falta (crash entre os dois moves):
                // completa o par, sem voltar a escrever o .bin.
                escreverAtomico(ficheiroHdr, hdr);
                sincronizarPasta();
                eventos.registarEImprimir("STORE",
                        "chunkId=" + chunkId + " bytes=" + bin.length + " (faltava .hdr)");
                return new Rececao(Resultado.OK, sha, "completado .hdr");
            }

            // 3. Só um chunk novo consome quota. Uma réplica cheia recusa antes
            // de gastar I/O (e o líder tentará outra réplica).
            if (bin.length > quota - usados) {
                String detalhe = "chunkId=" + chunkId + " bytes=" + bin.length
                        + " usados=" + usados + " quota=" + quota;
                eventos.registarEImprimir("QUOTA", detalhe);
                return new Rececao(Resultado.QUOTA, null, detalhe);
            }

            // 4. Escreve primeiro o cabeçalho e só depois o bin: o bin é a parte
            // grande e é o que os leitores procuram; um .hdr órfão é inofensivo.
            escreverAtomico(ficheiroHdr, hdr);
            escreverAtomico(ficheiroBin, bin);
            sincronizarPasta();

            usados += bin.length;
            escritas++;
            eventos.registarEImprimir("STORE",
                    "chunkId=" + chunkId + " bytes=" + bin.length + " usados=" + usados);
            return new Rececao(Resultado.OK, sha, "gravado");

        } catch (IOException e) {
            eventos.registarEImprimir("ERRO",
                    "falha ao gravar o chunk " + chunkId + ": " + e.getMessage());
            return new Rececao(Resultado.ERRO, sha, e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Auxiliares
    // ------------------------------------------------------------------

    public static String sha256Hex(byte[] dados) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(dados));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponível nesta JVM", e);
        }
    }

    /** Ficheiro temporário na mesma pasta + ATOMIC_MOVE (o leitor nunca vê metade). */
    private void escreverAtomico(Path destino, byte[] dados) throws IOException {
        Path temporario = pasta.resolve("." + destino.getFileName() + "." + UUID.randomUUID() + ".tmp");
        try {
            try (FileChannel ch = FileChannel.open(temporario, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                ByteBuffer buf = ByteBuffer.wrap(dados);
                while (buf.hasRemaining()) ch.write(buf);
                ch.force(true);
            }
            try {
                Files.move(temporario, destino, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporario, destino, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporario);
        }
    }

    private void sincronizarPasta() {
        try (FileChannel ch = FileChannel.open(pasta, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignorada) {
            // best-effort, documentado no README
        }
    }

    /** Usado pelos testes para verificar o que ficou em disco. */
    static String lerTexto(Path ficheiro) throws IOException {
        return Files.readString(ficheiro, StandardCharsets.UTF_8);
    }
}
