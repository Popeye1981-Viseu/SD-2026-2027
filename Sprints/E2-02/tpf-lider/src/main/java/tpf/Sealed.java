package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.List;

/**
 * Chunk selado em disco (E2-02, objetivo 4): par de ficheiros na pasta `selados/`.
 *
 *   selados/<chunkId>.bin   texto cifrado, incluindo a tag GCM (16 bytes)
 *   selados/<chunkId>.hdr   texto: "keyVersion;saltBase64;nonceBase64\n"
 *
 * <chunkId> é o SHA-256 hexadecimal do CONTEÚDO do `.bin`, pelo que o nome do
 * ficheiro é verificável: `sha256sum selados/<id>.bin` devolve <id>.
 * O `.hdr` é metadado do mesmo chunk e usa o mesmo id (não é, ele próprio, um
 * hash do seu conteúdo — o id identifica o chunk, não cada ficheiro).
 */
public record Sealed(String chunkId, ChunkCrypto.Metadados meta, Path bin, Path hdr, long tamanhoCifrado) {

    /** Subpasta onde os chunks selados são guardados. */
    public static final String PASTA = "selados";
    public static final String EXT_BIN = ".bin";
    public static final String EXT_HDR = ".hdr";

    // ------------------------------------------------------------------
    // Escrita
    // ------------------------------------------------------------------

    /**
     * Grava `.bin` e `.hdr` de forma durável: cada ficheiro com `force(true)` e,
     * no fim, um `fsync` à pasta para que as entradas de diretório também
     * sobrevivam a um crash.
     */
    public static Sealed gravar(Path pastaDados, ChunkCrypto.Cifrado c) throws IOException {
        Path pasta = pastaDados.resolve(PASTA);
        Files.createDirectories(pasta);

        String id = c.chunkId();
        Path bin = pasta.resolve(id + EXT_BIN);
        Path hdr = pasta.resolve(id + EXT_HDR);

        // O .bin é escrito primeiro: se o .hdr faltar, o chunk é irrecuperável, mas o
        // nome continua a ser verificável e o ficheiro não fica com metade dos bytes.
        escreverForcado(bin, c.textoCifrado());

        String linhaHdr = c.meta().keyVersion() + ";"
                + Base64.getEncoder().encodeToString(c.meta().salt()) + ";"
                + Base64.getEncoder().encodeToString(c.meta().nonce()) + "\n";
        escreverForcado(hdr, linhaHdr.getBytes(StandardCharsets.UTF_8));

        LogRegistos.sincronizarPasta(pasta);   // entradas de diretório duráveis

        return new Sealed(id, c.meta(), bin, hdr, c.textoCifrado().length);
    }

    // ------------------------------------------------------------------
    // Leitura
    // ------------------------------------------------------------------

    /** Lê os metadados (`keyVersion`, `salt`, `nonce`) de um chunk já selado. */
    public static Sealed ler(Path pastaDados, String chunkId) throws IOException, ChunkCrypto.CifraException {
        Path pasta = pastaDados.resolve(PASTA);
        Path bin = pasta.resolve(chunkId + EXT_BIN);
        Path hdr = pasta.resolve(chunkId + EXT_HDR);

        if (!Files.exists(bin)) {
            throw new ChunkCrypto.CifraException("chunk " + chunkId + " não existe em " + pasta);
        }
        if (!Files.exists(hdr)) {
            throw new ChunkCrypto.CifraException("falta o metadado " + hdr.getFileName()
                    + " do chunk " + chunkId);
        }

        String[] campos = Files.readString(hdr, StandardCharsets.UTF_8).trim().split(";", -1);
        if (campos.length != 3) {
            throw new ChunkCrypto.CifraException("cabeçalho inválido em " + hdr.getFileName()
                    + ": esperava keyVersion;salt;nonce");
        }

        int keyVersion;
        byte[] salt, nonce;
        try {
            keyVersion = Integer.parseInt(campos[0].trim());
            salt = Base64.getDecoder().decode(campos[1].trim());
            nonce = Base64.getDecoder().decode(campos[2].trim());
        } catch (RuntimeException e) {
            throw new ChunkCrypto.CifraException("cabeçalho inválido em " + hdr.getFileName()
                    + ": " + e.getMessage(), e);
        }

        return new Sealed(chunkId, new ChunkCrypto.Metadados(keyVersion, salt, nonce),
                bin, hdr, Files.size(bin));
    }

    /**
     * Abre o chunk: verifica o id, decifra e reconstrói os registos por leitura
     * inversa (que ignora e valida o padding).
     */
    public List<Registo> registos(byte[] chaveMestra)
            throws IOException, ChunkCrypto.CifraException, Chunk.CorrupcaoException {
        byte[] cifrado = Files.readAllBytes(bin);
        byte[] claro = ChunkCrypto.abrir(chaveMestra, chunkId, meta, cifrado);
        return Chunk.lerInverso(claro);
    }

    // ------------------------------------------------------------------
    // Auxiliares
    // ------------------------------------------------------------------

    private static void escreverForcado(Path ficheiro, byte[] dados) throws IOException {
        try (FileChannel ch = FileChannel.open(ficheiro, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.wrap(dados);
            while (buf.hasRemaining()) ch.write(buf);
            ch.force(true);
        }
    }
}