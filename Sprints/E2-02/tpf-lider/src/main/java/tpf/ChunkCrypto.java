package tpf;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.HexFormat;

/**
 * Cifra e identificação de chunks (E2-02, objetivo 4).
 *
 *   DEK   = HKDF-SHA256(MK, salt, info="tpf-chunk-v1")   (extrair + expandir 1 bloco)
 *   cifra = AES-256-GCM(texto com padding, nonce, tag 128 bits)
 *   aad   = "tpf-v1" || keyVersion (4 bytes) || salt || nonce
 *   chunkId = SHA-256 hexadecimal do texto cifrado, INCLUINDO a tag
 *
 * `abrir` verifica por esta ordem: (1) hash contra o chunkId, (2) keyVersion,
 * (3) autenticação GCM. Qualquer falha lança CifraException.
 */
public final class ChunkCrypto {

    public static final int KEY_VERSION = 1;
    public static final int SALT_BYTES = 32;
    public static final int NONCE_BYTES = 12;
    public static final int TAG_BITS = 128;          // 16 bytes
    public static final int TAG_BYTES = TAG_BITS / 8;

    private static final byte[] INFO = "tpf-chunk-v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] AAD_PREFIXO = "tpf-v1".getBytes(StandardCharsets.UTF_8);
    private static final String ALG_HKDF = "HmacSHA256";
    private static final String ALG_CIFRA = "AES/GCM/NoPadding";
    private static final SecureRandom RNG = new SecureRandom();

    private ChunkCrypto() {}

    /** Falha de cifra, de autenticação ou de identificação. */
    public static final class CifraException extends Exception {
        public CifraException(String mensagem) { super(mensagem); }
        public CifraException(String mensagem, Throwable causa) { super(mensagem, causa); }
    }

    /**
     * Metadados públicos de um chunk (vão para o `.hdr`).
     * `keyVersion` pode ser qualquer inteiro ≥ 1: aceitar versões desconhecidas aqui
     * permite que `abrir` as rejeite com uma mensagem clara em vez de falhar no parse.
     */
    public record Metadados(int keyVersion, byte[] salt, byte[] nonce) {
        public Metadados {
            if (keyVersion < 1) throw new IllegalArgumentException("keyVersion tem de ser >= 1");
            if (salt == null || salt.length != SALT_BYTES)
                throw new IllegalArgumentException("salt tem de ter " + SALT_BYTES + " bytes");
            if (nonce == null || nonce.length != NONCE_BYTES)
                throw new IllegalArgumentException("nonce tem de ter " + NONCE_BYTES + " bytes");
        }
    }

    /** Resultado de cifrar um chunk. */
    public record Cifrado(String chunkId, Metadados meta, byte[] textoCifrado) {}

    // ------------------------------------------------------------------
    // Cifrar / abrir
    // ------------------------------------------------------------------

    /** Cifra o texto (já com padding) com DEK derivada da chave mestra e metadados novos. */
    public static Cifrado cifrar(byte[] chaveMestra, byte[] textoComPadding) throws CifraException {
        if (chaveMestra == null || chaveMestra.length != 32) {
            throw new CifraException("a chave mestra tem de ter 32 bytes");
        }

        byte[] salt = new byte[SALT_BYTES];
        byte[] nonce = new byte[NONCE_BYTES];
        RNG.nextBytes(salt);
        RNG.nextBytes(nonce);

        try {
            byte[] dek = derivarChave(chaveMestra, salt);
            Cipher c = Cipher.getInstance(ALG_CIFRA);
            c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dek, "AES"),
                    new GCMParameterSpec(TAG_BITS, nonce));
            c.updateAAD(aad(KEY_VERSION, salt, nonce));
            byte[] cifrado = c.doFinal(textoComPadding);

            return new Cifrado(sha256Hex(cifrado), new Metadados(KEY_VERSION, salt, nonce), cifrado);

        } catch (GeneralSecurityException e) {
            throw new CifraException("falha ao cifrar o chunk: " + e.getMessage(), e);
        }
    }

    /**
     * Verifica e decifra um chunk. A ordem das verificações é a do enunciado:
     * hash contra o id, versão da chave, autenticação GCM.
     */
    public static byte[] abrir(byte[] chaveMestra, String chunkId, Metadados meta, byte[] textoCifrado)
            throws CifraException {
        if (chaveMestra == null || chaveMestra.length != 32) {
            throw new CifraException("a chave mestra tem de ter 32 bytes");
        }
        if (textoCifrado == null || textoCifrado.length < TAG_BYTES) {
            throw new CifraException("texto cifrado demasiado curto para conter a tag GCM");
        }

        // 1. O id é o SHA-256 do conteúdo cifrado (incluindo a tag): deteta alterações
        //    antes de qualquer operação criptográfica.
        String calculado = sha256Hex(textoCifrado);
        if (chunkId == null || !calculado.equalsIgnoreCase(chunkId)) {
            throw new CifraException("chunkId não corresponde ao SHA-256 do conteúdo (esperado "
                    + chunkId + ", calculado " + calculado + ")");
        }

        // 2. Versão da chave: só a v1 é conhecida.
        if (meta.keyVersion() != KEY_VERSION) {
            throw new CifraException("keyVersion " + meta.keyVersion() + " não suportada (apenas "
                    + KEY_VERSION + ")");
        }

        // 3. Autenticação e decifra.
        try {
            byte[] dek = derivarChave(chaveMestra, meta.salt());
            Cipher c = Cipher.getInstance(ALG_CIFRA);
            c.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek, "AES"),
                    new GCMParameterSpec(TAG_BITS, meta.nonce()));
            c.updateAAD(aad(meta.keyVersion(), meta.salt(), meta.nonce()));
            return c.doFinal(textoCifrado);

        } catch (GeneralSecurityException e) {
            throw new CifraException("autenticação do chunk falhou (AES-GCM): " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------
    // Primitivas
    // ------------------------------------------------------------------

    /**
     * HKDF-SHA256: extrair (HMAC(salt, MK) -> prk) e expandir um único bloco
     * (HMAC(prk, info || 0x01) -> 32 bytes).
     */
    static byte[] derivarChave(byte[] chaveMestra, byte[] salt) throws GeneralSecurityException {
        Mac mac = Mac.getInstance(ALG_HKDF);

        // HKDF-Extract: o salt é a chave do HMAC; a chave mestra é a mensagem.
        mac.init(new SecretKeySpec(salt, ALG_HKDF));
        byte[] prk = mac.doFinal(chaveMestra);

        // HKDF-Expand (L = 32 <= HashLen): um só bloco, com o contador 0x01.
        mac.init(new SecretKeySpec(prk, ALG_HKDF));
        mac.update(INFO);
        mac.update((byte) 0x01);
        return mac.doFinal();
    }

    /** aad = "tpf-v1" || keyVersion (4 bytes big-endian) || salt || nonce. */
    static byte[] aad(int keyVersion, byte[] salt, byte[] nonce) {
        return ByteBuffer.allocate(AAD_PREFIXO.length + 4 + salt.length + nonce.length)
                .put(AAD_PREFIXO)
                .putInt(keyVersion)
                .put(salt)
                .put(nonce)
                .array();
    }

    /** SHA-256 em hexadecimal (64 dígitos minúsculos). */
    public static String sha256Hex(byte[] dados) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(dados));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponível nesta JVM", e);
        }
    }
}