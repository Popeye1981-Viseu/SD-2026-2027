package tpf;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/** Testes da cifra de chunks (E2-02, objetivo 5). */
class ChunkCryptoTest {

    private static byte[] chave(byte semente) {
        byte[] k = new byte[32];
        for (int i = 0; i < k.length; i++) k[i] = (byte) (semente + i);
        return k;
    }

    private static final byte[] MASTER = chave((byte) 1);

    private static byte[] claro() {
        return "conteudo-do-chunk-com-padding-suficiente".getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void cifraEDecifraRoundTrip() throws Exception {
        ChunkCrypto.Cifrado c = ChunkCrypto.cifrar(MASTER, claro());

        assertArrayEquals(claro(), ChunkCrypto.abrir(MASTER, c.chunkId(), c.meta(), c.textoCifrado()));

        assertEquals(64, c.chunkId().length(), "chunkId é SHA-256 hexadecimal");
        assertEquals(ChunkCrypto.sha256Hex(c.textoCifrado()), c.chunkId(),
                "chunkId tem de ser o SHA-256 do texto cifrado (com tag)");
        assertEquals(ChunkCrypto.SALT_BYTES, c.meta().salt().length);
        assertEquals(ChunkCrypto.NONCE_BYTES, c.meta().nonce().length);
        assertEquals(ChunkCrypto.KEY_VERSION, c.meta().keyVersion());
        assertEquals(claro().length + ChunkCrypto.TAG_BYTES, c.textoCifrado().length,
                "GCM acrescenta 16 bytes de tag");
    }

    @Test
    void doisChunksDoMesmoTextoUsamSaltENonceDiferentes() throws Exception {
        ChunkCrypto.Cifrado a = ChunkCrypto.cifrar(MASTER, claro());
        ChunkCrypto.Cifrado b = ChunkCrypto.cifrar(MASTER, claro());

        assertFalse(Arrays.equals(a.meta().salt(), b.meta().salt()), "o salt é aleatório por chunk");
        assertFalse(Arrays.equals(a.meta().nonce(), b.meta().nonce()), "o nonce é aleatório por chunk");
        assertNotEquals(a.chunkId(), b.chunkId(), "saídas diferentes => ids diferentes");
    }

    @Test
    void byteAlteradoNoTextoCifradoFalha() throws Exception {
        ChunkCrypto.Cifrado c = ChunkCrypto.cifrar(MASTER, claro());
        byte[] alterado = c.textoCifrado().clone();
        alterado[3] ^= 0x01;

        // Primeira verificação do `abrir`: o hash deixa de corresponder ao id.
        assertThrows(ChunkCrypto.CifraException.class,
                () -> ChunkCrypto.abrir(MASTER, c.chunkId(), c.meta(), alterado));
    }

    @Test
    void byteAlteradoComIdRecalculadoFalhaNaAutenticacaoGcm() throws Exception {
        // Mesmo que o atacante recalcule o id, o GCM tem de detetar a alteração.
        ChunkCrypto.Cifrado c = ChunkCrypto.cifrar(MASTER, claro());
        byte[] alterado = c.textoCifrado().clone();
        alterado[3] ^= 0x01;
        String novoId = ChunkCrypto.sha256Hex(alterado);

        assertThrows(ChunkCrypto.CifraException.class,
                () -> ChunkCrypto.abrir(MASTER, novoId, c.meta(), alterado));
    }

    @Test
    void saltDiferenteFalha() throws Exception {
        ChunkCrypto.Cifrado c = ChunkCrypto.cifrar(MASTER, claro());
        byte[] salt = c.meta().salt().clone();
        salt[0] ^= 0x01;

        var meta = new ChunkCrypto.Metadados(c.meta().keyVersion(), salt, c.meta().nonce());
        assertThrows(ChunkCrypto.CifraException.class,
                () -> ChunkCrypto.abrir(MASTER, c.chunkId(), meta, c.textoCifrado()));
    }

    @Test
    void nonceDiferenteFalha() throws Exception {
        ChunkCrypto.Cifrado c = ChunkCrypto.cifrar(MASTER, claro());
        byte[] nonce = c.meta().nonce().clone();
        nonce[11] ^= 0x80;

        var meta = new ChunkCrypto.Metadados(c.meta().keyVersion(), c.meta().salt(), nonce);
        assertThrows(ChunkCrypto.CifraException.class,
                () -> ChunkCrypto.abrir(MASTER, c.chunkId(), meta, c.textoCifrado()));
    }

    @Test
    void keyVersionDiferenteFalha() throws Exception {
        ChunkCrypto.Cifrado c = ChunkCrypto.cifrar(MASTER, claro());
        var meta = new ChunkCrypto.Metadados(2, c.meta().salt(), c.meta().nonce());

        ChunkCrypto.CifraException e = assertThrows(ChunkCrypto.CifraException.class,
                () -> ChunkCrypto.abrir(MASTER, c.chunkId(), meta, c.textoCifrado()));
        assertTrue(e.getMessage().contains("keyVersion"), e.getMessage());
    }

    @Test
    void chaveMestraDiferenteFalha() throws Exception {
        ChunkCrypto.Cifrado c = ChunkCrypto.cifrar(MASTER, claro());

        assertThrows(ChunkCrypto.CifraException.class,
                () -> ChunkCrypto.abrir(chave((byte) 99), c.chunkId(), c.meta(), c.textoCifrado()));
    }

    @Test
    void chunkIdDeOutroChunkFalha() throws Exception {
        ChunkCrypto.Cifrado a = ChunkCrypto.cifrar(MASTER, claro());
        ChunkCrypto.Cifrado b = ChunkCrypto.cifrar(MASTER, claro());

        assertThrows(ChunkCrypto.CifraException.class,
                () -> ChunkCrypto.abrir(MASTER, b.chunkId(), a.meta(), a.textoCifrado()));
    }

    @Test
    void hkdfSeparaExtrairEExpandir() throws Exception {
        byte[] salt = new byte[ChunkCrypto.SALT_BYTES];
        byte[] a = ChunkCrypto.derivarChave(MASTER, salt);

        byte[] outroSalt = salt.clone();
        outroSalt[0] = 1;
        byte[] b = ChunkCrypto.derivarChave(MASTER, outroSalt);

        assertEquals(32, a.length, "a DEK tem 32 bytes");
        assertFalse(Arrays.equals(a, b), "salt diferente => DEK diferente");
    }
}