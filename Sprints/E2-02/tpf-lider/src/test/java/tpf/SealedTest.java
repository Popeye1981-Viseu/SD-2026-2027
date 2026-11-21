package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Escrita e abertura de chunks selados (E2-02, objetivo 4). */
class SealedTest {

    private static byte[] master() {
        byte[] k = new byte[32];
        for (int i = 0; i < k.length; i++) k[i] = (byte) (i + 7);
        return k;
    }

    private static Chunk chunkCom(String texto) {
        Chunk c = Chunk.novo(new Registo(1, Instant.ofEpochMilli(1_735_689_600_000L),
                "ana", "arquivo", texto));
        c.adicionar(new Registo(2, Instant.ofEpochMilli(1_735_689_601_000L),
                "rui", "arquivo", "outro"));
        return c;
    }

    @Test
    void nomeDoBinEhSha256DoSeuConteudoESemTextoEmClaro(@TempDir Path pasta) throws Exception {
        byte[] chave = master();
        byte[] claro = chunkCom("segredo-unico-123").serializarComPadding();

        Sealed s = Sealed.gravar(pasta, ChunkCrypto.cifrar(chave, claro));

        byte[] bin = Files.readAllBytes(s.bin());
        assertEquals(ChunkCrypto.sha256Hex(bin), s.chunkId(),
                "o chunkId é o SHA-256 do conteúdo do .bin");
        assertEquals(s.chunkId() + Sealed.EXT_BIN, s.bin().getFileName().toString());
        assertEquals(s.chunkId() + Sealed.EXT_HDR, s.hdr().getFileName().toString());

        assertFalse(contem(bin, "segredo-unico-123".getBytes(StandardCharsets.UTF_8)),
                "o .bin não pode conter o texto em claro");
        assertFalse(contem(Files.readAllBytes(s.hdr()), "segredo".getBytes(StandardCharsets.UTF_8)),
                "o .hdr só tem keyVersion;salt;nonce");

        assertEquals(claro.length + ChunkCrypto.TAG_BYTES, bin.length,
                ".bin = texto em claro (múltiplo de PAD_BYTES) + tag GCM");
        assertEquals(0, claro.length % Wire.PAD_BYTES);
    }

    @Test
    void hdrTemKeyVersionSaltENonce(@TempDir Path pasta) throws Exception {
        Sealed s = Sealed.gravar(pasta, ChunkCrypto.cifrar(master(), chunkCom("x").serializarComPadding()));

        String hdr = Files.readString(s.hdr(), StandardCharsets.UTF_8).trim();
        String[] campos = hdr.split(";", -1);

        assertEquals(3, campos.length, "keyVersion;salt;nonce");
        assertEquals("1", campos[0]);
        assertEquals(32, java.util.Base64.getDecoder().decode(campos[1]).length);
        assertEquals(12, java.util.Base64.getDecoder().decode(campos[2]).length);
    }

    @Test
    void lerEAbrirReconstroiOsRegistos(@TempDir Path pasta) throws Exception {
        byte[] chave = master();
        Chunk original = chunkCom("primeiro texto");

        Sealed gravado = Sealed.gravar(pasta, ChunkCrypto.cifrar(chave, original.serializarComPadding()));
        Sealed lido = Sealed.ler(pasta, gravado.chunkId());

        List<Registo> registos = lido.registos(chave);
        assertEquals(original.registos(), registos);
    }

    @Test
    void byteAlteradoNoBinFazAbrirFalhar(@TempDir Path pasta) throws Exception {
        byte[] chave = master();
        Sealed s = Sealed.gravar(pasta, ChunkCrypto.cifrar(chave, chunkCom("x").serializarComPadding()));

        byte[] bin = Files.readAllBytes(s.bin());
        bin[bin.length / 2] ^= 0x01;
        Files.write(s.bin(), bin);

        assertThrows(ChunkCrypto.CifraException.class, () -> Sealed.ler(pasta, s.chunkId()).registos(chave));
    }

    @Test
    void outraChaveMestraFazAbrirFalhar(@TempDir Path pasta) throws Exception {
        Sealed s = Sealed.gravar(pasta, ChunkCrypto.cifrar(master(), chunkCom("x").serializarComPadding()));

        assertThrows(ChunkCrypto.CifraException.class, () -> Sealed.ler(pasta, s.chunkId()).registos(new byte[32]));
    }

    private static boolean contem(byte[] haystack, byte[] agulha) {
        if (agulha.length == 0 || haystack.length < agulha.length) return false;
        for (int i = 0; i <= haystack.length - agulha.length; i++) {
            boolean igual = true;
            for (int j = 0; j < agulha.length; j++) {
                if (haystack[i + j] != agulha[j]) { igual = false; break; }
            }
            if (igual) return true;
        }
        return false;
    }
}
