package tpf;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Serialização do chunk, padding e leitura inversa (E2-02, objetivo 3). */
class ChunkTest {

    private static Registo r(long lsn, String texto) {
        return new Registo(lsn, Instant.ofEpochMilli(1_735_689_600_000L + lsn * 1000),
                "ana", "arquivo", texto);
    }

    private static Chunk tresRegistos() {
        Chunk c = Chunk.novo(r(1, "primeiro"));
        c.adicionar(r(2, "segundo; com ponto e vírgula"));
        c.adicionar(r(3, "terceiro\ncom mudança de linha"));
        return c;
    }

    @Test
    void serializacaoTerminaEmMultiploDePadding() {
        byte[] claro = tresRegistos().serializarComPadding();

        assertEquals(0, claro.length % Wire.PAD_BYTES,
                "o texto em claro tem de ser múltiplo de PAD_BYTES");
        assertTrue(claro.length >= Wire.PAD_BYTES);
        assertTrue(claro.length < Wire.PAD_BYTES * 2 + tresRegistos().bytes(),
                "não se acrescenta mais de um bloco de padding");
    }

    @Test
    void leituraDiretaEInversaReconstroemOsMesmosRegistos() throws Exception {
        Chunk original = tresRegistos();
        byte[] claro = original.serializarComPadding();

        List<Registo> direta = Chunk.ler(claro);
        List<Registo> inversa = Chunk.lerInverso(claro);

        assertEquals(original.registos(), direta);
        assertEquals(original.registos(), inversa, "a leitura inversa devolve a mesma ordem de LSN");
        assertEquals(1, inversa.get(0).lsn());
        assertEquals(3, inversa.get(2).lsn());
    }

    @Test
    void textoComPontoEVirgulaENewlineSobrevive() throws Exception {
        Chunk c = Chunk.novo(r(1, "a;b\nc\td"));
        List<Registo> lidos = Chunk.lerInverso(c.serializarComPadding());

        assertEquals("a;b\nc\td", lidos.get(0).texto());
    }

    @Test
    void paddingDiferenteDeZeroEhCorrupcao() {
        byte[] claro = tresRegistos().serializarComPadding();
        claro[claro.length - 1] = 0x01;          // um byte no padding

        assertThrows(Chunk.CorrupcaoException.class, () -> Chunk.ler(claro));
        assertThrows(Chunk.CorrupcaoException.class, () -> Chunk.lerInverso(claro));
    }

    @Test
    void assinaturaErradaEhCorrupcao() {
        byte[] claro = tresRegistos().serializarComPadding();
        claro[0] = 'X';

        assertThrows(Chunk.CorrupcaoException.class, () -> Chunk.ler(claro));
        assertThrows(Chunk.CorrupcaoException.class, () -> Chunk.lerInverso(claro));
    }

    @Test
    void tamanhoNaoMultiploDePaddingEhCorrupcao() {
        byte[] claro = tresRegistos().serializarComPadding();
        byte[] cortado = java.util.Arrays.copyOf(claro, claro.length - 1);

        assertThrows(Chunk.CorrupcaoException.class, () -> Chunk.ler(cortado));
    }

    @Test
    void tamanhoDeCorpoIncorretoEhApanhadoPelaLeituraInversa() {
        byte[] claro = tresRegistos().serializarComPadding();

        // Encontrar o último int32 de tamanho: fica imediatamente antes do padding.
        int lenConteudo = java.nio.ByteBuffer.wrap(claro, Chunk.MAGIC.length, 4).getInt();
        int posTamanho = Chunk.CABECALHO + lenConteudo - 4;
        java.nio.ByteBuffer.wrap(claro, posTamanho, 4).putInt(7);   // < MIN_CORPO

        assertThrows(Chunk.CorrupcaoException.class, () -> Chunk.lerInverso(claro));
    }

    @Test
    void instanteTemPrecisaoDeMilissegundos() throws Exception {
        Registo original = r(1, "x");
        List<Registo> lidos = Chunk.ler(Chunk.novo(original).serializarComPadding());

        assertEquals(original.instante(), lidos.get(0).instante());
    }
}