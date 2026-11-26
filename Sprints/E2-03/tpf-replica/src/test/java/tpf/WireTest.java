package tpf;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** Protocolo textual da réplica (cópia mínima de Wire). */
class WireTest {

    @Test
    void olaDePrimeiroRegistoEDeReconexao() {
        assertEquals("OLA;A", Wire.ola("A", null, null));
        assertEquals("OLA;A;Y2hhdmU=;dG9rZW4=", Wire.ola("A", "Y2hhdmU=", "dG9rZW4="));
        assertEquals("OLA;A;Y2hhdmU=", Wire.ola("A", "Y2hhdmU=", null));
    }

    @Test
    void provaSubPubHbInventarioEAck() {
        assertEquals("PROVA;A;c2ln", Wire.prova("A", "c2ln"));
        assertEquals("SUB;tpf-r-A", Wire.sub("tpf-r-A"));
        assertEquals("PUB;tpf-lider;HB;A;0;100", Wire.pub("tpf-lider", Wire.hb("A", 0, 100)));
        assertEquals("HB;A;10;20", Wire.hb("A", 10, 20));
        assertEquals("INVENTARIO;A;a,b", Wire.inventario("A", List.of("a", "b")));
        assertEquals("INVENTARIO;A;", Wire.inventario("A", List.of()));
        assertEquals("ACK;A;abc;abc", Wire.ack("A", "abc", "abc"));
    }

    @Test
    void decodeDeDesafioPubEStore() throws Exception {
        assertEquals("nonce", Wire.Desafio.decode("DESAFIO;nonce").nonceB64());

        Wire.Pub pub = Wire.Pub.decode("PUB;tpf-r-A;STORE;abc;aGRy;Ymlu");
        assertEquals("tpf-r-A", pub.topico());
        assertEquals("STORE;abc;aGRy;Ymlu", pub.mensagem());

        Wire.Store store = Wire.Store.decode(pub.mensagem());
        assertEquals("abc", store.chunkId());
        assertEquals("aGRy", store.hdrB64());
        assertEquals("Ymlu", store.binB64());
    }

    @Test
    void mensagensInvalidasSaoRejeitadas() {
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Desafio.decode("DESAFIO"));
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Desafio.decode("OLA;x"));
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Pub.decode("PUB;topico"));
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Pub.decode("SUB;x;y"));
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Store.decode("STORE;a;b"));
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Store.decode("STORE;;b;c"));
    }

    @Test
    void tipoEConteudoAssinatura() {
        assertEquals("DESAFIO", Wire.tipo("DESAFIO;abc"));
        assertEquals("", Wire.tipo(null));
        assertEquals("tpf-registo|A|bm9uY2U=", Wire.conteudoAssinatura("A", "bm9uY2U="));
        assertEquals("tpf-r-A", Wire.topicReplica("A"));
    }

    @Test
    void semRepeticoesPreservaAOrdem() {
        assertEquals(List.of("a", "b"), List.copyOf(Wire.semRepeticoes(List.of("a", "b", "a"))));
    }
}
