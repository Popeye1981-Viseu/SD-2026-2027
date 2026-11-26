package tpf;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Protocolo textual do E2-03: round-trip de todas as mensagens do sprint e
 * cálculo do quórum de durabilidade.
 */
class WireMensagensTest {

    @Test
    void olaDePrimeiroRegistoEDeLigacaoSeguinteFazemRoundTrip() throws Exception {
        Wire.Ola primeiro = new Wire.Ola("r1", "Y2hhdmVwdWJsaWNh", "dG9rZW4");
        String linha = primeiro.encode();
        assertEquals("OLA;r1;Y2hhdmVwdWJsaWNh;dG9rZW4", linha);
        Wire.Ola lido = Wire.Ola.decode(linha);
        assertEquals(primeiro, lido);
        assertTrue(lido.primeiroRegisto());

        Wire.Ola seguinte = Wire.Ola.decode("OLA;r1");
        assertEquals(new Wire.Ola("r1", null, null), seguinte);
        assertFalse(seguinte.primeiroRegisto());
        assertEquals("OLA;r1", seguinte.encode());

        // A chave sem token também é válida sintaticamente (o hub decide).
        assertEquals(new Wire.Ola("r1", "abc", null), Wire.Ola.decode("OLA;r1;abc"));
    }

    @Test
    void olaComTokenSemChaveEhRejeitado() {
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Ola.decode("OLA;r1;;token"));
    }

    @Test
    void desafioProvaSubEPubFazemRoundTrip() throws Exception {
        Wire.Desafio d = new Wire.Desafio("bm9uY2U=");
        assertEquals(d, Wire.Desafio.decode(d.encode()));

        Wire.Prova p = new Wire.Prova("r1", "c2ln");
        assertEquals("PROVA;r1;c2ln", p.encode());
        assertEquals(p, Wire.Prova.decode(p.encode()));

        Wire.Sub s = new Wire.Sub("tpf-r-r1");
        assertEquals(s, Wire.Sub.decode(s.encode()));

        // A mensagem interna de PUB pode conter ';' (HB e ACK têm 4 campos).
        Wire.Pub pub = new Wire.Pub("tpf-lider", "ACK;r1;abc;def");
        assertEquals("PUB;tpf-lider;ACK;r1;abc;def", pub.encode());
        assertEquals(pub, Wire.Pub.decode(pub.encode()));
    }

    @Test
    void hbAckEStoreFazemRoundTrip() throws Exception {
        Wire.Hb hb = new Wire.Hb("r1", 1234, 8_388_608);
        assertEquals("HB;r1;1234;8388608", hb.encode());
        assertEquals(hb, Wire.Hb.decode(hb.encode()));

        Wire.Ack ack = new Wire.Ack("r1", "a".repeat(64), "a".repeat(64));
        assertEquals(ack, Wire.Ack.decode(ack.encode()));

        Wire.Store store = new Wire.Store("b".repeat(64), "aGRy", "Ymlu");
        assertEquals("STORE;" + "b".repeat(64) + ";aGRy;Ymlu", store.encode());
        assertEquals(store, Wire.Store.decode(store.encode()));
    }

    @Test
    void mensagensComCamposEmFaltaOuTipoErradoSaoRejeitadas() {
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Hb.decode("HB;r1;1"));
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Ack.decode("ACK;r1;x"));
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Store.decode("STORE;x;y"));
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Sub.decode("PUB;topico"));
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Prova.decode("PROVA;r1"));
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Hb.decode("HB;r1;-1;2"));
        assertThrows(Wire.FormatoInvalido.class, () -> Wire.Hb.decode("HB;r1;abc;2"));
    }

    @Test
    void tipoDevolveOPrimeiroCampo() {
        assertEquals("HB", Wire.tipo("HB;r1;1;2"));
        assertEquals("OLA", Wire.tipo("OLA"));
        assertEquals("", Wire.tipo(""));
    }

    @Test
    void quorumDeDurabilidadeEhMaxDeWMinEMaioriaDeR() {
        assertEquals(2, Wire.quorumDurabilidade(3, 2));
        assertEquals(3, Wire.quorumDurabilidade(5, 2));
        assertEquals(3, Wire.quorumDurabilidade(5, 3));
        assertEquals(3, Wire.quorumDurabilidade(4, 2));
        assertEquals(2, Wire.quorumDurabilidade(2, 2));
        assertEquals(2, Wire.quorumDurabilidade(3, 1));
    }

    @Test
    void topicosSaoOsDoEnunciado() {
        assertEquals("tpf-lider", Wire.TOPICO_LIDER);
        assertEquals("tpf-ctl", Wire.TOPICO_CTL);
        assertEquals("tpf-r-abc", Wire.topicReplica("abc"));
    }
}
