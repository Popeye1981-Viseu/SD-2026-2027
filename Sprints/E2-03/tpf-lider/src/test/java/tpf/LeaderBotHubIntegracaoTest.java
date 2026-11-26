package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integração líder + hub + réplicas **sem rede**: as ligações usam o canal de
 * teste do hub, mas percorrem o caminho completo OLA/PROVA/SUB → STORE →
 * ACK → catálogo. É o que garante que as peças do E2-03 se ligam umas às
 * outras, e não apenas que cada uma passa isolada.
 */
class LeaderBotHubIntegracaoTest {

    private static final class CanalFalso implements PubSubHub.CanalLigacao {
        final List<String> enviados = new ArrayList<>();
        boolean fechada;

        @Override public void enviar(String mensagem) { enviados.add(mensagem); }
        @Override public void fechar(int codigo, String motivo) { fechada = true; }

        String ultimoDesafio() {
            for (int i = enviados.size() - 1; i >= 0; i--) {
                if (enviados.get(i).startsWith("DESAFIO;")) return enviados.get(i);
            }
            return null;
        }

        Wire.Store ultimoStore() throws Wire.FormatoInvalido {
            for (int i = enviados.size() - 1; i >= 0; i--) {
                Wire.Pub pub = Wire.Pub.decode(enviados.get(i));
                if ("STORE".equals(Wire.tipo(pub.mensagem()))) {
                    return Wire.Store.decode(pub.mensagem());
                }
            }
            return null;
        }
    }

    private static byte[] chave() {
        byte[] k = new byte[32];
        for (int i = 0; i < k.length; i++) k[i] = (byte) (i + 1);
        return k;
    }

    private static KeyPair par() throws Exception {
        return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    }

    private static String publicaB64(KeyPair par) {
        return Base64.getEncoder().encodeToString(par.getPublic().getEncoded());
    }

    private static String assinar(KeyPair par, String id, String nonce) throws Exception {
        Signature s = Signature.getInstance("Ed25519");
        s.initSign(par.getPrivate());
        s.update(("tpf-registo|" + id + "|" + nonce).getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(s.sign());
    }

    @Test
    void escritaCompletaAtravessaOHubEPassaADuravel(@TempDir Path pasta) throws Exception {
        Eventos eventos = new Eventos(pasta);
        LogRegistos log = LogRegistos.abrir(pasta, eventos);
        Catalog catalogo = Catalog.vazio(pasta, 3, 2);
        AtomicLong relogio = new AtomicLong(1_000);
        ReplicaRegistry registry = new ReplicaRegistry(eventos,
                new Placement(1.0, 0), relogio::get);

        LeaderBot lider = LeaderBot.paraTestesComReplicas(eventos, log, chave(), pasta, 4096,
                catalogo, registry, null, 10_000, 3, 3);

        PubSubHub hub = new PubSubHub(0, eventos, registry, pasta, lider::receberDoHub,
                relogio::get);
        lider.ligarHub(hub);

        Map<String, CanalFalso> canais = new LinkedHashMap<>();
        Map<String, PubSubHub.Ligacao> ligacoes = new LinkedHashMap<>();
        for (String id : List.of("a", "b", "c")) {
            KeyPair par = par();
            CanalFalso canal = new CanalFalso();
            PubSubHub.Ligacao l = hub.novaLigacao(canal);
            hub.processar(l, new Wire.Ola(id, publicaB64(par), hub.gerarToken()).encode());
            String nonce = canal.ultimoDesafio().substring("DESAFIO;".length());
            hub.processar(l, new Wire.Prova(id, assinar(par, id, nonce)).encode());
            assertEquals(PubSubHub.Fase.AUTENTICADA, l.fase(), "réplica " + id);
            hub.processar(l, new Wire.Sub(Wire.topicReplica(id)).encode());
            hub.processar(l, new Wire.Sub(Wire.TOPICO_CTL).encode());
            // O HB chega pelo mesmo caminho das réplicas reais.
            hub.processar(l, "PUB;" + Wire.TOPICO_LIDER + ";"
                    + new Wire.Hb(id, 0, 10_000_000).encode());
            assertEquals(10_000_000, registry.info(id).quota());
            canais.put(id, canal);
            ligacoes.put(id, l);
        }
        for (CanalFalso c : canais.values()) c.enviados.clear();

        // Selagem + colocação: o líder publica STORE pelo hub.
        lider.registar("ana", "arquivo", "x".repeat(5_000));

        Wire.Store store = null;
        for (CanalFalso c : canais.values()) {
            Wire.Store s = c.ultimoStore();
            if (s != null) {
                store = s;
                break;
            }
        }
        assertNotNull(store, "nenhuma réplica recebeu STORE pelo hub");
        String chunkId = store.chunkId();
        assertTrue(catalogo.copias(chunkId).isEmpty(), "nenhum ACK ainda");
        assertEquals(Catalog.Estado.SELADO, catalogo.estado(chunkId));

        // Dois ACK válidos, entregues pelo hub com o id autenticado da ligação.
        for (Map.Entry<String, PubSubHub.Ligacao> e : ligacoes.entrySet()) {
            if (canais.get(e.getKey()).ultimoStore() == null) continue;
            String ack = new Wire.Ack(e.getKey(), chunkId, chunkId).encode();
            hub.processar(e.getValue(), "PUB;" + Wire.TOPICO_LIDER + ";" + ack);
            if (catalogo.copias(chunkId).size() == 2) break;
        }

        assertEquals(Catalog.Estado.DURAVEL, catalogo.estado(chunkId));
        assertEquals(2, catalogo.copias(chunkId).size());

        lider.fechar();
    }
}
