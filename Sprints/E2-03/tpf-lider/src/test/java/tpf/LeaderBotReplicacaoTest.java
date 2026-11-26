package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Caminho de escrita replicada (E2-03, objetivo 7): STORE, validação de ACK,
 * quórum de durabilidade, rejeições e nova ronda após ACK_TIMEOUT_MS.
 *
 * Não usa Discord nem rede: o publicador é injetado e as mensagens que o líder
 * enviaria ao hub são capturadas.
 */
class LeaderBotReplicacaoTest {

    private static final long MB = 1024L * 1024L;

    private static byte[] chave() {
        byte[] k = new byte[32];
        for (int i = 0; i < k.length; i++) k[i] = (byte) (i + 1);
        return k;
    }

    private static final class Ambiente {
        LeaderBot lider;
        Catalog catalogo;
        ReplicaRegistry registry;
        List<String[]> publicados = new CopyOnWriteArrayList<>();
        Path pasta;
    }

    private static Ambiente ambiente(Path pasta, int ackTimeoutMs, int maxRondas,
                                     List<String> replicas) throws Exception {
        Eventos eventos = new Eventos(pasta);
        LogRegistos log = LogRegistos.abrir(pasta, eventos);
        Catalog catalogo = Catalog.vazio(pasta, 3, 2);
        AtomicLong relogio = new AtomicLong(1_000);
        ReplicaRegistry registry = new ReplicaRegistry(eventos,
                new Placement(1.0, 0), relogio::get);

        for (String id : replicas) {
            registry.autenticada(id);
            registry.onHeartbeat(new Wire.Hb(id, 0, 10 * MB), 1_000);
        }

        Ambiente a = new Ambiente();
        a.pasta = pasta;
        a.catalogo = catalogo;
        a.registry = registry;
        a.lider = LeaderBot.paraTestesComReplicas(eventos, log, chave(), pasta, 4096,
                catalogo, registry,
                (topico, mensagem) -> a.publicados.add(new String[]{topico, mensagem}),
                ackTimeoutMs, maxRondas, 3);
        return a;
    }

    private static List<String> idsDosStores(List<String[]> publicados, String chunkId) throws Exception {
        List<String> ids = new ArrayList<>();
        for (String[] p : publicados) {
            if (!p[1].startsWith("STORE;")) continue;
            Wire.Store s = Wire.Store.decode(p[1]);
            if (!s.chunkId().equals(chunkId)) continue;
            assertTrue(p[0].startsWith("tpf-r-"), "STORE dirigido ao tópico privado: " + p[0]);
            ids.add(p[0].substring("tpf-r-".length()));
        }
        return ids;
    }

    private static String chunkIdDeUmStore(List<String[]> publicados) throws Exception {
        for (String[] p : publicados) {
            if (p[1].startsWith("STORE;")) return Wire.Store.decode(p[1]).chunkId();
        }
        fail("nenhum STORE publicado");
        return null;
    }

    private static void esperar(String descricao, BooleanSupplier condicao) throws Exception {
        long limite = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < limite) {
            if (condicao.getAsBoolean()) return;
            Thread.sleep(20);
        }
        fail("timeout à espera de: " + descricao);
    }

    @Test
    void ackValidoConfirmaCopiaEAckInvalidoEhRejeitado(@TempDir Path pasta) throws Exception {
        Ambiente a = ambiente(pasta, 10_000, 3, List.of("a", "b", "c", "d"));

        // "d" tem quota 0: nunca é escolhida, mas existe no registo.
        a.registry.onHeartbeat(new Wire.Hb("d", 0, 0), 1_000);

        // Um registo grande provoca a selagem e a colocação imediata.
        a.lider.registar("ana", "arquivo", "x".repeat(5_000));
        String chunkId = chunkIdDeUmStore(a.publicados);
        List<String> escolhidas = idsDosStores(a.publicados, chunkId);

        assertEquals(3, escolhidas.size(), "R = 3 réplicas distintas");
        assertEquals(3, new HashSet<>(escolhidas).size());
        assertFalse(escolhidas.contains("d"), "a réplica sem espaço não pode ser escolhida");
        assertTrue(a.catalogo.copias(chunkId).isEmpty());
        assertEquals(Catalog.Estado.SELADO, a.catalogo.estado(chunkId));

        // 1) hash errado
        a.lider.receberDoHub(escolhidas.get(0),
                new Wire.Ack(escolhidas.get(0), chunkId, "0".repeat(64)).encode());
        assertTrue(a.catalogo.copias(chunkId).isEmpty());

        // 2) id da mensagem diferente do id autenticado da ligação
        a.lider.receberDoHub(escolhidas.get(0),
                new Wire.Ack(escolhidas.get(1), chunkId, chunkId).encode());
        assertTrue(a.catalogo.copias(chunkId).isEmpty());

        // 3) réplica que não recebeu o STORE deste chunk
        a.lider.receberDoHub("d", new Wire.Ack("d", chunkId, chunkId).encode());
        assertTrue(a.catalogo.copias(chunkId).isEmpty());

        String diario = Files.readString(pasta.resolve(LogRegistos.FICHEIRO_EVENTOS),
                StandardCharsets.UTF_8);
        assertEquals(3, diario.lines().filter(l -> l.contains(";ACK-REJEITADO;")).count(), diario);

        // ACK válido de duas réplicas: DURAVEL (R = 3 => max(2, 2) = 2).
        a.lider.receberDoHub(escolhidas.get(0),
                new Wire.Ack(escolhidas.get(0), chunkId, chunkId).encode());
        assertEquals(Catalog.Estado.SELADO, a.catalogo.estado(chunkId), "1 ACK ainda não chega");
        a.lider.receberDoHub(escolhidas.get(1),
                new Wire.Ack(escolhidas.get(1), chunkId, chunkId).encode());

        assertEquals(Catalog.Estado.DURAVEL, a.catalogo.estado(chunkId));
        assertEquals(Set.of(escolhidas.get(0), escolhidas.get(1)), a.catalogo.copias(chunkId));
        assertTrue(diarioAtualizado(pasta).contains(";DURAVEL;"));
        assertTrue(diarioAtualizado(pasta).contains(";STORE;"));
        assertTrue(diarioAtualizado(pasta).contains(";RONDA;"));

        // Um ACK duplicado não transforma a mesma cópia em duas.
        a.lider.receberDoHub(escolhidas.get(0),
                new Wire.Ack(escolhidas.get(0), chunkId, chunkId).encode());
        assertEquals(2, a.catalogo.copias(chunkId).size());

        a.lider.fechar();
    }

    @Test
    void aposTimeoutNovaRondaColocaATerceiraCopiaNaQuartaReplica(@TempDir Path pasta) throws Exception {
        Ambiente a = ambiente(pasta, 150, 3, List.of("a", "b", "c", "d"));

        a.lider.registar("ana", "arquivo", "x".repeat(5_000));
        String chunkId = chunkIdDeUmStore(a.publicados);
        List<String> escolhidas = idsDosStores(a.publicados, chunkId);
        assertEquals(3, escolhidas.size());

        Set<String> todas = Set.of("a", "b", "c", "d");
        String quarta = todas.stream().filter(id -> !escolhidas.contains(id)).findFirst().orElseThrow();

        // Uma réplica devolve hash errado; as outras duas confirmam.
        a.lider.receberDoHub(escolhidas.get(0),
                new Wire.Ack(escolhidas.get(0), chunkId, "f".repeat(64)).encode());
        a.lider.receberDoHub(escolhidas.get(1),
                new Wire.Ack(escolhidas.get(1), chunkId, chunkId).encode());
        a.lider.receberDoHub(escolhidas.get(2),
                new Wire.Ack(escolhidas.get(2), chunkId, chunkId).encode());
        assertEquals(Catalog.Estado.DURAVEL, a.catalogo.estado(chunkId));
        assertEquals(2, a.catalogo.copias(chunkId).size());

        // Ao fim de ACK_TIMEOUT_MS, uma nova ronda exclui as três tentadas e
        // escolhe a quarta réplica, mesmo o chunk já sendo DURAVEL.
        esperar("STORE da segunda ronda para " + quarta,
                () -> a.publicados.stream().anyMatch(p ->
                        p[0].equals("tpf-r-" + quarta) && p[1].startsWith("STORE;")));

        List<String> ids = idsDosStores(a.publicados, chunkId);
        assertEquals(4, ids.size(), "a quarta réplica recebeu STORE na segunda ronda");
        assertEquals(quarta, ids.get(3));

        a.lider.receberDoHub(quarta, new Wire.Ack(quarta, chunkId, chunkId).encode());
        esperar("três cópias confirmadas",
                () -> a.catalogo.copias(chunkId).size() == 3);
        assertEquals(Set.of(escolhidas.get(1), escolhidas.get(2), quarta),
                a.catalogo.copias(chunkId));

        String diario = diarioAtualizado(pasta);
        assertTrue(diario.contains("RONDA;" + chunkId + " 2"), diario);

        a.lider.fechar();
    }

    @Test
    void comMenosDeRTodasFicamEmTodasESubreplicadoEhRegistado(@TempDir Path pasta) throws Exception {
        Ambiente a = ambiente(pasta, 10_000, 3, List.of("a", "b"));

        a.lider.registar("ana", "arquivo", "x".repeat(5_000));
        String chunkId = chunkIdDeUmStore(a.publicados);

        assertEquals(Set.of("a", "b"), new HashSet<>(idsDosStores(a.publicados, chunkId)));
        assertEquals(1, a.catalogo.subreplicados());

        String diario = diarioAtualizado(pasta);
        assertTrue(diario.contains("SUBREPLICADO;" + chunkId + " 2/3"), diario);

        // Com dois ACK válidos, mesmo subreplicado, o chunk fica DURAVEL.
        a.lider.receberDoHub("a", new Wire.Ack("a", chunkId, chunkId).encode());
        a.lider.receberDoHub("b", new Wire.Ack("b", chunkId, chunkId).encode());
        assertEquals(Catalog.Estado.DURAVEL, a.catalogo.estado(chunkId));
        assertEquals(1, a.catalogo.subreplicados(), "continua subreplicado (2 < 3)");

        a.lider.fechar();
    }

    @Test
    void hbDeOutroIdEhIgnorado(@TempDir Path pasta) throws Exception {
        Ambiente a = ambiente(pasta, 10_000, 3, List.of("a"));
        a.lider.receberDoHub("a", new Wire.Hb("b", 0, 1_000_000).encode());
        assertNull(a.registry.info("b"));
        assertTrue(diarioAtualizado(pasta).contains("HB de a com id de outra réplica"));
        a.lider.fechar();
    }

    private static String diarioAtualizado(Path pasta) throws Exception {
        return Files.readString(pasta.resolve(LogRegistos.FICHEIRO_EVENTOS), StandardCharsets.UTF_8);
    }
}
