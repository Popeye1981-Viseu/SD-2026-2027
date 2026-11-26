package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Estado das réplicas, transições VIVA/SUSPEITA/MORTA e atomicidade de
 * reserveAndChoose (E2-03, objetivo 5 e critério "dois chunks ao mesmo tempo
 * não ultrapassam o espaço livre").
 */
class ReplicaRegistryTest {

    private static ReplicaRegistry registo(Path pasta, Eventos eventos, AtomicLong relogio,
                                           long margem) {
        return new ReplicaRegistry(eventos, new Placement(1.0, margem), relogio::get);
    }

    @Test
    void transicoesDeEstadoSeguemOsLimitesDeSuspeitaEMorta(@TempDir Path pasta) throws Exception {
        Eventos eventos = new Eventos(pasta);
        AtomicLong relogio = new AtomicLong(1_000);
        ReplicaRegistry reg = registo(pasta, eventos, relogio, 0);

        reg.autenticada("r1");
        reg.onHeartbeat(new Wire.Hb("r1", 0, 100_000), 1_000);
        assertEquals(Estado.VIVA, reg.estado("r1"));

        reg.atualizarEstados(1_000 + Wire.SUSPEITA_MS - 1);
        assertEquals(Estado.VIVA, reg.estado("r1"), "antes do limite continua VIVA");

        reg.atualizarEstados(1_000 + Wire.SUSPEITA_MS);
        assertEquals(Estado.SUSPEITA, reg.estado("r1"), "3 heartbeats em falta => SUSPEITA");

        reg.atualizarEstados(1_000 + Wire.MORTA_MS - 1);
        assertEquals(Estado.SUSPEITA, reg.estado("r1"));

        reg.atualizarEstados(1_000 + Wire.MORTA_MS);
        assertEquals(Estado.MORTA, reg.estado("r1"), "5 heartbeats em falta => MORTA");

        // Um heartbeat faz regressar a VIVA (REGRESSO).
        reg.onHeartbeat(new Wire.Hb("r1", 0, 100_000), 1_000 + Wire.MORTA_MS + 1);
        assertEquals(Estado.VIVA, reg.estado("r1"));

        String diario = Files.readString(pasta.resolve(LogRegistos.FICHEIRO_EVENTOS),
                StandardCharsets.UTF_8);
        assertTrue(diario.contains("r1 MORTA -> VIVA"), diario);
        assertTrue(diario.contains("r1 VIVA -> SUSPEITA"), diario);
        assertTrue(diario.contains("r1 SUSPEITA -> MORTA"), diario);
    }

    @Test
    void semHeartbeatNuncaEhCandidataEConhecidaFicaMorta(@TempDir Path pasta) throws Exception {
        Eventos eventos = new Eventos(pasta);
        AtomicLong relogio = new AtomicLong(1_000);
        ReplicaRegistry reg = registo(pasta, eventos, relogio, 0);
        reg.conhecida("r1");

        assertEquals(Estado.MORTA, reg.estado("r1"));
        assertTrue(reg.reserveAndChoose("c", 10, 3, Set.of()).isEmpty(),
                "uma réplica sem heartbeat não pode receber chunks");
    }

    @Test
    void reserveAndChooseEhAtomicaEntreDuasThreads(@TempDir Path pasta) throws Exception {
        Eventos eventos = new Eventos(pasta);
        AtomicLong relogio = new AtomicLong(1_000);
        // Uma única réplica com 100 bytes; dois chunks de 60 bytes só podem
        // caber um de cada vez (100 - 60 = 40 < 60).
        ReplicaRegistry reg = registo(pasta, eventos, relogio, 0);
        reg.autenticada("r1");
        reg.onHeartbeat(new Wire.Hb("r1", 0, 100), 1_000);

        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch partida = new CountDownLatch(1);
        List<Future<List<String>>> futuros = new ArrayList<>();
        for (String chunk : List.of("c1", "c2")) {
            futuros.add(pool.submit(() -> {
                partida.await();
                return reg.reserveAndChoose(chunk, 60, 1, Set.of());
            }));
        }
        partida.countDown();

        int escolhas = 0;
        for (Future<List<String>> f : futuros) {
            if (!f.get(5, TimeUnit.SECONDS).isEmpty()) escolhas++;
        }
        pool.shutdownNow();

        assertEquals(1, escolhas, "só um dos dois chunks pode reservar os 60 bytes");
        assertEquals(60L, reg.info("r1").reservados());
        assertEquals(40L, reg.livre("r1"));
        assertTrue(reg.reserveAndChoose("c3", 41, 1, Set.of()).isEmpty(),
                "41 bytes já não cabem nos 40 livres");
        assertFalse(reg.reserveAndChoose("c4", 40, 1, Set.of()).isEmpty(),
                "40 bytes cabem exatamente");
    }

    @Test
    void confirmarPassaReservaParaUsadosELibertarDevolveEspaco(@TempDir Path pasta) throws Exception {
        Eventos eventos = new Eventos(pasta);
        AtomicLong relogio = new AtomicLong(1_000);
        ReplicaRegistry reg = registo(pasta, eventos, relogio, 0);
        reg.autenticada("r1");
        reg.onHeartbeat(new Wire.Hb("r1", 10, 100), 1_000);

        assertEquals(List.of("r1"), reg.reserveAndChoose("c1", 40, 1, Set.of()));
        assertEquals(40, reg.info("r1").reservados());
        assertEquals(50, reg.livre("r1"));

        assertTrue(reg.confirmarReserva("r1", 40));
        assertEquals(0, reg.info("r1").reservados());
        assertEquals(50, reg.info("r1").usados(), "10 reportados + 40 confirmados");
        assertEquals(50, reg.livre("r1"));

        // Uma reserva que expira não passa a usados.
        assertEquals(List.of("r1"), reg.reserveAndChoose("c2", 20, 1, Set.of()));
        assertTrue(reg.libertar("r1", 20));
        assertEquals(0, reg.info("r1").reservados());
        assertEquals(50, reg.info("r1").usados());

        // O HB seguinte corrige os usados com o valor real reportado.
        reg.onHeartbeat(new Wire.Hb("r1", 45, 100), 1_001);
        assertEquals(45, reg.info("r1").usados());
    }

    @Test
    void mortaEhExcluidaMasSuspeitaAindaPodeSerEscolhida(@TempDir Path pasta) throws Exception {
        Eventos eventos = new Eventos(pasta);
        AtomicLong relogio = new AtomicLong(1_000);
        ReplicaRegistry reg = registo(pasta, eventos, relogio, 0);
        reg.autenticada("r1");
        reg.onHeartbeat(new Wire.Hb("r1", 0, 100), 1_000);

        reg.atualizarEstados(1_000 + Wire.SUSPEITA_MS);
        relogio.set(1_000 + Wire.SUSPEITA_MS);
        assertEquals(Estado.SUSPEITA, reg.estado("r1"));
        assertEquals(List.of("r1"), reg.reserveAndChoose("c1", 10, 1, Set.of()),
                "SUSPEITA continua a contar: pode voltar a qualquer momento");

        reg.atualizarEstados(1_000 + Wire.MORTA_MS);
        relogio.set(1_000 + Wire.MORTA_MS);
        assertEquals(Estado.MORTA, reg.estado("r1"));
        assertTrue(reg.reserveAndChoose("c2", 10, 1, Set.of()).isEmpty());
    }

    @Test
    void removidaDeixaDeSerConhecida(@TempDir Path pasta) throws Exception {
        Eventos eventos = new Eventos(pasta);
        AtomicLong relogio = new AtomicLong(1_000);
        ReplicaRegistry reg = registo(pasta, eventos, relogio, 0);
        reg.autenticada("r1");
        assertNotNull(reg.estado("r1"));
        assertTrue(reg.remover("r1"));
        assertNull(reg.estado("r1"));
        assertEquals(0, reg.total());
        assertFalse(reg.remover("r1"));
    }
}
