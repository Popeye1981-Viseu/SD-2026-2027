package tpf;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Colocação ponderada (E2-03, objetivo 6): determinismo, filtro de espaço e
 * distribuição da primeira escolha.
 *
 * A distribuição é medida tal como na experiência 5: 10 000 chunkIds, sem
 * reservas, com quotas livres de 8, 2, 4 e 6 MB. Com α = 1 espera-se ~40 %
 * para a réplica de 8 MB e ~10 % para a de 2 MB; com α = 0, ~25 % para todas
 * (o espaço livre deixa de influenciar a ordenação, mas continua a filtrar).
 */
class PlacementTest {

    private static final long MB = 1024L * 1024L;

    private static final List<Placement.Candidate> QUOTAS = List.of(
            new Placement.Candidate("A", 8 * MB),
            new Placement.Candidate("B", 2 * MB),
            new Placement.Candidate("C", 4 * MB),
            new Placement.Candidate("D", 6 * MB));

    private static String chunkId(int i) {
        return String.format("%064x", 0x1000_0000L + i);
    }

    @Test
    void aEscolhaEhDeterministicaEPreservaAsPrimeirasPosicoes() {
        Placement p = new Placement(1.0, Wire.MARGEM_BYTES);
        List<String> primeira = p.choose(chunkId(42), 1000, 3, QUOTAS, Set.of());
        assertEquals(3, primeira.size());
        assertEquals(3, primeira.stream().distinct().count());

        for (int i = 0; i < 20; i++) {
            assertEquals(primeira, p.choose(chunkId(42), 1000, 3, QUOTAS, Set.of()));
        }

        // A ordem de iteração da coleção não pode alterar o resultado.
        List<Placement.Candidate> outraOrdem = new ArrayList<>(QUOTAS);
        Collections.reverse(outraOrdem);
        assertEquals(primeira, p.choose(chunkId(42), 1000, 3, outraOrdem, Set.of()));
    }

    @Test
    void filtroDeEspacoEExclusaoSaoAplicados() {
        Placement p = new Placement(1.0, MB);                 // margem de 1 MB
        // A réplica B tem 2 MB: só cabe um chunk de 1 MB porque 1 MB + 1 MB = 2 MB.
        List<Placement.Candidate> cs = List.of(
                new Placement.Candidate("A", 8 * MB),
                new Placement.Candidate("B", 2 * MB));

        List<String> comUmMb = p.choose("c", MB - 1, 2, cs, Set.of());
        assertEquals(2, comUmMb.size(), "as duas têm livre >= tamanho + margem");

        List<String> comDoisMb = p.choose("c", 2 * MB, 2, cs, Set.of());
        assertEquals(List.of("A"), comDoisMb, "B não tem 2 MB + 1 MB de margem");

        List<String> excluindoA = p.choose("c", MB - 1, 2, cs, Set.of("A"));
        assertEquals(List.of("B"), excluindoA);
    }

    @Test
    void distribuicaoComAlfaUmAproximaSeDoEspacoLivre() {
        Map<String, Double> fracao = medir(1.0);
        assertEquals(0.40, fracao.get("A"), 0.02, "A (8 MB) devia ser ~40 %");
        assertEquals(0.10, fracao.get("B"), 0.015, "B (2 MB) devia ser ~10 %");
        assertEquals(0.20, fracao.get("C"), 0.02, "C (4 MB) devia ser ~20 %");
        assertEquals(0.30, fracao.get("D"), 0.02, "D (6 MB) devia ser ~30 %");
    }

    @Test
    void comAlfaZeroAEscolhaEhUniformeMasOEspacoAindaFiltra() {
        Map<String, Double> fracao = medir(0.0);
        for (String id : List.of("A", "B", "C", "D")) {
            assertEquals(0.25, fracao.get(id), 0.02, id + " devia ser ~25 % com α = 0");
        }

        // O α = 0 anula o peso, mas o filtro livre >= tamanho + margem continua.
        Placement p = new Placement(0.0, 0);
        List<String> semEspaco = p.choose("c", 9 * MB, 4, QUOTAS, Set.of());
        assertTrue(semEspaco.isEmpty(), "nenhuma réplica tem 9 MB livres");
    }

    @Test
    void comAlfaDoisAcentuaAsDiferencasDeEspaco() {
        Map<String, Double> fracao = medir(2.0);
        assertEquals(0.533, fracao.get("A"), 0.02, "A devia ser ~53 %");
        assertEquals(0.033, fracao.get("B"), 0.015, "B devia ser ~3 %");
        assertEquals(0.133, fracao.get("C"), 0.02, "C devia ser ~13 %");
        assertEquals(0.300, fracao.get("D"), 0.02, "D devia ser ~30 %");
    }

    private static Map<String, Double> medir(double alpha) {
        Placement p = new Placement(alpha, 0);
        Map<String, Integer> contagem = new HashMap<>();
        final int n = 10_000;
        for (int i = 0; i < n; i++) {
            List<String> escolhidas = p.choose(chunkId(i), 0, 3, QUOTAS, Set.of());
            assertEquals(3, escolhidas.size());
            contagem.merge(escolhidas.get(0), 1, Integer::sum);
        }
        Map<String, Double> fracao = new HashMap<>();
        for (Map.Entry<String, Integer> e : contagem.entrySet()) {
            fracao.put(e.getKey(), e.getValue() / (double) n);
        }
        return fracao;
    }

    @Test
    void pontuacaoUsaOsPrimeiros53BitsEmIntervaloAberto() {
        for (int i = 0; i < 1000; i++) {
            double u = Placement.unit("chunk-" + i, "r1");
            assertTrue(u > 0 && u < 1, "u tem de estar em ]0,1[: " + u);
        }
        assertEquals(Placement.unit("x", "y"), Placement.unit("x", "y"));
        assertNotEquals(Placement.unit("x", "y"), Placement.unit("x", "z"));
    }
}
