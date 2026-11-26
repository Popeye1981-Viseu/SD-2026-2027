package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Catálogo (E2-03, objetivo 7): formato, transição SELADO/DURAVEL, escrita
 * atómica e recarregamento no arranque.
 */
class CatalogTest {

    private static Sealed selado(String chunkId, long tamanho) {
        byte[] salt = new byte[32];
        byte[] nonce = new byte[12];
        for (int i = 0; i < salt.length; i++) salt[i] = (byte) i;
        for (int i = 0; i < nonce.length; i++) nonce[i] = (byte) (i + 1);
        return new Sealed(chunkId, new ChunkCrypto.Metadados(1, salt, nonce),
                Path.of(chunkId + ".bin"), Path.of(chunkId + ".hdr"), tamanho);
    }

    private List<Path> temporarios(Path pasta) throws IOException {
        try (Stream<Path> s = Files.list(pasta)) {
            return s.filter(p -> p.getFileName().toString().startsWith("." + Catalog.FICHEIRO))
                    .toList();
        }
    }

    @Test
    void adicionarConfirmarEGravarNoFormatoDoEnunciado(@TempDir Path pasta) throws Exception {
        Catalog c = Catalog.vazio(pasta, 3, 2);
        String id = "a".repeat(64);

        Catalog.EntradaCatalogo e = c.adicionar(selado(id, 8208), 1, 75);
        assertEquals(Catalog.Estado.SELADO, e.estado());
        assertTrue(e.replicas().isEmpty());
        assertEquals(8208, e.tamanho());
        assertEquals(1, c.total());
        assertEquals(1, c.subreplicados());
        assertTrue(Files.exists(pasta.resolve(Catalog.FICHEIRO)));

        assertTrue(c.confirmar(id, "r1"));
        assertEquals(Catalog.Estado.SELADO, c.estado(id), "1 ACK < quórum (2)");
        assertEquals(1, c.copias(id).size());

        assertTrue(c.confirmar(id, "r2"));
        assertEquals(Catalog.Estado.DURAVEL, c.estado(id), "2 ACK >= max(W_MIN, maioria)");

        String linha = Files.readString(pasta.resolve(Catalog.FICHEIRO), StandardCharsets.UTF_8);
        assertEquals(id + ";1;75;8208;r1,r2;DURAVEL\n", linha);
        assertEquals(0, temporarios(pasta).size(), "não pode ficar nenhum temporário");
    }

    @Test
    void quorumComR5ExigeTresCopias(@TempDir Path pasta) throws Exception {
        Catalog c = Catalog.vazio(pasta, 5, 2);
        String id = "b".repeat(64);
        c.adicionar(selado(id, 100), 2, 9);

        c.confirmar(id, "r1");
        c.confirmar(id, "r2");
        assertEquals(Catalog.Estado.SELADO, c.estado(id), "2 < max(2, 3) com R = 5");
        c.confirmar(id, "r3");
        assertEquals(Catalog.Estado.DURAVEL, c.estado(id));
    }

    @Test
    void catalogoSobreviveAoReinicioEATodasAsAlteracoes(@TempDir Path pasta) throws Exception {
        Catalog c = Catalog.vazio(pasta, 3, 2);
        String a = "a".repeat(64);
        String b = "b".repeat(64);
        c.adicionar(selado(a, 4100), 1, 2);
        c.adicionar(selado(b, 8200), 3, 10);
        c.confirmar(a, "r2");
        c.confirmar(b, "r1");
        c.confirmar(b, "r3");
        c.confirmar(b, "r2");

        Catalog lido = Catalog.carregar(pasta, 3, 2);
        assertEquals(2, lido.total());
        assertEquals(Catalog.Estado.SELADO, lido.estado(a));
        assertEquals(Catalog.Estado.DURAVEL, lido.estado(b));
        assertEquals(java.util.Set.of("r2"), lido.copias(a));
        assertEquals(java.util.Set.of("r1", "r2", "r3"), lido.copias(b));
        assertEquals(10, lido.entrada(b).lsnAte());
        assertEquals(2, lido.chunksPorReplica("r2"));
        assertEquals(1, lido.chunksPorReplica("r1"));
        assertEquals(1, lido.subreplicados(), "só o chunk A tem menos de R cópias");
    }

    @Test
    void adicionarDuasVezesOMesmoChunkNaoODuplica(@TempDir Path pasta) throws Exception {
        Catalog c = Catalog.vazio(pasta, 3, 2);
        String id = "c".repeat(64);
        c.adicionar(selado(id, 100), 1, 1);
        c.confirmar(id, "r1");
        c.adicionar(selado(id, 100), 1, 1);        // chunk imutável: não recomeça

        assertEquals(1, c.total());
        assertEquals(java.util.Set.of("r1"), c.copias(id));
    }

    @Test
    void ficheiroInvalidoAbortaOCarregamento(@TempDir Path pasta) throws Exception {
        Files.writeString(pasta.resolve(Catalog.FICHEIRO),
                "a;1;2;3;r1;SELADO\nlinha-invalida\n", StandardCharsets.UTF_8);
        assertThrows(IOException.class, () -> Catalog.carregar(pasta));

        Files.writeString(pasta.resolve(Catalog.FICHEIRO),
                "a;1;2;3;r1;ESTADO-QUE-NAO-EXISTE\n", StandardCharsets.UTF_8);
        assertThrows(IOException.class, () -> Catalog.carregar(pasta));

        Files.writeString(pasta.resolve(Catalog.FICHEIRO),
                "a;1;2;3;r1;SELADO\na;4;5;6;r2;SELADO\n", StandardCharsets.UTF_8);
        assertThrows(IOException.class, () -> Catalog.carregar(pasta));
    }

    @Test
    void ficheiroVazioOuAusenteDaCatalogoVazio(@TempDir Path pasta) throws Exception {
        assertEquals(0, Catalog.carregar(pasta).total());
        Files.writeString(pasta.resolve(Catalog.FICHEIRO), "", StandardCharsets.UTF_8);
        assertEquals(0, Catalog.carregar(pasta).total());
    }

    @Test
    void confirmarChunkDesconhecidoDevolveFalseSemAlterarOFicheiro(@TempDir Path pasta) throws Exception {
        Catalog c = Catalog.vazio(pasta, 3, 2);
        assertFalse(c.confirmar("desconhecido", "r1"));
        assertEquals(0, c.total());
    }
}
