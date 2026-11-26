package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.time.Instant;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Separação entre "registo durável" (fase 1, log) e "chunk derivado" (fase 2).
 *
 * A falha do chunk é forçada de forma determinística: em vez da pasta `selados/`
 * existe um FICHEIRO com esse nome, pelo que `Sealed.gravar` falha ao criar a pasta.
 */
class LeaderBotChunkTest {

    private static byte[] chave() {
        byte[] k = new byte[32];
        for (int i = 0; i < k.length; i++) k[i] = (byte) (i + 1);
        return k;
    }

    @Test
    void falhaDoChunkNaoInvalidaUmRegistoJaDuravel(@TempDir Path pasta) throws Exception {
        // `selados` é um FICHEIRO: a selagem falha de forma determinística.
        Files.write(pasta.resolve(Sealed.PASTA), new byte[]{1});

        Eventos eventos = new Eventos(pasta);
        LogRegistos log = LogRegistos.abrir(pasta, eventos);
        LeaderBot lider = LeaderBot.paraTestes(eventos, log, chave(), pasta);

        // Com o limite por omissão (64 KiB), um ou dois registos pequenos nunca chegam a
        // selar — a falha só apareceria no shutdown. Textos de ~30 KB fazem o TERCEIRO
        // registo ultrapassar CHUNK_MAX_BYTES e obrigam a selar dentro de `registar`.
        String grande = "x".repeat(30_000);

        try {
            assertEquals(1, lider.registar("ana", "arquivo", grande).lsn());
            assertEquals(2, lider.registar("ana", "arquivo", grande).lsn());

            // O 3.º registo provoca a selagem, a selagem falha, e a chamada NÃO lança:
            // o registo está durável, o LSN avança e a falha fica só como evento ERRO.
            assertEquals(3, lider.registar("ana", "arquivo", grande).lsn());

            assertEquals(3, LogRegistos.lerTodos(pasta).size(),
                    "nada pode ser perdido: os três registos estão no log");

            String diario = Files.readString(pasta.resolve(LogRegistos.FICHEIRO_EVENTOS),
                    StandardCharsets.UTF_8);
            assertTrue(diario.contains(";ERRO;"), "a falha do chunk tem de ficar registada");
            assertTrue(diario.contains("chunk abandonado"), diario);
            assertTrue(diario.contains("registo lsn=3"), diario);
        } finally {
            lider.fechar();
        }
    }

    @Test
    void falhaDoLogNaoAvancaOLsnNemOMandaParaOChunk(@TempDir Path pasta) throws Exception {
        Eventos eventos = new Eventos(pasta);
        LogRegistos log = LogRegistos.abrir(pasta, eventos);
        LeaderBot lider = LeaderBot.paraTestes(eventos, log, chave(), pasta);

        try {
            assertEquals(1, lider.registar("ana", "arquivo", "primeiro").lsn());

            log.close();                                        // fase 1 passa a falhar

            assertThrows(java.io.IOException.class,
                    () -> lider.registar("ana", "arquivo", "segundo"));

            assertEquals(1, log.ultimoLsn(), "o LSN não pode avançar com uma escrita falhada");
            assertEquals(1, LogRegistos.lerTodos(pasta).size(), "sem duplicados no log");
        } finally {
            lider.fechar();
        }
    }

    @Test
    void umSoRegistoProduzUmChunkSeladoPelaSelagemTemporal(@TempDir Path pasta) throws Exception {
        Eventos eventos = new Eventos(pasta);
        LogRegistos log = LogRegistos.abrir(pasta, eventos);
        LeaderBot lider = LeaderBot.paraTestes(eventos, log, chave(), pasta);

        try {
            // UM registo, sem nenhuma escrita adicional: a selagem só pode vir do
            // temporizador armado no primeiro registo do chunk.
            Registo r = lider.registar("ana", "arquivo", "unico registo");
            assertEquals(1, r.lsn());

            Path selados = pasta.resolve(Sealed.PASTA);
            Path bin = null;
            long limite = System.currentTimeMillis() + Wire.SELAGEM_MS + 5_000;  // folga p/ I/O

            while (System.currentTimeMillis() < limite) {
                if (Files.isDirectory(selados)) {
                    try (Stream<Path> ficheiros = Files.list(selados)) {
                        bin = ficheiros
                                .filter(p -> p.getFileName().toString().endsWith(Sealed.EXT_BIN))
                                .findFirst().orElse(null);
                    }
                    if (bin != null) break;
                }
                Thread.sleep(50);
            }

            assertNotNull(bin, "nenhum chunk selado apareceu em "
                    + (Wire.SELAGEM_MS + 5_000) + " ms (SELAGEM_MS=" + Wire.SELAGEM_MS + ")");

            // O `.bin` fica visível antes do evento SELADO (o evento é escrito a
            // seguir, com fsync). Esperar pelo evento evita uma corrida no teste.
            String diario = "";
            long fimEvento = System.currentTimeMillis() + 2_000;
            while (System.currentTimeMillis() < fimEvento) {
                diario = Files.readString(pasta.resolve(LogRegistos.FICHEIRO_EVENTOS),
                        StandardCharsets.UTF_8);
                if (diario.contains(";SELADO;")) break;
                Thread.sleep(20);
            }

            // O nome é o SHA-256 do conteúdo e o evento SELADO cobre exatamente o LSN 1.
            String chunkId = bin.getFileName().toString().replace(Sealed.EXT_BIN, "");
            assertEquals(ChunkCrypto.sha256Hex(Files.readAllBytes(bin)), chunkId);

            assertTrue(diario.contains(";SELADO;"), diario);
            assertTrue(diario.contains("chunkId=" + chunkId), diario);
            assertTrue(diario.contains("lsn=1-1"), diario);

            // E o chunk selado abre-se e devolve exatamente o registo que o originou.
            List<Registo> dentro = Sealed.ler(pasta, chunkId).registos(chave());
            assertEquals(1, dentro.size());
            Registo lido = dentro.get(0);
            assertEquals(r.lsn(), lido.lsn());
            assertEquals(r.autor(), lido.autor());
            assertEquals(r.canal(), lido.canal());
            assertEquals(r.texto(), lido.texto());
            assertEquals(Instant.ofEpochMilli(r.instante().toEpochMilli()), lido.instante(),
                    "o chunk guarda o instante com precisão de milissegundos");
        } finally {
            lider.fechar();
        }
    }

    @Test
    void rajadaProduzVariosChunksPorTamanhoTodosMultiplosDePad(@TempDir Path pasta) throws Exception {
        Eventos eventos = new Eventos(pasta);
        LogRegistos log = LogRegistos.abrir(pasta, eventos);

        final int n = 200;
        final long limite = 4096;                       // o mesmo valor que -Dtpf.chunkMax=4096
        LeaderBot lider = LeaderBot.paraTestes(eventos, log, chave(), pasta, limite);

        try {
            long[] extremos = lider.injetarRajada("ana", n, 0);   // o método que /rajada usa
            assertEquals(1, extremos[0]);
            assertEquals(n, extremos[1]);
        } finally {
            lider.fechar();                             // sela o chunk que ficou aberto
        }

        // 1) Vários chunks, cada um nomeado pelo SHA-256 do seu conteúdo.
        List<Path> bins;
        try (Stream<Path> ficheiros = Files.list(pasta.resolve(Sealed.PASTA))) {
            bins = ficheiros.filter(p -> p.getFileName().toString().endsWith(Sealed.EXT_BIN))
                    .sorted().toList();
        }
        assertTrue(bins.size() >= 2, "esperava vários chunks, encontrei " + bins.size());
        for (Path bin : bins) {
            assertEquals(ChunkCrypto.sha256Hex(Files.readAllBytes(bin)),
                    bin.getFileName().toString().replace(Sealed.EXT_BIN, ""));
        }

        // 2) Um SELADO por chunk, intervalos contíguos a cobrir 1..n, bytes= múltiplo de PAD.
        List<String> selados = Files.readAllLines(pasta.resolve(LogRegistos.FICHEIRO_EVENTOS),
                        StandardCharsets.UTF_8).stream()
                .filter(l -> l.contains(";SELADO;")).toList();

        assertEquals(bins.size(), selados.size(), "um SELADO por chunk");

        long esperadoInicio = 1;
        for (String linha : selados) {
            String[] lsn = extrair(linha, "lsn=[0-9]+-[0-9]+").substring(4).split("-");
            long de = Long.parseLong(lsn[0]);
            long ate = Long.parseLong(lsn[1]);

            assertEquals(esperadoInicio, de, "intervalos têm de ser contíguos: " + linha);
            assertTrue(ate >= de, "intervalo inválido: " + linha);
            esperadoInicio = ate + 1;

            long bytes = Long.parseLong(extrair(linha, "bytes=[0-9]+").substring("bytes=".length()));
            assertEquals(0, bytes % Wire.PAD_BYTES, "bytes=" + bytes + " não é múltiplo de PAD_BYTES");
        }
        assertEquals(n + 1L, esperadoInicio, "os intervalos têm de cobrir 1.." + n);

        // 3) O log tem os n registos, com LSN 1..n sem saltos.
        List<Registo> noLog = LogRegistos.lerTodos(pasta);
        assertEquals(n, noLog.size());
        for (int i = 0; i < n; i++) assertEquals(i + 1, noLog.get(i).lsn());

        // 4) Cada chunk abre EXATAMENTE os registos anunciados no seu SELADO, e esses
        //    registos são iguais aos do log nesse mesmo intervalo.
        for (String linha : selados) {
            String chunkId = extrair(linha, "chunkId=[0-9a-f]{64}").substring("chunkId=".length());

            String[] lsn = extrair(linha, "lsn=[0-9]+-[0-9]+").substring(4).split("-");
            int de = Integer.parseInt(lsn[0]);
            int ate = Integer.parseInt(lsn[1]);

            List<Registo> noChunk = Sealed.ler(pasta, chunkId).registos(chave());

            assertEquals(ate - de + 1, noChunk.size(),
                    "o chunk " + chunkId + " devia conter " + (ate - de + 1) + " registos");
            assertEquals(de, noChunk.get(0).lsn(), "primeiro LSN do chunk " + chunkId);
            assertEquals(ate, noChunk.get(noChunk.size() - 1).lsn(), "último LSN do chunk " + chunkId);

            List<Registo> noIntervalo = noLog.subList(de - 1, ate);   // LSN de -> índice de-1
            for (int i = 0; i < noChunk.size(); i++) {
                assertMesmosCampos(noIntervalo.get(i), noChunk.get(i),
                        "chunk " + chunkId + ", posição " + i);
            }
        }
    }

    /**
     * Igualdade nos campos que o critério exige (LSN, autor, canal, texto) mais o instante
     * com a precisão do formato do chunk (milissegundos). O log guarda nanossegundos, por
     * isso comparar o `Instant` cru falharia por precisão e não por conteúdo.
     */
    private static void assertMesmosCampos(Registo esperado, Registo obtido, String contexto) {
        assertEquals(esperado.lsn(), obtido.lsn(), contexto + ": lsn");
        assertEquals(esperado.autor(), obtido.autor(), contexto + ": autor");
        assertEquals(esperado.canal(), obtido.canal(), contexto + ": canal");
        assertEquals(esperado.texto(), obtido.texto(), contexto + ": texto");
        assertEquals(esperado.instante().toEpochMilli(), obtido.instante().toEpochMilli(),
                contexto + ": instante (ms)");
    }
    /** Devolve a primeira correspondência da regex na linha (falha o teste se não existir). */
    private static String extrair(String linha, String regex) {
        var matcher = java.util.regex.Pattern.compile(regex).matcher(linha);
        assertTrue(matcher.find(), "não encontrei " + regex + " em: " + linha);
        return matcher.group();
    }
}
