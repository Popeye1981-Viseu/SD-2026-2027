package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class LogRegistosTest {

    private static final Instant T0 = Instant.parse("2025-01-01T00:00:00Z");

    // ------------------------------------------------------------------
    // Auxiliares
    // ------------------------------------------------------------------

    /** Constrói uma linha igual à que o escritor produz (com '\n'). */
    private static byte[] linha(long lsn, String autor, String canal, String texto) {
        return LogRegistos.formatar(lsn, T0, autor, canal, texto);
    }

    private static Path ficheiro(Path pasta) {
        return pasta.resolve(LogRegistos.FICHEIRO);
    }

    private static Path ficheiroEventos(Path pasta) {
        return pasta.resolve(LogRegistos.FICHEIRO_EVENTOS);
    }

    private static Path ficheiroIntencao(Path pasta) {
        return pasta.resolve(LogRegistos.FICHEIRO_INTENCAO);
    }

    private static byte[] unir(byte[]... partes) {
        int total = 0;
        for (byte[] p : partes) total += p.length;
        byte[] r = new byte[total];
        int pos = 0;
        for (byte[] p : partes) {
            System.arraycopy(p, 0, r, pos, p.length);
            pos += p.length;
        }
        return r;
    }

    /** Primeiros `n` bytes de um array (simula uma escrita cortada a meio). */
    private static byte[] cortado(byte[] dados, int n) {
        return Arrays.copyOf(dados, n);
    }

    /** SHA-256 hex dos primeiros `n` bytes de `dados` (atalho para o helper do produtor). */
    private static String hash(byte[] dados, int n) {
        return LogRegistos.sha256Hex(dados, n);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Quantas linhas de `eventos-lider.txt` têm este tipo de evento (campo 2). */
    private static long contarEventos(Path pasta, String evento) throws IOException {
        Path f = ficheiroEventos(pasta);
        if (!Files.exists(f)) return 0;
        long n = 0;
        for (String linha : Files.readAllLines(f, StandardCharsets.UTF_8)) {
            String[] campos = linha.split(";", 3);
            if (campos.length >= 2 && campos[1].equals(evento)) n++;
        }
        return n;
    }

    /** Conteúdo de `eventos-lider.txt` como texto (vazio se não existir). */
    private static String lerEventos(Path pasta) throws IOException {
        Path f = ficheiroEventos(pasta);
        return Files.exists(f) ? Files.readString(f, StandardCharsets.UTF_8) : "";
    }

    /** Acrescenta uma linha de evento válida ao diário. */
    private static void escreverEvento(Path pasta, String evento, String detalhe) throws IOException {
        Path f = ficheiroEventos(pasta);
        String anterior = Files.exists(f) ? Files.readString(f, StandardCharsets.UTF_8) : "";
        Files.writeString(f, anterior + T0 + ";" + evento + ";" + detalhe + "\n", StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------
    // Canal de teste: simula falhas de I/O num "ficheiro" em memória
    // ------------------------------------------------------------------

    /**
     * Canal em memória que simula escrita parcial, `force` falhado e truncagem
     * falhada. É a única forma de exercitar o fail-stop de forma determinística.
     *
     * `falharForcarEm` indica QUAIS as chamadas a `forcar()` falham (1 = a
     * primeira). É o que permite distinguir dois cenários diferentes:
     *  - rollback com truncagem E force bem sucedidos  -> o log continua utilizável;
     *  - rollback com truncagem ok mas force falhado   -> o log entra em fail-stop.
     */
    private static final class CanalFalso implements LogRegistos.Canal {

        private byte[] conteudo;
        private final boolean falharEscrita;
        private final Set<Integer> falharForcarEm;   // chamadas 1-indexed a forcar()
        private final boolean falharTruncagem;

        int escritas;
        int forcares;
        int truncagens;

        CanalFalso(byte[] inicial) {
            this(inicial, false, Set.of(), false);
        }

        CanalFalso(byte[] inicial, boolean falharEscrita, Set<Integer> falharForcarEm, boolean falharTruncagem) {
            this.conteudo = inicial.clone();
            this.falharEscrita = falharEscrita;
            this.falharForcarEm = falharForcarEm;
            this.falharTruncagem = falharTruncagem;
        }

        /** Vista do conteúdo atual do "disco" (cópia, para comparações seguras). */
        byte[] conteudo() {
            return conteudo.clone();
        }

        @Override public void escrever(ByteBuffer buf) throws IOException {
            escritas++;
            if (!falharEscrita) {
                byte[] b = new byte[buf.remaining()];
                buf.get(b);
                conteudo = unir(conteudo, b);
                return;
            }
            // Escrita PARCIAL: metade da linha chega ao "disco" e o resto falha.
            byte[] metade = new byte[Math.max(1, buf.remaining() / 2)];
            buf.get(metade);
            conteudo = unir(conteudo, metade);
            throw new IOException("falha simulada de escrita");
        }

        @Override public void forcar() throws IOException {
            forcares++;
            if (falharForcarEm.contains(forcares)) {
                throw new IOException("falha simulada de force (chamada " + forcares + ")");
            }
        }

        @Override public void truncar(long tamanho) throws IOException {
            truncagens++;
            if (falharTruncagem) throw new IOException("falha simulada de truncate");
            if (tamanho > conteudo.length) throw new IOException("truncate não pode alargar");
            conteudo = Arrays.copyOf(conteudo, (int) tamanho);
        }

        @Override public long tamanho() {
            return conteudo.length;
        }

        @Override public void close() {
            // nada a fechar
        }
    }

    // ------------------------------------------------------------------
    // FAIL-STOP: escrita/force falhados
    // ------------------------------------------------------------------

    @Test
    void falhaDeRollbackBloqueiaOLogENaoAceitaNovasEscritas(@TempDir Path pasta) throws Exception {
        byte[] inicial = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), inicial);

        // Canal que falha a escrita E a truncagem: o rollback não é possível.
        CanalFalso canal = new CanalFalso(inicial, true, Set.of(), true);

        try (LogRegistos log = LogRegistos.paraTestes(pasta, canal, 1, 0, null)) {
            IOException e = assertThrows(IOException.class, () -> log.acrescentar("ana", "arquivo", "segundo"));
            assertEquals(1, e.getSuppressed().length, "A falha do rollback devia ficar em getSuppressed().");

            assertTrue(log.estaBloqueado(), "Sem rollback o log tem de entrar em fail-stop.");
            assertEquals(1, canal.escritas);
            assertEquals(1, canal.truncagens, "O rollback devia ter sido tentado uma vez.");

            // Segunda tentativa: recusada ANTES de tocar no canal.
            int escritasAntes = canal.escritas;
            IOException bloqueadoEx = assertThrows(IOException.class,
                    () -> log.acrescentar("ana", "arquivo", "terceiro"));
            assertTrue(bloqueadoEx.getMessage().contains("bloqueado"),
                    "Mensagem inesperada: " + bloqueadoEx.getMessage());
            assertEquals(escritasAntes, canal.escritas,
                    "Uma instância bloqueada não pode voltar a escrever no log.");
            assertEquals(1, canal.truncagens, "Uma instância bloqueada não pode voltar a truncar.");
            assertEquals(1, log.ultimoLsn(), "O LSN não pode avançar com uma escrita falhada.");
        }

        assertArrayEquals(inicial, Files.readAllBytes(ficheiro(pasta)),
                "O fail-stop não pode alterar o ficheiro sem uma reparação com intenção.");
    }

    @Test
    void rollbackComTruncagemOkMasForceFalhadoBloqueiaOLog(@TempDir Path pasta) throws Exception {
        // A escrita chega ao canal, o force #1 (durabilidade) falha, o truncate do
        // rollback CORRE, mas o force #2 (do rollback) falha.
        byte[] inicial = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), inicial);

        CanalFalso canal = new CanalFalso(inicial, false, Set.of(1, 2), false);

        try (LogRegistos log = LogRegistos.paraTestes(pasta, canal, 1, 0, null)) {
            IOException e = assertThrows(IOException.class, () -> log.acrescentar("ana", "arquivo", "segundo"));
            assertEquals(1, e.getSuppressed().length,
                    "A falha do force do rollback devia ficar em getSuppressed().");

            assertTrue(log.estaBloqueado(), "Truncagem sem force durável tem de bloquear o log.");
            assertEquals(2, canal.forcares, "Um force da escrita + um force do rollback.");
            assertEquals(1, canal.truncagens, "O rollback devia ter truncado uma vez.");
            assertArrayEquals(inicial, canal.conteudo(), "A truncagem repôs o conteúdo: não pode ficar agravado.");

            int escritasAntes = canal.escritas;
            assertThrows(IOException.class, () -> log.acrescentar("ana", "arquivo", "terceiro"));
            assertEquals(escritasAntes, canal.escritas,
                    "Uma instância bloqueada não pode voltar a escrever no log.");
            assertEquals(1, canal.truncagens, "Uma instância bloqueada não pode voltar a truncar.");
            assertEquals(1, log.ultimoLsn(), "O LSN não pode avançar com uma escrita falhada.");
            assertArrayEquals(inicial, canal.conteudo(), "O conteúdo não pode ser agravado.");
        }

        assertArrayEquals(inicial, Files.readAllBytes(ficheiro(pasta)),
                "O ficheiro em disco tem de ficar intacto para o arranque seguinte reparar.");
    }

    @Test
    void escritaFalhadaComForceDeRollbackFalhadoBloqueiaOLog(@TempDir Path pasta) throws Exception {
        // Variante com a falha do lado da escrita: a escrita falha (cauda parcial no
        // canal), o truncate corre, mas o force do rollback falha.
        byte[] inicial = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), inicial);

        CanalFalso canal = new CanalFalso(inicial, true, Set.of(1), false);

        try (LogRegistos log = LogRegistos.paraTestes(pasta, canal, 1, 0, null)) {
            assertThrows(IOException.class, () -> log.acrescentar("ana", "arquivo", "segundo"));

            assertTrue(log.estaBloqueado(), "Rollback sem force durável tem de bloquear o log.");
            assertEquals(1, canal.truncagens);
            assertEquals(1, canal.forcares);
            assertArrayEquals(inicial, canal.conteudo(), "A truncagem repôs o conteúdo.");

            assertThrows(IOException.class, () -> log.acrescentar("ana", "arquivo", "terceiro"));
            assertEquals(1, log.ultimoLsn());
        }

        assertArrayEquals(inicial, Files.readAllBytes(ficheiro(pasta)));
    }

    @Test
    void falhaDeEscritaComRollbackBemSucedidoNaoBloqueia(@TempDir Path pasta) throws Exception {
        byte[] inicial = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), inicial);

        CanalFalso canal = new CanalFalso(inicial, true, Set.of(), false);

        try (LogRegistos log = LogRegistos.paraTestes(pasta, canal, 1, 0, null)) {
            assertThrows(IOException.class, () -> log.acrescentar("ana", "arquivo", "segundo"));

            assertFalse(log.estaBloqueado(), "Com rollback bem sucedido o log continua utilizável.");
            assertEquals(inicial.length, canal.tamanho(),
                    "O rollback tem de repor o ficheiro no tamanho inicial.");
            assertEquals(1, log.ultimoLsn(), "O LSN não pode avançar com uma escrita falhada.");
        }
    }

    @Test
    void falhaDeForceComRollbackBemSucedidoNaoBloqueiaEALinhaSeguinteFunciona(@TempDir Path pasta) throws Exception {
        byte[] inicial = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), inicial);

        CanalFalso canal = new CanalFalso(inicial, false, Set.of(1), false);

        try (LogRegistos log = LogRegistos.paraTestes(pasta, canal, 1, 0, null)) {
            assertThrows(IOException.class, () -> log.acrescentar("ana", "arquivo", "segundo"));
            assertFalse(log.estaBloqueado(), "Uma falha de force com rollback possível não bloqueia o log.");
            assertEquals(inicial.length, canal.tamanho(), "A linha não durável tem de ser reposta.");

            assertEquals(2, log.acrescentar("rui", "arquivo", "segundo"));
        }
    }

    // ------------------------------------------------------------------
    // Intenção de reparação: retoma e não duplica REPARACAO
    // ------------------------------------------------------------------

    @Test
    void intencaoPendenteComReparacaoJaEmitidaNaoDuplicaEvento(@TempDir Path pasta) throws Exception {
        // Crash DEPOIS de emitir REPARACAO e ANTES de apagar a intenção.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), l1);                 // log já reparado (tamanho == fimValido)

        String id = "intent-fixo-1";
        LogRegistos.escreverIntencao(pasta, new LogRegistos.IntencaoReparacao(id, l1.length, 37, 1));
        escreverEvento(pasta, "REPARACAO",
                "registos.log: 37 bytes de uma escrita parcial descartados; ultimoLsn=1; intent=" + id);

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn(), "A reparação não pode inventar registos.");
            assertEquals(37, log.bytesDescartadosNaRecuperacao(),
                    "A reparação concluída neste arranque tem de ser reportada.");
        }

        assertEquals(1, contarEventos(pasta, "REPARACAO"), "A reparação não pode ser registada duas vezes.");
        assertFalse(Files.exists(ficheiroIntencao(pasta)),
                "A intenção tem de ser apagada depois de a evidência existir.");

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn());
            assertEquals(0, log.bytesDescartadosNaRecuperacao(),
                    "Sem intenção pendente não há reparação a reportar.");
        }
        assertEquals(1, contarEventos(pasta, "REPARACAO"), "Reinícios não podem duplicar o evento.");
    }

    @Test
    void intencaoPendenteSemEventoConcluiATruncagemEEmiteUmUnicoEvento(@TempDir Path pasta) throws Exception {
        // Crash DEPOIS de gravar a intenção e ANTES de truncar: a cauda ainda existe
        // e tem exatamente `descartados` bytes.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        long descartados = l2.length / 2;
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, (int) descartados)));

        String id = "intent-fixo-2";
        LogRegistos.escreverIntencao(pasta, new LogRegistos.IntencaoReparacao(id, l1.length, descartados, 1));

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn(), "Só a linha 1 devia sobreviver.");
            assertEquals(descartados, log.bytesDescartadosNaRecuperacao(),
                    "O arranque devia reportar a mesma reparação que a intenção descrevia.");
        }

        assertEquals(l1.length, Files.size(ficheiro(pasta)), "O ficheiro devia ficar truncado no último '\\n'.");
        assertEquals(1, contarEventos(pasta, "REPARACAO"));
        assertTrue(lerEventos(pasta).contains("intent=" + id), "O evento tem de identificar a intenção.");
        assertFalse(Files.exists(ficheiroIntencao(pasta)));

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn());
            assertEquals(0, log.bytesDescartadosNaRecuperacao());
        }
        assertEquals(1, contarEventos(pasta, "REPARACAO"));
    }

    @Test
    void intencaoPendenteSemEventoNemCaudaEmiteUmUnicoEvento(@TempDir Path pasta) throws Exception {
        // Crash DEPOIS de truncar e ANTES de emitir o evento: tamanho == fimValido.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), l1);

        String id = "intent-fixo-3";
        LogRegistos.escreverIntencao(pasta, new LogRegistos.IntencaoReparacao(id, l1.length, 37, 1));

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn());
            assertEquals(37, log.bytesDescartadosNaRecuperacao(),
                    "A reparação foi concluída neste arranque: tem de ser reportada.");
        }

        assertEquals(1, contarEventos(pasta, "REPARACAO"),
                "A reparação aconteceu, por isso a evidência tem de ser emitida uma vez.");
        assertTrue(lerEventos(pasta).contains("intent=" + id));
        assertFalse(Files.exists(ficheiroIntencao(pasta)));
    }

    @Test
    void reparacaoSemIntencaoPreviaEmiteUmUnicoEventoERetomaONumeroCerto(@TempDir Path pasta) throws Exception {
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, 10)));

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn());
            assertTrue(log.bytesDescartadosNaRecuperacao() > 0);
            assertEquals(2, log.acrescentar("rui", "arquivo", "segundo"));
        }

        assertEquals(1, contarEventos(pasta, "REPARACAO"));
        assertFalse(Files.exists(ficheiroIntencao(pasta)), "A intenção não pode ficar pendente.");
        assertEquals(2, LogRegistos.lerTodos(pasta).size());
    }

    @Test
    void caudaParcialDoFicheiroDeEventosEhTruncadaAntesDeEscreverAIntencao(@TempDir Path pasta) throws Exception {
        // O diário de eventos ficou com uma linha cortada (sem '\n').
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), unir(l1, cortado(linha(2, "rui", "arquivo", "segundo"), 10)));
        Files.write(ficheiroEventos(pasta), unir(
                utf8("2025-01-01T00:00:00Z;ARRANQUE;líder online\n"),
                utf8("2025-01-01T00:00:01Z;CORTADO")));   // sem '\n'

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn());
        }

        String eventos = lerEventos(pasta);
        assertFalse(eventos.contains("CORTADO"), "A cauda parcial do diário de eventos tem de desaparecer.");
        assertTrue(eventos.endsWith("\n"), "O diário de eventos tem de terminar numa linha completa.");
        assertEquals(1, contarEventos(pasta, "REPARACAO"));
    }

    // ------------------------------------------------------------------
    // Validação estrita do prefixo do log (P1..P6)
    // ------------------------------------------------------------------

    @Test
    void intencaoComHashCorretoEhConcluida(@TempDir Path pasta) throws Exception {
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        long descartados = l2.length / 2;
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, (int) descartados)));

        LogRegistos.escreverIntencao(pasta, new LogRegistos.IntencaoReparacao(
                "intent-hash-ok", l1.length, descartados, 1, hash(l1, l1.length)));

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn());
            assertEquals(descartados, log.bytesDescartadosNaRecuperacao());
        }
        assertEquals(l1.length, Files.size(ficheiro(pasta)));
        assertEquals(1, contarEventos(pasta, "REPARACAO"));
        assertFalse(Files.exists(ficheiroIntencao(pasta)));
    }

    @Test
    void intencaoComHashQueNaoCorrespondeAbortaArranque(@TempDir Path pasta) throws Exception {
        // Byte alterado dentro do Base64 pode continuar a descodificar: só o hash
        // deteta a diferença. Aqui simulamos a divergência com um hash "de outro log".
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, 10)));

        String hashErrado = hash(l1, l1.length - 1);   // descreve um prefixo diferente
        LogRegistos.escreverIntencao(pasta, new LogRegistos.IntencaoReparacao(
                "intent-hash-mau", l1.length, 10, 1, hashErrado));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertEquals(l1.length + 10, Files.size(ficheiro(pasta)), "Nada pode ser truncado.");
        assertTrue(Files.exists(ficheiroIntencao(pasta)), "A intenção tem de ser preservada.");
        assertEquals(0, contarEventos(pasta, "REPARACAO"), "Não pode emitir evidência de uma reparação inválida.");
    }

    @Test
    void intencaoComPrefixoCorrompidoAbortaAntesDeTruncarOuEmitir(@TempDir Path pasta) throws Exception {
        // O prefixo [0, fimValido) contém uma linha terminada inválida.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] lixo = utf8("lixo-terminado-por-newline\n");
        byte[] l3 = linha(3, "rui", "arquivo", "terceiro");
        Files.write(ficheiro(pasta), unir(l1, lixo, cortado(l3, 10)));

        long fimValido = l1.length + lixo.length;
        LogRegistos.escreverIntencao(pasta,
                new LogRegistos.IntencaoReparacao("intent-prefixo-mau", fimValido, 10, 3));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertEquals(fimValido + 10, Files.size(ficheiro(pasta)), "O log não pode ser truncado.");
        assertTrue(Files.exists(ficheiroIntencao(pasta)), "A intenção tem de ser preservada.");
        assertEquals(0, contarEventos(pasta, "REPARACAO"), "Não pode emitir evidência antes de validar.");
    }

    @Test
    void intencaoComUltimoLsnIncorretoAbortaArranque(@TempDir Path pasta) throws Exception {
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), l1);

        LogRegistos.escreverIntencao(pasta,
                new LogRegistos.IntencaoReparacao("intent-lsn-mau", l1.length, 0, 2));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertEquals(l1.length, Files.size(ficheiro(pasta)));
        assertTrue(Files.exists(ficheiroIntencao(pasta)));
        assertEquals(0, contarEventos(pasta, "REPARACAO"));
    }

    @Test
    void intencaoComFimValidoQueCortaUmRegistoAAbortaArranque(@TempDir Path pasta) throws Exception {
        // fimValido a meio de uma linha: o prefixo não termina num '\n'.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        Files.write(ficheiro(pasta), unir(l1, l2));

        LogRegistos.escreverIntencao(pasta,
                new LogRegistos.IntencaoReparacao("intent-meio", l1.length + 10, 5, 1));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertEquals(l1.length + l2.length, Files.size(ficheiro(pasta)), "Nada pode ser truncado.");
        assertTrue(Files.exists(ficheiroIntencao(pasta)));
    }

    @Test
    void intencaoVaziaComUltimoLsnNaoNuloAbortaArranque(@TempDir Path pasta) throws Exception {
        Files.createDirectories(pasta);
        LogRegistos.escreverIntencao(pasta,
                new LogRegistos.IntencaoReparacao("intent-vazia", 0, 0, 5));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertTrue(Files.exists(ficheiroIntencao(pasta)));
        assertEquals(0, contarEventos(pasta, "REPARACAO"));
    }

    @Test
    void intencaoPendenteComLogAusenteAbortaEPreservaTudo(@TempDir Path pasta) throws Exception {
        Files.createDirectories(pasta);
        LogRegistos.escreverIntencao(pasta, new LogRegistos.IntencaoReparacao("intent-sem-log", 20, 5, 1));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertFalse(Files.exists(ficheiro(pasta)), "O arranque não pode criar um log novo por causa da intenção.");
        assertTrue(Files.exists(ficheiroIntencao(pasta)), "A intenção tem de ser preservada.");
        assertEquals(0, contarEventos(pasta, "REPARACAO"));
    }

    @Test
    void intencaoPendenteComLogMaisCurtoAbortaSemAlterarNada(@TempDir Path pasta) throws Exception {
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), l1);

        LogRegistos.escreverIntencao(pasta,
                new LogRegistos.IntencaoReparacao("intent-curto", l1.length + 50, 10, 1));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertArrayEquals(l1, Files.readAllBytes(ficheiro(pasta)), "O log não pode ser alterado.");
        assertTrue(Files.exists(ficheiroIntencao(pasta)), "A intenção tem de ser preservada.");
        assertEquals(0, contarEventos(pasta, "REPARACAO"));
    }

    @Test
    void intencaoPendenteComLogQueCresceuAbortaSemTruncar(@TempDir Path pasta) throws Exception {
        // Intenção antiga (o apagar falhou) e o log voltou a crescer com registos
        // completos depois de fimValido: truncar apagaria registos confirmados.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        Files.write(ficheiro(pasta), unir(l1, l2));

        LogRegistos.escreverIntencao(pasta, new LogRegistos.IntencaoReparacao("intent-antiga", l1.length, 10, 1));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertEquals(l1.length + l2.length, Files.size(ficheiro(pasta)),
                "Um log que cresceu não pode ser truncado.");
        assertTrue(Files.exists(ficheiroIntencao(pasta)));
        assertEquals(0, contarEventos(pasta, "REPARACAO"));
    }

    // ------------------------------------------------------------------
    // P7: a cauda tem de ter exatamente `descartados` bytes
    // ------------------------------------------------------------------

    @Test
    void intencaoComCaudaMenorQueODescritoAbortaArranque(@TempDir Path pasta) throws Exception {
        // A intenção diz que descartou 37 bytes, mas a cauda só tem 5.
        // Truncar e reportar 37 seria inventar uma reparação que não aconteceu.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, 5)));

        LogRegistos.escreverIntencao(pasta,
                new LogRegistos.IntencaoReparacao("intent-cauda-curta", l1.length, 37, 1));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertEquals(l1.length + 5, Files.size(ficheiro(pasta)), "O log não pode ser truncado.");
        assertTrue(Files.exists(ficheiroIntencao(pasta)), "A intenção tem de ser preservada.");
        assertEquals(0, contarEventos(pasta, "REPARACAO"),
                "Não pode reportar 37 bytes descartados quando só havia 5.");
    }

    @Test
    void intencaoComCaudaMaiorQueODescritoAbortaArranque(@TempDir Path pasta) throws Exception {
        // A intenção diz 5 bytes, a cauda tem 20: o log cresceu depois da intenção.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, 20)));

        LogRegistos.escreverIntencao(pasta,
                new LogRegistos.IntencaoReparacao("intent-cauda-longa", l1.length, 5, 1));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertEquals(l1.length + 20, Files.size(ficheiro(pasta)), "Nada pode ser truncado.");
        assertTrue(Files.exists(ficheiroIntencao(pasta)));
        assertEquals(0, contarEventos(pasta, "REPARACAO"));
    }

    @Test
    void intencaoComDescartadosZeroMasCaudaPresenteAbortaArranque(@TempDir Path pasta) throws Exception {
        // descartados=0 descreve um log sem cauda; se há cauda, o estado não é o descrito.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), unir(l1, utf8("resto-de-uma-linha")));

        LogRegistos.escreverIntencao(pasta,
                new LogRegistos.IntencaoReparacao("intent-zero", l1.length, 0, 1));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertEquals(l1.length + "resto-de-uma-linha".length(), Files.size(ficheiro(pasta)));
        assertTrue(Files.exists(ficheiroIntencao(pasta)));
        assertEquals(0, contarEventos(pasta, "REPARACAO"));
    }

    @Test
    void intencaoComCaudaExataConcluiMesmoComHash(@TempDir Path pasta) throws Exception {
        // Caso legítimo: tamanho == fimValido + descartados, sem '\n' na cauda.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        long descartados = 17;
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, (int) descartados)));

        LogRegistos.escreverIntencao(pasta, new LogRegistos.IntencaoReparacao(
                "intent-exata", l1.length, descartados, 1, hash(l1, l1.length)));

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn());
            assertEquals(descartados, log.bytesDescartadosNaRecuperacao());
        }

        assertEquals(l1.length, Files.size(ficheiro(pasta)));
        assertEquals(1, contarEventos(pasta, "REPARACAO"));
        assertTrue(lerEventos(pasta).contains("intent=intent-exata"));
        assertFalse(Files.exists(ficheiroIntencao(pasta)));
    }

    // ------------------------------------------------------------------
    // Diário de eventos: deduplicação estrita e validação
    // ------------------------------------------------------------------

    @Test
    void eventoDeOutroTipoComMesmoIntentNaoSuprimeReparacao(@TempDir Path pasta) throws Exception {
        // Uma linha ARRANQUE cujo detalhe menciona o intentId não é evidência de
        // reparação. A procura por substring suprimia o REPARACAO.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        long descartados = l2.length / 2;
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, (int) descartados)));

        String id = "intent-em-outro-evento";
        LogRegistos.escreverIntencao(pasta,
                new LogRegistos.IntencaoReparacao(id, l1.length, descartados, 1));
        escreverEvento(pasta, "ARRANQUE", "reparação pendente intent=" + id);
        escreverEvento(pasta, "REGISTO", "lsn=1 autor=ana canal=arquivo bytes=58; intent=" + id);

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn());
        }

        assertEquals(1, contarEventos(pasta, "REPARACAO"),
                "A evidência tem de ser emitida: nenhuma outra linha a pode substituir.");
        assertEquals(1, contarEventos(pasta, "ARRANQUE"));
        assertEquals(1, contarEventos(pasta, "REGISTO"));
        assertFalse(Files.exists(ficheiroIntencao(pasta)));
    }

    @Test
    void intentIdQueEhPrefixoDeOutroNaoDeduplica(@TempDir Path pasta) throws Exception {
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        long descartados = l2.length / 2;
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, (int) descartados)));

        String id = "abc";
        LogRegistos.escreverIntencao(pasta,
                new LogRegistos.IntencaoReparacao(id, l1.length, descartados, 1));
        escreverEvento(pasta, "REPARACAO",
                "registos.log: 9 bytes de uma escrita parcial descartados; ultimoLsn=1; intent=abc-outro");

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn());
        }

        assertEquals(2, contarEventos(pasta, "REPARACAO"),
                "A reparação de 'abc' tem de emitir a sua própria evidência.");
        assertTrue(lerEventos(pasta).contains("intent=abc\n"),
                "Tem de existir uma linha com o intent exato.");
    }

    @Test
    void linhaDeEventosInvalidaTerminadaAbortaArranque(@TempDir Path pasta) throws Exception {
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), l1);
        Files.write(ficheiroEventos(pasta), unir(
                utf8("2025-01-01T00:00:00Z;ARRANQUE;ok\n"),
                utf8("linha-sem-campos\n")));

        long tamanhoEventos = Files.size(ficheiroEventos(pasta));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertEquals(tamanhoEventos, Files.size(ficheiroEventos(pasta)),
                "O diário de eventos tem de ser preservado para inspeção.");
        assertArrayEquals(l1, Files.readAllBytes(ficheiro(pasta)));
    }

    @Test
    void linhaDeEventosComInstanteInvalidoAbortaArranque(@TempDir Path pasta) throws Exception {
        Files.write(ficheiro(pasta), linha(1, "ana", "arquivo", "primeiro"));
        Files.write(ficheiroEventos(pasta), utf8("nao-e-um-instante;ARRANQUE;ok\n"));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));
        assertTrue(Files.exists(ficheiroEventos(pasta)));
    }

    @Test
    void diarioComLinhaInvalidaECaudaParcialNaoEhAlterado(@TempDir Path pasta) throws Exception {
        // O diário tem (1) uma linha completa inválida E (2) uma cauda parcial.
        // Antes, a cauda era cortada antes da validação, pelo que o arranque abortava
        // com o diário já modificado. Agora: validar primeiro, alterar depois.
        Files.write(ficheiro(pasta), linha(1, "ana", "arquivo", "primeiro"));
        byte[] diario = unir(
                utf8("2025-01-01T00:00:00Z;ARRANQUE;ok\n"),
                utf8("linha-sem-campos\n"),                  // completa e inválida
                utf8("2025-01-01T00:00:02Z;REPAR"));         // cauda parcial (sem '\n')
        Files.write(ficheiroEventos(pasta), diario);

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertArrayEquals(diario, Files.readAllBytes(ficheiroEventos(pasta)),
                "Nenhuma truncagem pode acontecer antes de o diário ser validado.");
    }

    @Test
    void diarioComLinhaComDoisCamposAbortaSemAlterar(@TempDir Path pasta) throws Exception {
        Files.write(ficheiro(pasta), linha(1, "ana", "arquivo", "primeiro"));
        byte[] diario = unir(
                utf8("2025-01-01T00:00:00Z;ARRANQUE\n"),     // 2 campos: falta o detalhe
                utf8("cauda-parcial"));
        Files.write(ficheiroEventos(pasta), diario);

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertArrayEquals(diario, Files.readAllBytes(ficheiroEventos(pasta)),
                "O diário não pode ser tocado quando há uma linha completa inválida.");
    }

    // ------------------------------------------------------------------
    // Intenção ilegível
    // ------------------------------------------------------------------

    @Test
    void intencaoIlegivelAbortaArranqueEPreservaOFicheiro(@TempDir Path pasta) throws Exception {
        Files.createDirectories(pasta);
        byte[] lixo = utf8("isto-nao-e-uma-intencao\n");
        Files.write(ficheiroIntencao(pasta), lixo);
        Files.write(ficheiro(pasta), linha(1, "ana", "arquivo", "primeiro"));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertArrayEquals(lixo, Files.readAllBytes(ficheiroIntencao(pasta)),
                "Uma intenção ilegível NÃO pode ser apagada em silêncio.");
        assertEquals(0, contarEventos(pasta, "REPARACAO"));
    }

    @Test
    void intencaoComCamposEmFaltaAbortaArranque(@TempDir Path pasta) throws Exception {
        Files.createDirectories(pasta);
        Files.write(ficheiroIntencao(pasta), utf8("tpf-reparacao-v1\nintentId=abc\n"));
        Files.write(ficheiro(pasta), linha(1, "ana", "arquivo", "primeiro"));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));
        assertTrue(Files.exists(ficheiroIntencao(pasta)));
    }

    @Test
    void intencaoComLinhaSemChaveValorAbortaArranque(@TempDir Path pasta) throws Exception {
        Files.createDirectories(pasta);
        Files.write(ficheiroIntencao(pasta),
                utf8("tpf-reparacao-v1\nintentId=abc\nfimValido=10\ndescartados=1\nultimoLsn=1\nlixo\n"));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));
        assertTrue(Files.exists(ficheiroIntencao(pasta)));
    }

    @Test
    void intencaoComHashMalFormadoAbortaArranque(@TempDir Path pasta) throws Exception {
        Files.createDirectories(pasta);
        Files.write(ficheiroIntencao(pasta),
                utf8("tpf-reparacao-v1\nintentId=abc\nfimValido=10\ndescartados=1\nultimoLsn=1\n"
                        + "prefixoSha256=nao-e-hex\n"));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));
        assertTrue(Files.exists(ficheiroIntencao(pasta)));
    }

    // ------------------------------------------------------------------
    // Regras de validação do log
    // ------------------------------------------------------------------

    @Test
    void caudaSemTerminadorEhTruncadaENaoRepeteLsn(@TempDir Path pasta) throws Exception {
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, l2.length / 2)));

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn(), "Só a linha 1 devia sobreviver.");
            assertTrue(log.bytesDescartadosNaRecuperacao() > 0);
            assertEquals(l1.length, Files.size(ficheiro(pasta)), "O ficheiro devia ficar truncado no último '\\n'.");
            assertEquals(2, log.acrescentar("rui", "arquivo", "segundo"));
        }
        assertEquals(2, LogRegistos.lerTodos(pasta).size());
    }

    @Test
    void caudaComTerminadorMasBase64InvalidoAbortaArranque(@TempDir Path pasta) throws Exception {
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] ma = utf8("2;2025-01-01T00:00:00Z;rui;arquivo;@@@isto-nao-e-base64@@@\n");
        Files.write(ficheiro(pasta), unir(l1, ma));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));
        assertEquals(l1.length + ma.length, Files.size(ficheiro(pasta)),
                "Uma linha terminada pode ter LSN confirmado: o ficheiro não pode ser alterado.");
        assertFalse(Files.exists(ficheiroIntencao(pasta)), "Uma corrupção não gera intenção de reparação.");
    }

    @Test
    void linhaInvalidaNoMeioAbortaArranque(@TempDir Path pasta) throws Exception {
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] lixo = utf8("lixo-no-meio-do-ficheiro\n");
        byte[] l3 = linha(3, "rui", "arquivo", "terceiro");
        Files.write(ficheiro(pasta), unir(l1, lixo, l3));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));
    }

    @Test
    void lsnForaDeSequenciaAbortaArranque(@TempDir Path pasta) throws Exception {
        Files.write(ficheiro(pasta), unir(
                linha(1, "ana", "arquivo", "primeiro"),
                linha(3, "rui", "arquivo", "terceiro")));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));
    }

    @Test
    void caudaQueAindaAnalisaBemMasSemTerminadorEhDescartada(@TempDir Path pasta) throws Exception {
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "");
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, l2.length - 1)));   // corta o '\n'

        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.ultimoLsn(), "O terminador é o marcador de commit: sem '\\n' não é registo.");
        }
    }

    @Test
    void textoComPontoEVirgulaENewlineSobreviveAoRoundTrip(@TempDir Path pasta) throws Exception {
        String texto = "linha com ; e\nmudança de linha";
        try (LogRegistos log = LogRegistos.abrir(pasta, null)) {
            assertEquals(1, log.acrescentar("autor;com;pontos", "arquivo", texto));
        }
        var lidos = LogRegistos.lerTodos(pasta);
        assertEquals(1, lidos.size());
        assertEquals(texto, lidos.get(0).texto());
        assertEquals("autor_com_pontos", lidos.get(0).autor(), "O ';' tem de ser neutralizado no escritor.");
    }

    @Test
    void intencaoComCaudaDeTamanhoCertoMasTerminadaAbortaArranque(@TempDir Path pasta) throws Exception {
        // A cauda tem EXATAMENTE os `descartados` bytes registados, mas é uma linha
        // COMPLETA (terminada por '\n'). O tamanho bate certo por coincidência; só o
        // terminador desmascara o estado: há um registo confirmado depois de fimValido.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");

        byte[] logInicial = unir(l1, l2);
        Files.write(ficheiro(pasta), logInicial);

        LogRegistos.escreverIntencao(pasta, new LogRegistos.IntencaoReparacao(
                "intent-cauda-terminada", l1.length, l2.length, 1, hash(l1, l1.length)));
        byte[] intencaoInicial = Files.readAllBytes(ficheiroIntencao(pasta));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertArrayEquals(logInicial, Files.readAllBytes(ficheiro(pasta)),
                "O log não pode ser truncado: a cauda contém um registo completo.");
        assertArrayEquals(intencaoInicial, Files.readAllBytes(ficheiroIntencao(pasta)),
                "A intenção tem de ficar preservada byte a byte.");
        assertEquals(0, contarEventos(pasta, "REPARACAO"),
                "Não pode ser emitida evidência de uma reparação que destruiria um registo.");
        assertFalse(Files.exists(ficheiroEventos(pasta)),
                "O diário de eventos nem devia ter sido criado: nada foi reparado.");
    }

    @Test
    void intencaoComTerminadorANoMeioDaCaudaAbortaArranque(@TempDir Path pasta) throws Exception {
        // Cauda = [linha 2 completa] + [fragmento da linha 3]. O último byte da cauda
        // NÃO é '\n', e o total coincide com `descartados`: uma verificação ingénua
        // (endsWith("\n")) deixaria passar. Só a varredura do intervalo [fimValido,
        // tamanho) deteta a fronteira de registo lá dentro.
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        byte[] l3 = linha(3, "rui", "arquivo", "terceiro");
        byte[] fragmento = cortado(l3, 10);          // 10 bytes, sem '\n'

        byte[] logInicial = unir(l1, l2, fragmento);
        Files.write(ficheiro(pasta), logInicial);

        long descartados = (long) l2.length + fragmento.length;
        LogRegistos.escreverIntencao(pasta, new LogRegistos.IntencaoReparacao(
                "intent-terminador-no-meio", l1.length, descartados, 1, hash(l1, l1.length)));
        byte[] intencaoInicial = Files.readAllBytes(ficheiroIntencao(pasta));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertArrayEquals(logInicial, Files.readAllBytes(ficheiro(pasta)),
                "Nada pode ser truncado quando a cauda atravessa um registo completo.");
        assertArrayEquals(intencaoInicial, Files.readAllBytes(ficheiroIntencao(pasta)),
                "A intenção tem de ficar preservada byte a byte.");
        assertEquals(0, contarEventos(pasta, "REPARACAO"),
                "Não pode ser emitida evidência de reparação.");
    }

    @Test
    void intencaoIncompativelNaoAlteraODiarioDeEventos(@TempDir Path pasta) throws Exception {
        // Três condições ao mesmo tempo: o log precisa de reparação, o diário tem uma
        // cauda parcial (o primeiro alvo de escrita do arranque) e a intenção pendente
        // é INCOMPATÍVEL com o log (descartados ≠ tamanho real da cauda).
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        byte[] l2 = linha(2, "rui", "arquivo", "segundo");
        Files.write(ficheiro(pasta), unir(l1, cortado(l2, 10)));

        byte[] diario = unir(
                utf8("2025-01-01T00:00:00Z;ARRANQUE;ok\n"),
                utf8("2025-01-01T00:00:01Z;REPAR"));          // cauda parcial (sem '\n')
        Files.write(ficheiroEventos(pasta), diario);

        LogRegistos.escreverIntencao(pasta, new LogRegistos.IntencaoReparacao(
                "intent-incompativel", l1.length, 999, 1));    // 999 ≠ 10 bytes de cauda

        byte[] logInicial = Files.readAllBytes(ficheiro(pasta));
        byte[] intencaoInicial = Files.readAllBytes(ficheiroIntencao(pasta));

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertArrayEquals(diario, Files.readAllBytes(ficheiroEventos(pasta)),
                "o diário NÃO pode ser truncado antes de a intenção ser validada");
        assertArrayEquals(logInicial, Files.readAllBytes(ficheiro(pasta)),
                "o log não pode ser truncado");
        assertArrayEquals(intencaoInicial, Files.readAllBytes(ficheiroIntencao(pasta)),
                "a intenção tem de ser preservada");
        assertEquals(0, contarEventos(pasta, "REPARACAO"));
    }

    @Test
    void logCorrompidoNaoAlteraODiarioDeEventos(@TempDir Path pasta) throws Exception {
        byte[] l1 = linha(1, "ana", "arquivo", "primeiro");
        Files.write(ficheiro(pasta), unir(l1, utf8("lixo-terminado-por-newline\n")));

        byte[] diario = unir(
                utf8("2025-01-01T00:00:00Z;ARRANQUE;ok\n"),
                utf8("2025-01-01T00:00:01Z;CORT"));
        Files.write(ficheiroEventos(pasta), diario);

        assertThrows(LogRegistos.CorrupcaoException.class, () -> LogRegistos.abrir(pasta, null));

        assertArrayEquals(diario, Files.readAllBytes(ficheiroEventos(pasta)),
                "o diário tem de ficar byte a byte como estava");
        assertFalse(Files.exists(ficheiroIntencao(pasta)),
                "uma corrupção não pode gerar nem deixar intenção");
    }
}