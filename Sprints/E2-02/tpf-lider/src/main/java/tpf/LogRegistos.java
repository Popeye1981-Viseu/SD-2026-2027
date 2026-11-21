package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Diário durável do arquivo: o ficheiro `registos.log`.
 *
 * Formato de cada linha (E2-02):
 *
 *     lsn ; instante ISO-8601 ; autor ; canal ; texto em Base64   '\n'
 *
 * Invariantes:
 *  1. durabilidade: `force(true)` acontece ANTES de o LSN ser devolvido/respondido;
 *  2. numeração: LSN sequencial a partir de 1, sem repetições nem saltos;
 *  3. o '\n' final é o MARCADOR DE COMMIT da linha.
 *
 * Regra de recuperação (assente em 3):
 *  - cauda SEM '\n'          -> escrita parcial -> truncar (com intenção durável);
 *  - linha COM '\n' inválida -> corrupção       -> abortar o arranque;
 *  - linha inválida no meio  -> corrupção       -> abortar o arranque.
 *
 * Cinco garantias adicionais deste ficheiro:
 *  - FAIL-STOP: se o rollback de uma escrita falhada também falhar, a instância
 *    fica bloqueada (`bloqueado = true`) e recusa novas escritas.
 *  - INTENÇÃO DURÁVEL: a reparação da cauda é registada em `reparacao.intent`
 *    ANTES de se tocar no log; o evento REPARACAO é emitido no máximo uma vez.
 *  - PREFIXO VALIDADO: uma intenção pendente só é concluída se o prefixo
 *    `[0, fimValido)` do log terminar exatamente em `fimValido` E corresponder a
 *    `ultimoLsn` — estrutura completa, LSN 1..N sem lacunas e, quando a intenção
 *    traz `prefixoSha256`, igualdade criptográfica do conteúdo.
 *  - CAUDA CONFERIDA: `fimValido + descartados` é o tamanho que o log tinha quando
 *    a intenção foi gravada. Logo, `tamanho == fimValido` significa "já truncado"
 *    e `tamanho == fimValido + descartados` significa "cauda ainda presente".
 *    Qualquer outro tamanho é um estado NÃO descrito pela intenção -> aborta.
 *  - VALIDAR ANTES DE ALTERAR: nenhum ficheiro é cortado antes de as suas linhas
 *    completas serem validadas. Corrupção nunca deixa marcas da nossa passagem.
 *
 * Assunção documentada no README: o append local tem semântica de prefixo
 * (o conteúdo em disco é sempre um prefixo do que foi escrito).
 */
public final class LogRegistos implements AutoCloseable {

    /** Nome do ficheiro de registos, dentro da pasta de dados do líder. */
    public static final String FICHEIRO = "registos.log";

    /** Nome do diário de eventos (escrito pelo {@link Eventos}; lido aqui para validar e deduplicar). */
    public static final String FICHEIRO_EVENTOS = "eventos-lider.txt";

    /** Intenção de reparação pendente: existe apenas entre o início e o fim de uma truncagem. */
    public static final String FICHEIRO_INTENCAO = "reparacao.intent";

    private static final char SEP = ';';                       // separador de campo de registos.log
    private static final int NUM_CAMPOS = 5;                   // campos de uma linha de registos.log
    private static final int NUM_CAMPOS_EVENTO = 3;            // campos de uma linha de eventos
    private static final String CABECALHO_INTENCAO = "tpf-reparacao-v1";
    private static final String EVENTO_REPARACAO = "REPARACAO";
    private static final int BLOCO_LEITURA = 8192;             // bytes por leitura incremental

    /** Limite prático para validar o prefixo em memória (~2 GiB). */
    private static final long MAX_PREFIXO_BYTES = Integer.MAX_VALUE - 8L;

    /** Formato do SHA-256 em hexadecimal (64 dígitos minúsculos). */
    private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-f]{64}");

    /** O arranque não pode continuar: o estado em disco não é o esperado. */
    public static final class CorrupcaoException extends Exception {
        public CorrupcaoException(String mensagem) { super(mensagem); }
    }

    // ------------------------------------------------------------------
    // Intenção de reparação
    // ------------------------------------------------------------------

    /**
     * Descrição durável de uma truncagem de cauda parcial.
     *
     * Gravada em `reparacao.intent` antes da truncagem e apagada depois de o
     * evento REPARACAO ficar em `eventos-lider.txt`. É o que torna a reparação
     * retomável e o evento não duplicável.
     *
     * Invariante que o arranque verifica: no momento em que a intenção foi
     * gravada, `registos.log` tinha exatamente `fimValido + descartados` bytes.
     *
     * @param intentId       identificador único desta reparação (aparece no evento REPARACAO)
     * @param fimValido      offset do fim do último registo válido (tamanho final do ficheiro)
     * @param descartados    quantos bytes de cauda parcial foram descartados
     * @param ultimoLsn      último LSN válido do log antes da reparação
     * @param prefixoSha256  SHA-256 (hex) dos bytes `[0, fimValido)`, ou null em intenções
     *                       antigas. É a única propriedade que liga a intenção ao CONTEÚDO
     *                       do prefixo, e não apenas à sua forma e comprimento.
     */
    public record IntencaoReparacao(String intentId, long fimValido, long descartados,
                                    long ultimoLsn, String prefixoSha256) {

        public IntencaoReparacao {
            if (intentId == null || intentId.isBlank())
                throw new IllegalArgumentException("intentId não pode ser vazio");
            // O id aparece numa linha de eventos e num ficheiro chave=valor: não pode
            // conter os separadores desses formatos.
            if (intentId.indexOf(';') >= 0 || intentId.indexOf('\n') >= 0 || intentId.indexOf('=') >= 0)
                throw new IllegalArgumentException("intentId inválido: " + intentId);
            if (fimValido < 0) throw new IllegalArgumentException("fimValido não pode ser negativo");
            if (descartados < 0) throw new IllegalArgumentException("descartados não pode ser negativo");
            if (ultimoLsn < 0) throw new IllegalArgumentException("ultimoLsn não pode ser negativo");
            if (prefixoSha256 != null && !SHA256_HEX.matcher(prefixoSha256).matches())
                throw new IllegalArgumentException("prefixoSha256 tem de ser 64 dígitos hex: " + prefixoSha256);
        }

        /** Intenção sem impressão digital do prefixo (formato antigo: validação só estrutural). */
        public IntencaoReparacao(String intentId, long fimValido, long descartados, long ultimoLsn) {
            this(intentId, fimValido, descartados, ultimoLsn, null);
        }

        /** Tamanho que `registos.log` tinha quando esta intenção foi gravada. */
        public long tamanhoNaGravacao() { return fimValido + descartados; }
    }

    // ------------------------------------------------------------------
    // Abstração do canal de escrita (para injeção de falhas nos testes)
    // ------------------------------------------------------------------

    /**
     * Operações mínimas que o log precisa de um canal de escrita.
     *
     * Existe como interface para que os testes possam injetar falhas de
     * escrita/truncagem — não há forma de provocar um erro de I/O determinístico
     * num `FileChannel` real, e o comportamento de fail-stop tem de ser testável.
     */
    interface Canal extends AutoCloseable {
        /** Escreve até esgotar o buffer; pode deixar o ficheiro a meio de uma linha. */
        void escrever(ByteBuffer buf) throws IOException;

        /** Força os bytes para o disco (`fsync`). */
        void forcar() throws IOException;

        /** Corta o ficheiro no tamanho indicado (nunca alarga). */
        void truncar(long tamanho) throws IOException;

        /** Tamanho atual do ficheiro, em bytes. */
        long tamanho() throws IOException;

        @Override void close() throws IOException;
    }

    /** Canal real, sobre `registos.log` (aberto em modo append). */
    private static final class CanalFicheiro implements Canal {

        private final FileChannel ch;

        CanalFicheiro(Path ficheiro) throws IOException {
            // APPEND: cada escrita vai para o fim, sem reposicionar o canal.
            this.ch = FileChannel.open(ficheiro, StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        }

        @Override public void escrever(ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) ch.write(buf);
        }

        @Override public void forcar() throws IOException {
            ch.force(true);
        }

        @Override public void truncar(long tamanho) throws IOException {
            ch.truncate(tamanho);
        }

        @Override public long tamanho() throws IOException {
            return ch.size();
        }

        @Override public void close() throws IOException {
            ch.close();
        }
    }

    // ------------------------------------------------------------------
    // Estado
    // ------------------------------------------------------------------

    private final Path pasta;
    private final Canal canal;            // canal de append, já posicionado no fim
    private final Eventos eventos;        // pode ser null (testes)
    private long ultimoLsn;               // último LSN aceite/escrito
    private long bytesDescartados;        // bytes reparados por este arranque

    // FAIL-STOP: passado a true quando uma escrita falha e o rollback também.
    // volatile porque `estaBloqueado()` pode ser lido de outras threads (ex.: LeaderBot).
    private volatile boolean bloqueado;
    private volatile String motivoBloqueio;

    private LogRegistos(Path pasta, Canal canal, long ultimoLsn, long bytesDescartados, Eventos eventos) {
        this.pasta = pasta;
        this.canal = canal;
        this.ultimoLsn = ultimoLsn;
        this.bytesDescartados = bytesDescartados;
        this.eventos = eventos;
    }

    /**
     * Constrói uma instância com um canal injetado. Reservado a testes: permite
     * simular falhas de escrita e de rollback que, com um ficheiro real, seriam
     * impossíveis de provocar de forma determinística.
     */
    static LogRegistos paraTestes(Path pasta, Canal canal, long ultimoLsn, long bytesDescartados, Eventos eventos) {
        return new LogRegistos(pasta, canal, ultimoLsn, bytesDescartados, eventos);
    }

    // ------------------------------------------------------------------
    // Abertura + recuperação
    // ------------------------------------------------------------------

    /**
     * Abre (ou cria) `registos.log`, validando e reparando o ficheiro.
     *
     * Ordem das operações (a ordem é a garantia):
     *  0a. LÊ e VALIDA as linhas completas de `eventos-lider.txt` — sem alterar nada;
     *  0b. só depois, corta a cauda parcial do diário (é escrita parcial, não corrupção);
     *  1.  lê e conclui uma intenção de reparação pendente, validando o prefixo do log
     *      e conferindo que o tamanho corresponde a `fimValido` (já truncado) ou a
     *      `fimValido + descartados` (cauda ainda presente);
     *  2.  varre `registos.log`; se houver cauda sem '\n', grava a intenção (force),
     *      trunca, emite REPARACAO (no máximo uma vez) e apaga a intenção.
     *
     * Nenhum ficheiro é modificado antes de as suas linhas completas serem validadas:
     * perante corrupção, os ficheiros ficam byte-a-byte como estavam.
     *
     * @throws CorrupcaoException se houver uma linha terminada por '\n' que não seja
     *         um registo válido, um LSN fora de sequência, uma intenção de reparação
     *         ilegível ou incompatível com o log, ou uma linha de eventos inválida.
     *         Em qualquer destes casos o chamador deve terminar o processo: o
     *         arranque NÃO pode prosseguir.
     */
    public static LogRegistos abrir(Path pasta, Eventos eventos) throws IOException, CorrupcaoException {
        Path ficheiro = pasta.resolve(FICHEIRO);
        Files.createDirectories(pasta);

        // 0a) VALIDAR o diário de eventos ANTES de lhe tocar. Uma linha completa
        //     inválida é corrupção: abortamos com o ficheiro intacto.
        Path ficheiroEventos = pasta.resolve(FICHEIRO_EVENTOS);
        byte[] dadosEventos = lerBytesOpcional(ficheiroEventos);
        long eventosDescartados = 0;

        if (dadosEventos != null) {
            int fimValidoEventos = fimValidoDe(dadosEventos);
            validarLinhasEventos(dadosEventos, fimValidoEventos);

            // 0b) Só agora, com o ficheiro validado, se corta a cauda parcial.
            //     É seguro: o canal de `Eventos` usa O_APPEND, logo escreve sempre no
            //     fim do ficheiro atual, mesmo que este tenha sido truncado entretanto.
            if (fimValidoEventos < dadosEventos.length) {
                eventosDescartados = dadosEventos.length - (long) fimValidoEventos;
                truncar(ficheiroEventos, fimValidoEventos);
            }
        }

        if (eventosDescartados > 0) {
            System.err.println("AVISO: " + ficheiroEventos.getFileName() + ": "
                    + eventosDescartados + " bytes de uma escrita parcial descartados.");
        }

        long descartados = 0;

        // 1) Intenção pendente de um arranque anterior: concluí-la primeiro.
        //    É idempotente, não duplica o evento REPARACAO e — se o estado do log
        //    não for compatível — aborta sem tocar em nada (ver concluirReparacao).
        IntencaoReparacao pendente = lerIntencao(pasta);
        if (pendente != null) {
            concluirReparacao(pasta, pendente);
            // A reparação foi concluída/emitida NESTE arranque: os bytes descartados
            // são os que a intenção descreve, não zero. Sem isto, o operador veria
            // "sem reparações" logo a seguir a uma reparação ter corrido.
            descartados = pendente.descartados();
        }

        long ultimo;

        if (Files.exists(ficheiro)) {
            // Lemos os BYTES e procuramos '\n' explicitamente.
            // Nunca usar BufferedReader.readLine(): esconde se a última linha
            // tinha ou não terminador, que é precisamente a informação que decide
            // entre "escrita parcial" e "corrupção".
            byte[] dados = Files.readAllBytes(ficheiro);
            Varrimento v = varrer(dados);            // lança CorrupcaoException se houver lixo no meio
            ultimo = v.ultimoLsn();
            int fimValido = v.fimValido();

            if (fimValido < dados.length) {
                // Sobrou cauda sem '\n': escrita parcial interrompida por crash.
                long novos = dados.length - (long) fimValido;

                // 2) A INTENÇÃO tem de estar durável ANTES de se tocar no log,
                //    e leva a impressão digital do prefixo que diz ser válido.
                IntencaoReparacao intencao = new IntencaoReparacao(
                        UUID.randomUUID().toString(), fimValido, novos, ultimo,
                        sha256Hex(dados, fimValido));
                escreverIntencao(pasta, intencao);
                concluirReparacao(pasta, intencao);

                // Soma (e não atribuição): pode haver uma intenção pendente concluída
                // acima e, ainda assim, uma cauda nova descoberta pelo varrimento.
                descartados += novos;
            }
        } else {
            ultimo = 0;                              // ficheiro novo: o primeiro LSN será 1
        }

        // Só agora abrimos o canal de escrita, já com o ficheiro num estado íntegro.
        Canal canal = new CanalFicheiro(ficheiro);
        return new LogRegistos(pasta, canal, ultimo, descartados, eventos);
    }

    /** Lê todos os bytes do ficheiro, ou null se não existir. */
    private static byte[] lerBytesOpcional(Path ficheiro) throws IOException {
        return Files.exists(ficheiro) ? Files.readAllBytes(ficheiro) : null;
    }

    /** Offset imediatamente após o último '\n' (0 se não houver nenhum ou não houver dados). */
    static int fimValidoDe(byte[] dados) {
        if (dados == null) return 0;
        for (int i = dados.length - 1; i >= 0; i--) {
            if (dados[i] == '\n') return i + 1;
        }
        return 0;
    }

    // ------------------------------------------------------------------
    // Ficheiro de intenção
    // ------------------------------------------------------------------

    /**
     * Grava `reparacao.intent` e força-o para o disco, incluindo a entrada de
     * diretório que torna o nome visível.
     *
     * O `force(true)` do ficheiro e a sincronização do diretório são essenciais: a
     * intenção tem de sobreviver exatamente ao crash que a reparação seguinte vai
     * encontrar. Sem o `fsync` do diretório, um crash abrupto poderia deixar os
     * dados duráveis mas o NOME ausente (ou o inverso), conforme o sistema de
     * ficheiros e o modo de journaling.
     */
    static void escreverIntencao(Path pasta, IntencaoReparacao i) throws IOException {
        String texto = CABECALHO_INTENCAO + "\n"
                + "intentId=" + i.intentId() + "\n"
                + "fimValido=" + i.fimValido() + "\n"
                + "descartados=" + i.descartados() + "\n"
                + "ultimoLsn=" + i.ultimoLsn() + "\n"
                + (i.prefixoSha256() == null ? "" : "prefixoSha256=" + i.prefixoSha256() + "\n");

        Path ficheiro = pasta.resolve(FICHEIRO_INTENCAO);
        try (FileChannel ch = FileChannel.open(ficheiro, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.wrap(texto.getBytes(StandardCharsets.UTF_8));
            while (buf.hasRemaining()) ch.write(buf);
            ch.force(true);                  // dados + inode
        }
        sincronizarPasta(pasta);              // entrada de diretório (best-effort)
    }

    /**
     * Tenta forçar a entrada de diretório para o disco (`fsync` ao diretório),
     * de forma a tornar durável a criação ou remoção de `reparacao.intent`.
     *
     * Best-effort por necessidade: em Linux/macOS `FileChannel.open(dir, READ)`
     * seguido de `force(true)` faz exatamente isto; em Windows abrir um diretório
     * como canal falha, e a durabilidade da entrada fica dependente do sistema de
     * ficheiros (NTFS regista a criação praticamente de forma síncrona, mas não é
     * uma garantia formal). Não abortamos o arranque por causa disto: a intenção
     * continua a ser o melhor mecanismo disponível, e recusar arrancar por falta de
     * suporte do SO seria pior do que a janela de risco que resta.
     *
     * @return true se a sincronização foi efetiva; false se a plataforma não a suporta.
     */
    static boolean sincronizarPasta(Path pasta) {
        try (FileChannel dir = FileChannel.open(pasta, StandardOpenOption.READ)) {
            dir.force(true);
            return true;
        } catch (IOException | UnsupportedOperationException | SecurityException e) {
            System.err.println("AVISO: não foi possível sincronizar a entrada de diretório de "
                    + pasta.toAbsolutePath() + " (" + e.getMessage() + ")."
                    + " A durabilidade da criação/remoção de " + FICHEIRO_INTENCAO
                    + " fica dependente do sistema de ficheiros.");
            return false;
        }
    }

    /**
     * Lê `reparacao.intent`; devolve null se não existir.
     *
     * O campo `prefixoSha256` é opcional (intenções antigas não o têm): quando
     * existe, é a validação mais forte do prefixo do log; quando falta, aplicam-se
     * as validações estruturais.
     *
     * @throws CorrupcaoException se o ficheiro existir mas estiver ilegível ou
     *         malformado. NÃO o apagamos: uma intenção ilegível é sinal de crash
     *         a meio de uma reparação ou de alteração externa, e apagá-la
     *         esconderia a evidência e permitiria que o arranque prosseguisse
     *         sobre um estado que ninguém validou. Abortar permite inspeção manual.
     */
    static IntencaoReparacao lerIntencao(Path pasta) throws IOException, CorrupcaoException {
        Path ficheiro = pasta.resolve(FICHEIRO_INTENCAO);
        if (!Files.exists(ficheiro)) return null;

        // readAllLines lança MalformedInputException (IOException) se o ficheiro
        // não for UTF-8 válido; o chamador já trata de abortar.
        List<String> linhas = Files.readAllLines(ficheiro, StandardCharsets.UTF_8);
        if (linhas.isEmpty() || !CABECALHO_INTENCAO.equals(linhas.get(0).trim())) {
            throw ilegivel(ficheiro, "cabeçalho ausente ou desconhecido");
        }

        Map<String, String> campos = new HashMap<>();
        for (int i = 1; i < linhas.size(); i++) {
            String l = linhas.get(i).trim();
            if (l.isEmpty()) continue;
            int eq = l.indexOf('=');
            if (eq <= 0) throw ilegivel(ficheiro, "linha sem 'chave=valor': '" + l + "'");
            campos.put(l.substring(0, eq), l.substring(eq + 1));
        }

        try {
            return new IntencaoReparacao(
                    campos.get("intentId"),
                    Long.parseLong(campos.get("fimValido")),
                    Long.parseLong(campos.get("descartados")),
                    Long.parseLong(campos.get("ultimoLsn")),
                    campos.get("prefixoSha256"));    // null => validação só estrutural
        } catch (RuntimeException e) {
            // NumberFormatException (campo em falta, null ou não numérico) ou
            // IllegalArgumentException do construtor do record (id/hash/valores inválidos).
            throw ilegivel(ficheiro, e.toString());
        }
    }

    /** Constrói a exceção de intenção ilegível, com a instrução de remediação. */
    private static CorrupcaoException ilegivel(Path ficheiro, String motivo) {
        return new CorrupcaoException("Ficheiro de intenção ilegível: "
                + ficheiro.toAbsolutePath() + " (" + motivo + ")."
                + " O arranque foi abortado e nada foi alterado: inspecione o ficheiro"
                + " e, se a reparação já não for necessária, remova-o manualmente.");
    }

    /** Exceção para um estado do log incompatível com a intenção pendente. */
    private static CorrupcaoException incompativel(Path ficheiro, IntencaoReparacao i, String motivo) {
        return new CorrupcaoException("Reparação pendente (intent=" + i.intentId()
                + ") incompatível com " + ficheiro.toAbsolutePath() + ": " + motivo + "."
                + " Nada foi alterado — nem o log, nem a intenção, nem o diário de eventos."
                + " Arranque abortado para inspeção manual.");
    }

    // ------------------------------------------------------------------
    // Conclusão (idempotente) de uma reparação
    // ------------------------------------------------------------------

    /**
     * Conclui uma reparação descrita por uma intenção: valida o estado do log,
     * trunca (se ainda for preciso), emite REPARACAO no máximo uma vez e só então
     * apaga a intenção.
     *
     * É esta ordem que torna a operação retomável em qualquer ponto de crash:
     *  - crash após gravar a intenção e antes de truncar  -> trunca agora;
     *  - crash após truncar e antes de emitir o evento    -> emite agora;
     *  - crash após emitir o evento e antes de apagar     -> não emite de novo.
     *
     * @throws CorrupcaoException se o estado do log não for compatível com a intenção.
     */
    private static void concluirReparacao(Path pasta, IntencaoReparacao i)
            throws IOException, CorrupcaoException {
        Path ficheiro = pasta.resolve(FICHEIRO);

        // 1) Validar o estado ANTES de alterar seja o que for.
        long tamanho = validarEstadoContraIntencao(ficheiro, i);

        // 2) Truncar apenas se a cauda ainda estiver lá (tamanho > fimValido).
        //    Já se sabe que tem exatamente `descartados` bytes e nenhum '\n'.
        if (tamanho > i.fimValido()) truncar(ficheiro, i.fimValido());

        // 3) Emitir a evidência no MÁXIMO uma vez, mesmo que o processo tenha
        //    morrido entre a escrita do evento e o apagar da intenção.
        Path ficheiroEventos = pasta.resolve(FICHEIRO_EVENTOS);
        if (!eventoJaEmitido(ficheiroEventos, i.intentId())) {
            anexarEvento(ficheiroEventos, EVENTO_REPARACAO,
                    "registos.log: " + i.descartados() + " bytes de uma escrita parcial descartados;"
                            + " ultimoLsn=" + i.ultimoLsn() + "; intent=" + i.intentId());
        }

        // 4) Só depois de a evidência estar durável é que a intenção desaparece.
        Files.deleteIfExists(pasta.resolve(FICHEIRO_INTENCAO));
        sincronizarPasta(pasta);         // a remoção também é uma alteração de diretório
    }

    /**
     * Confirma que `registos.log` é compatível com a intenção pendente.
     *
     * Propriedades verificadas (ver README §5.2):
     *  P1 tamanho ≥ fimValido (ou o ficheiro é coerente com fimValido == 0);
     *  P2 o prefixo termina num '\n' (fimValido == 0 dispensa);
     *  P3 o prefixo é integralmente consumido por linhas completas;
     *  P4 cada linha do prefixo é estruturalmente válida;
     *  P5 os LSN do prefixo são 1..N sem lacunas e N == ultimoLsn;
     *  P6 SHA-256(prefixo) == prefixoSha256, quando a intenção o traz;
     *  P7 se o log cresceu, a cauda tem EXATAMENTE `descartados` bytes e nenhum '\n'
     *     — é a única forma de distinguir "ainda por truncar" de "já truncado"
     *     (tamanho == fimValido) e de detetar um estado que a intenção não descreve.
     *
     * @return o tamanho atual de `registos.log` (0 se não existir).
     */
    private static long validarEstadoContraIntencao(Path ficheiro, IntencaoReparacao i)
            throws IOException, CorrupcaoException {
        boolean existe = Files.exists(ficheiro);

        if (!existe) {
            // Sem log, só é coerente uma intenção "vazia": nada escrito ainda.
            if (i.fimValido() > 0) {
                throw incompativel(ficheiro, i, "esperava " + i.fimValido()
                        + " bytes válidos mas o ficheiro não existe (volume desmontado? ficheiro movido?)");
            }
            if (i.ultimoLsn() != 0) {
                throw incompativel(ficheiro, i, "não existe ficheiro, mas a intenção indica ultimoLsn="
                        + i.ultimoLsn() + " (deveria ser 0)");
            }
            return 0;
        }

        long tamanho = Files.size(ficheiro);

        if (tamanho < i.fimValido()) {
            throw incompativel(ficheiro, i, "esperava " + i.fimValido() + " bytes mas o ficheiro só tem "
                    + tamanho + "; concluir a reparação exigiria inventar conteúdo");
        }

        validarPrefixo(ficheiro, i);                 // P2..P6

        if (tamanho > i.fimValido()) {
            long cauda = tamanho - i.fimValido();

            if (contemTerminador(ficheiro, i.fimValido(), tamanho)) {
                throw incompativel(ficheiro, i, "já tem uma linha terminada por '\\n' depois de fimValido="
                        + i.fimValido() + " (tamanho " + tamanho + "); a intenção é anterior a registos"
                        + " completos, e truncar apagaria registos possivelmente confirmados");
            }

            // P7: a intenção descreve um log de `fimValido + descartados` bytes.
            // Se a cauda não tiver esse tamanho, o log NÃO é o log desta intenção:
            // truncar e reportar `descartados` seria inventar uma reparação.
            if (cauda != i.descartados()) {
                throw incompativel(ficheiro, i, "a cauda tem " + cauda + " bytes sem terminador, mas a"
                        + " intenção registou descartados=" + i.descartados() + " (o log devia ter "
                        + i.tamanhoNaGravacao() + " bytes e tem " + tamanho + "). O estado não"
                        + " corresponde ao descrito — nem já reparado (tamanho == fimValido), nem"
                        + " por reparar (tamanho == fimValido + descartados)");
            }
        }
        // tamanho == fimValido: a truncagem JÁ terminou num arranque anterior; falta
        // apenas a evidência, emitida no passo 3 de concluirReparacao.

        return tamanho;
    }

    /**
     * Valida o prefixo `[0, fimValido)` do log: termina num registo completo,
     * consome todos os bytes em linhas válidas, tem LSN 1..N sem lacunas, o
     * último LSN é `ultimoLsn` e (quando registada) a impressão digital coincide.
     */
    private static void validarPrefixo(Path ficheiro, IntencaoReparacao i)
            throws IOException, CorrupcaoException {
        if (i.fimValido() == 0) {
            // Prefixo vazio: só é coerente se nenhum LSN tiver sido atribuído.
            if (i.ultimoLsn() != 0) {
                throw incompativel(ficheiro, i, "fimValido=0 mas a intenção indica ultimoLsn="
                        + i.ultimoLsn() + " (deveria ser 0)");
            }
            return;
        }

        byte[] prefixo = lerPrefixo(ficheiro, i.fimValido());

        // P2: o prefixo tem de acabar num terminador. Se não acabar, a truncagem
        // cortaria um registo a meio e inventaria uma fronteira.
        if (prefixo[prefixo.length - 1] != '\n') {
            throw incompativel(ficheiro, i, "o byte em fimValido-1 não é '\\n': o prefixo não termina"
                    + " num registo completo");
        }

        // P3 + P4 + P5: reutiliza o mesmo varrimento do arranque normal.
        Varrimento v;
        try {
            v = varrer(prefixo);
        } catch (CorrupcaoException e) {
            throw incompativel(ficheiro, i, "o prefixo não é uma sequência válida de registos ("
                    + e.getMessage() + ")");
        }
        if (v.fimValido() != prefixo.length) {
            throw incompativel(ficheiro, i, "sobram " + (prefixo.length - v.fimValido())
                    + " bytes fora de linhas completas no prefixo");
        }
        if (v.ultimoLsn() != i.ultimoLsn()) {
            throw incompativel(ficheiro, i, "o prefixo termina no LSN " + v.ultimoLsn()
                    + ", mas a intenção diz ultimoLsn=" + i.ultimoLsn());
        }

        // P6: única validação que liga a intenção ao CONTEÚDO e não só à forma.
        if (i.prefixoSha256() != null) {
            String real = sha256Hex(prefixo, prefixo.length);
            if (!real.equals(i.prefixoSha256())) {
                throw incompativel(ficheiro, i, "o conteúdo do prefixo não corresponde à impressão"
                        + " digital registada (calculado " + real + ")");
            }
        }
    }

    /** Lê exatamente os primeiros `fimValido` bytes do ficheiro. */
    private static byte[] lerPrefixo(Path ficheiro, long fimValido) throws IOException, CorrupcaoException {
        if (fimValido > MAX_PREFIXO_BYTES) {
            throw new CorrupcaoException("fimValido=" + fimValido + " excede o limite de validação ("
                    + MAX_PREFIXO_BYTES + " bytes) em " + ficheiro.toAbsolutePath()
                    + "; a reparação não pode ser validada em memória.");
        }

        byte[] buf = new byte[(int) fimValido];
        try (FileChannel ch = FileChannel.open(ficheiro, StandardOpenOption.READ)) {
            ByteBuffer bb = ByteBuffer.wrap(buf);
            while (bb.hasRemaining()) {
                if (ch.read(bb) <= 0) {                  // EOF inesperado: o ficheiro encolheu
                    throw new CorrupcaoException("O ficheiro " + ficheiro.toAbsolutePath()
                            + " encolheu durante a validação do prefixo (esperava " + fimValido + " bytes).");
                }
            }
        }
        return buf;
    }

    /** SHA-256 em hexadecimal dos primeiros `len` bytes. */
    static String sha256Hex(byte[] dados, int len) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(dados, 0, len);
            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponível nesta JVM", e);
        }
    }

    // ------------------------------------------------------------------
    // Diário de eventos: validação e deduplicação
    // ------------------------------------------------------------------

    /**
     * Valida as linhas completas de `eventos-lider.txt` SEM alterar o ficheiro.
     *
     * Aplica ao diário a mesma regra do log: uma linha terminada por '\n' tem de
     * ser uma linha válida (`instante;evento;detalhe`, com instante parseável e
     * evento não vazio). Se não for, o arranque aborta e o diário fica intacto.
     *
     * A cauda parcial (sem '\n') não é validada aqui — é escrita parcial e será
     * cortada depois, por quem já sabe que as linhas completas são válidas.
     */
    static void validarEventos(Path pasta) throws IOException, CorrupcaoException {
        byte[] dados = lerBytesOpcional(pasta.resolve(FICHEIRO_EVENTOS));
        validarLinhasEventos(dados, fimValidoDe(dados));
    }

    /** Valida as linhas completas de `[0, fimValido)` do diário de eventos. */
    private static void validarLinhasEventos(byte[] dados, int fimValido) throws CorrupcaoException {
        if (dados == null) return;

        int inicio = 0;
        int numero = 0;
        while (inicio < fimValido) {
            int fim = inicio;
            while (fim < fimValido && dados[fim] != '\n') fim++;

            numero++;
            String linha = new String(dados, inicio, fim - inicio, StandardCharsets.UTF_8);
            if (linha.endsWith("\r")) linha = linha.substring(0, linha.length() - 1);  // tolerância CRLF
            validarLinhaEvento(linha, numero);

            inicio = fim + 1;
        }
    }

    /** Valida uma linha do diário de eventos (sem o '\n'): 3 campos, instante e evento. */
    private static void validarLinhaEvento(String linha, int numero) throws CorrupcaoException {
        // split com limite 3: o detalhe pode conter ';'.
        String[] campos = linha.split(";", NUM_CAMPOS_EVENTO);

        if (campos.length < NUM_CAMPOS_EVENTO) {
            throw new CorrupcaoException("eventos-lider.txt, linha " + numero
                    + ": esperava instante;evento;detalhe (" + NUM_CAMPOS_EVENTO + " campos), encontrei "
                    + campos.length + ". A linha está terminada por '\\n', portanto não é uma escrita"
                    + " parcial: é corrupção. Arranque abortado e diário preservado.");
        }
        try {
            Instant.parse(campos[0].trim());
        } catch (DateTimeParseException e) {
            throw new CorrupcaoException("eventos-lider.txt, linha " + numero
                    + ": instante não parseável ('" + campos[0] + "')."
                    + " A linha está terminada por '\\n', portanto não é uma escrita parcial:"
                    + " é corrupção. Arranque abortado e diário preservado.");
        }
        if (campos[1].isBlank()) {
            throw new CorrupcaoException("eventos-lider.txt, linha " + numero
                    + ": evento vazio. Arranque abortado e diário preservado.");
        }
    }

    /**
     * True se o diário já tiver a evidência desta reparação.
     *
     * A deduplicação é ESTRITA: a linha tem de ser válida, o campo `evento` tem de
     * ser exatamente `REPARACAO` e o `detalhe` tem de conter um sub-campo
     * exatamente igual a `intent=<intentId>`. Procurar `intent=<id>` como substring
     * em qualquer linha suprimia a evidência (uma linha de outro tipo, ou um id que
     * é prefixo de outro, contariam como prova).
     */
    private static boolean eventoJaEmitido(Path ficheiroEventos, String intentId) throws IOException {
        if (!Files.exists(ficheiroEventos)) return false;

        for (String linha : Files.readAllLines(ficheiroEventos, StandardCharsets.UTF_8)) {
            String[] campos = linha.split(";", NUM_CAMPOS_EVENTO);
            if (campos.length < NUM_CAMPOS_EVENTO) continue;          // linha inválida: não é prova
            if (!EVENTO_REPARACAO.equals(campos[1].trim())) continue; // outro tipo de evento: não é prova
            if (mencionaIntencao(campos[2], intentId)) return true;
        }
        return false;
    }

    /** True se `detalhe` tiver um sub-campo (separado por ';') exatamente `intent=<intentId>`. */
    private static boolean mencionaIntencao(String detalhe, String intentId) {
        String alvo = "intent=" + intentId;
        for (String parte : detalhe.split(";")) {
            if (parte.trim().equals(alvo)) return true;
        }
        return false;
    }

    /**
     * Acrescenta uma linha a `eventos-lider.txt` e força-a para o disco.
     *
     * Não passa pelo objeto {@link Eventos} de propósito: a evidência da
     * reparação tem de ser escrita mesmo quando o chamador não tem um `Eventos`
     * (arranques de teste) e tem de ficar durável antes de a intenção ser apagada.
     */
    private static void anexarEvento(Path ficheiroEventos, String evento, String detalhe) throws IOException {
        String linha = Instant.now() + ";" + evento + ";"
                + (detalhe == null ? "" : detalhe.replace('\n', ' ').replace('\r', ' ')) + "\n";

        try (FileChannel ch = FileChannel.open(ficheiroEventos, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer buf = ByteBuffer.wrap(linha.getBytes(StandardCharsets.UTF_8));
            while (buf.hasRemaining()) ch.write(buf);
            ch.force(true);
        }
    }

    /** True se o intervalo [de, ate) contiver um '\n'. Lê em blocos, sem carregar tudo. */
    private static boolean contemTerminador(Path ficheiro, long de, long ate) throws IOException {
        try (FileChannel ch = FileChannel.open(ficheiro, StandardOpenOption.READ)) {
            ch.position(de);
            ByteBuffer buf = ByteBuffer.allocate(BLOCO_LEITURA);
            long restantes = ate - de;

            while (restantes > 0) {
                buf.clear();
                if (restantes < buf.capacity()) buf.limit((int) restantes);
                int lidos = ch.read(buf);
                if (lidos <= 0) break;
                for (int k = 0; k < lidos; k++) {
                    if (buf.get(k) == '\n') return true;
                }
                restantes -= lidos;
            }
        }
        return false;
    }

    /** Corta o ficheiro em `tamanho` bytes e força-o para o disco. */
    private static void truncar(Path ficheiro, long tamanho) throws IOException {
        try (FileChannel ch = FileChannel.open(ficheiro, StandardOpenOption.WRITE)) {
            ch.truncate(tamanho);
            ch.force(true);   // a reparação também tem de sobreviver a outro crash
        }
    }

    // ------------------------------------------------------------------
    // Varrimento / validação
    // ------------------------------------------------------------------

    /**
     * Resultado de varrer o ficheiro: último LSN válido, offset após o último
     * '\n' de uma linha aceite, e os registos lidos.
     */
    private record Varrimento(long ultimoLsn, int fimValido, List<Registo> registos) {}

    private static Varrimento varrer(byte[] dados) throws CorrupcaoException {
        long ultimo = 0;
        int inicio = 0;      // offset do primeiro byte da linha atual
        int numero = 0;      // número de linha, só para as mensagens de erro
        List<Registo> registos = new ArrayList<>();

        while (true) {
            // 1. Procura o terminador da próxima linha.
            int fim = -1;
            for (int i = inicio; i < dados.length; i++) {
                if (dados[i] == '\n') { fim = i; break; }
            }
            if (fim < 0) break;   // cauda sem '\n' -> escrita parcial; tratada em abrir()

            // 2. Linha terminada: é suposto estar completa e válida.
            numero++;
            String linha = new String(dados, inicio, fim - inicio, StandardCharsets.UTF_8);
            if (linha.endsWith("\r")) linha = linha.substring(0, linha.length() - 1); // tolerância CRLF

            Registo r;
            try {
                r = analisar(linha, numero);
            } catch (CorrupcaoException e) {
                // O '\n' chegou ao disco => o resto também chegou => não é escrita parcial.
                // Pode ser um LSN já confirmado ao utilizador: truncar criaria um salto.
                throw new CorrupcaoException(e.getMessage()
                        + " [offset " + inicio + ", linha " + numero + "]. A linha está terminada por '\\n',"
                        + " portanto não pode ser uma escrita parcial: é corrupção."
                        + " Arranque abortado para não haver repetições nem saltos de LSN.");
            }

            // 3. Continuidade dos LSN (a linha 1 tem de ter lsn=1).
            if (r.lsn() != ultimo + 1) {
                throw new CorrupcaoException("Linha " + numero + " [offset " + inicio + "]: LSN " + r.lsn()
                        + " fora de sequência (era esperado " + (ultimo + 1) + ").");
            }

            ultimo = r.lsn();
            registos.add(r);
            inicio = fim + 1;
        }
        return new Varrimento(ultimo, inicio, List.copyOf(registos));
    }

    /** Valida e converte uma linha (sem o '\n') num Registo. */
    private static Registo analisar(String linha, int numero) throws CorrupcaoException {
        // -1 preserva campos vazios à direita (texto vazio -> Base64 vazio).
        String[] campos = linha.split(";", -1);
        if (campos.length != NUM_CAMPOS) {
            throw new CorrupcaoException("Linha " + numero + ": esperava " + NUM_CAMPOS
                    + " campos, encontrei " + campos.length);
        }

        long lsn;
        try {
            lsn = Long.parseLong(campos[0]);
        } catch (NumberFormatException e) {
            throw new CorrupcaoException("Linha " + numero + ": LSN não numérico ('" + campos[0] + "')");
        }
        if (lsn < 1) throw new CorrupcaoException("Linha " + numero + ": LSN inválido (" + lsn + ")");

        Instant instante;
        try {
            instante = Instant.parse(campos[1]);
        } catch (DateTimeParseException e) {
            throw new CorrupcaoException("Linha " + numero + ": instante não parseável ('" + campos[1] + "')");
        }

        if (campos[2].isEmpty()) throw new CorrupcaoException("Linha " + numero + ": autor vazio");
        if (campos[3].isEmpty()) throw new CorrupcaoException("Linha " + numero + ": canal vazio");

        byte[] texto;
        try {
            // getDecoder() é estrito: rejeita caracteres fora do alfabeto Base64.
            texto = Base64.getDecoder().decode(campos[4]);
        } catch (IllegalArgumentException e) {
            throw new CorrupcaoException("Linha " + numero + ": Base64 inválido no 5.º campo");
        }

        return new Registo(lsn, instante, campos[2], campos[3],
                new String(texto, StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------
    // Escrita
    // ------------------------------------------------------------------

    /**
     * Atribui o LSN seguinte, acrescenta a linha a `registos.log`, força-a para o
     * disco e devolve o registo completo (com o instante que ficou no ficheiro).
     * É este objeto que entra no chunk aberto, para que log e chunk não divirjam.
     */
    public synchronized Registo acrescentarRegisto(String autor, String canalDiscord, String texto)
            throws IOException {
        verificarNaoBloqueado();

        long lsn = ultimoLsn + 1;
        Instant instante = Instant.now();
        byte[] linha = formatar(lsn, instante, autor, canalDiscord, texto);

        long posicaoInicial = canal.tamanho();
        try {
            canal.escrever(ByteBuffer.wrap(linha));
            canal.forcar();
        } catch (IOException e) {
            reporEstadoAnterior(posicaoInicial, e);
            throw e;
        }

        ultimoLsn = lsn;
        if (eventos != null) {
            eventos.registar("REGISTO", "lsn=" + lsn + " autor=" + autor
                    + " canal=" + canalDiscord + " bytes=" + linha.length);
        }
        // Os campos autor/canal do Registo são os ORIGINAIS (o log é que os sanitiza);
        // a garantia de não conter ';' ou '\n' é do formato binário do chunk, que é
        // length-prefixed.
        return new Registo(lsn, instante, autor, canalDiscord, texto);
    }

    /** Compatibilidade: devolve apenas o LSN atribuído. */
    public synchronized long acrescentar(String autor, String canalDiscord, String texto) throws IOException {
        return acrescentarRegisto(autor, canalDiscord, texto).lsn();
    }

    /**
     * Tenta repor o ficheiro no estado anterior à escrita falhada.
     *
     * Se o rollback também falhar — na truncagem OU no `force` que a torna
     * durável — a instância fica BLOQUEADA: o ficheiro pode ter uma cauda
     * inválida e, sem saber onde acaba, nenhuma escrita seguinte consegue
     * preservar a semântica de prefixo. A cauda é reparada no próximo arranque,
     * com a intenção durável.
     */
    private void reporEstadoAnterior(long posicaoInicial, IOException causa) {
        try {
            canal.truncar(posicaoInicial);
            canal.forcar();                          // a reposição também tem de ser durável
        } catch (IOException falhaReposicao) {
            causa.addSuppressed(falhaReposicao);
            bloqueado = true;
            motivoBloqueio = "a escrita falhou (" + causa.getMessage()
                    + ") e o rollback também (" + falhaReposicao.getMessage() + ")";
            System.err.println("ERRO CRÍTICO: " + motivoBloqueio + ".");
            System.err.println("O log de " + pasta.toAbsolutePath() + " fica BLOQUEADO neste processo:"
                    + " nenhum registo novo será escrito. Reinicie o líder para reparar a cauda.");
        }
    }

    /** Recusa escritas numa instância bloqueada (fail-stop). */
    private void verificarNaoBloqueado() throws IOException {
        if (bloqueado) {
            throw new IOException("Log bloqueado (fail-stop) em " + pasta.toAbsolutePath()
                    + ": " + motivoBloqueio + ". Nenhum registo novo é escrito neste processo;"
                    + " reinicie o líder para reparar a cauda de registos.log.");
        }
    }

    /** Serializa um registo na linha do log, incluindo o '\n' final. */
    static byte[] formatar(long lsn, Instant instante, String autor, String canal, String texto) {
        String base64 = Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
        String linha = lsn + "" + SEP + instante + SEP + limpar(autor) + SEP + limpar(canal)
                + SEP + base64 + "\n";
        return linha.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Neutraliza os caracteres de autor/canal que quebrariam o formato:
     * ';' (separador), CR/LF (fim de linha) e caracteres de controlo.
     *
     * Sem isto, um nome de utilizador com ';' geraria uma linha com 6 campos,
     * que a nossa própria recuperação classificaria como corrupção — ou seja,
     * o escritor estaria a corromper o log.
     */
    private static String limpar(String campo) {
        if (campo == null || campo.isEmpty()) return "_";
        StringBuilder sb = new StringBuilder(campo.length());
        for (int i = 0; i < campo.length(); i++) {
            char c = campo.charAt(i);
            sb.append(c == SEP || c == '\n' || c == '\r' || Character.isISOControl(c) ? '_' : c);
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------
    // Leitura (reutilizável por /recuperar no próximo sprint)
    // ------------------------------------------------------------------

    /** Registos válidos do ficheiro; uma cauda sem '\n' é ignorada (não truncada). */
    public static List<Registo> lerTodos(Path pasta) throws IOException, CorrupcaoException {
        Path ficheiro = pasta.resolve(FICHEIRO);
        if (!Files.exists(ficheiro)) return List.of();
        return varrer(Files.readAllBytes(ficheiro)).registos();
    }

    public synchronized long ultimoLsn() { return ultimoLsn; }

    /**
     * Bytes de escrita parcial descartados pela reparação concluída neste arranque
     * (0 se não houve nenhuma). Inclui uma intenção pendente de um arranque
     * anterior, mesmo que a truncagem já tivesse sido feita antes do crash: a
     * reparação só é "concluída" quando a evidência fica emitida e a intenção
     * desaparece, e é isso que este valor reporta.
     */
    public long bytesDescartadosNaRecuperacao() { return bytesDescartados; }

    /** True se a instância entrou em fail-stop (escrita falhada + rollback falhado). */
    public boolean estaBloqueado() { return bloqueado; }

    @Override
    public void close() throws IOException {
        canal.close();
    }
}