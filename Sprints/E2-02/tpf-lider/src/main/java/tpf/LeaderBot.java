package tpf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionMapping;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.CommandData;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;
import net.dv8tion.jda.api.utils.MarkdownSanitizer;

/**
 * Líder do arquivo distribuído (TPF) — Sprint E2-02: escrita local durável.
 *
 * Fluxo de um registo (único para /registar, #arquivo e /rajada), com DUAS fases
 * e semânticas de erro DIFERENTES:
 *
 *   FASE 1 — log (fonte de verdade): LSN + linha em registos.log com fsync.
 *            Se falhar, NADA foi gravado e o utilizador é informado; repetir é seguro.
 *
 *   FASE 2 — chunk (artefacto derivado): entra no chunk aberto, que pode selar.
 *            Se falhar, o registo CONTINUA durável e o LSN é definitivo: a resposta
 *            ao utilizador é o sucesso, e a falha só fica em eventos-lider.txt como
 *            ERRO, com o chunk aberto abandonado (reconstruível a partir do log).
 *
 * Só a IOException da fase 1 chega ao chamador. É isso que impede o duplicado:
 * o utilizador nunca é convidado a repetir um registo que já está no log.
 *
 * Texto: `isBlank()` VALIDA e `trim()` MUTA. Só se apara o que é token (ex.: o id
 * de /replica-remover); o texto de /registar é guardado exatamente como submetido.
 */
public final class LeaderBot extends ListenerAdapter {

    static final String CANAL_ARQUIVO = "arquivo";
    static final String PAPEL_ADMIN = "admin-arquivo";

    static final Set<String> COMANDOS_ADMIN =
            Set.of("config", "replica-token", "replica-remover", "rajada");

    static final Pattern ID_REPLICA = Pattern.compile("[A-Za-z0-9_-]{1,32}");
    static final int MAX_ECO = 200;
    static final int MAX_RAJADA = 5000;

    // ------------------------------------------------------------------
    // Estado do líder
    // ------------------------------------------------------------------

    private final Eventos eventos;
    private final LogRegistos log;
    private final byte[] chaveMestra;      // 32 bytes; usada para a DEK de cada chunk
    private final Path pastaDados;
    private final long limiteChunk;        // bytes em claro que provocam a selagem
    private final ScheduledExecutorService agendador;

    // Chunk aberto e o temporizador de selagem. Ambos só são tocados dentro de
    // métodos synchronized nesta instância (o agendador entra pelo mesmo lock).
    private Chunk chunkAberto;
    private ScheduledFuture<?> selagemAgendada;

    private LeaderBot(Eventos eventos, LogRegistos log, byte[] chaveMestra, Path pastaDados) {
        this(eventos, log, chaveMestra, pastaDados, Wire.CHUNK_MAX_BYTES);
    }

    private LeaderBot(Eventos eventos, LogRegistos log, byte[] chaveMestra, Path pastaDados,
                      long limiteChunk) {
        if (limiteChunk <= 0) throw new IllegalArgumentException("limiteChunk tem de ser > 0");
        this.eventos = eventos;
        this.log = log;
        this.chaveMestra = chaveMestra.clone();
        this.pastaDados = pastaDados;
        this.limiteChunk = limiteChunk;
        this.agendador = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tpf-agendador");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Instância com dependências injetadas, para testes que não precisam de Discord:
     * não arranca JDA nem regista comandos. Reservado a testes (mesmo padrão de
     * {@link LogRegistos#paraTestes}).
     */
    static LeaderBot paraTestes(Eventos eventos, LogRegistos log, byte[] chaveMestra, Path pastaDados) {
        return new LeaderBot(eventos, log, chaveMestra, pastaDados);
    }

    /**
     * Como acima, mas com o limite de chunk injectado. Existe porque
     * `Wire.CHUNK_MAX_BYTES` é `static final` e é lido uma única vez por JVM: sem este
     * parâmetro, um teste não consegue provocar vários chunks sem dormir por megabytes.
     */
    static LeaderBot paraTestes(Eventos eventos, LogRegistos log, byte[] chaveMestra, Path pastaDados,
                                long limiteChunk) {
        return new LeaderBot(eventos, log, chaveMestra, pastaDados, limiteChunk);
    }
    // ------------------------------------------------------------------
    // Arranque
    // ------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        String token = lerToken();
        String guildId = System.getProperty("tpf.guild", System.getenv("TPF_GUILD_ID"));
        if (guildId == null || guildId.isBlank()) {
            System.err.println("Falta o id do servidor: use -Dtpf.guild=<id> ou defina TPF_GUILD_ID.");
            System.exit(1);
        }

        // 1. Raiz de confiança (fail-fast, antes de qualquer socket)
        Path caminhoChave = Path.of(System.getProperty("tpf.masterKey", "master.key"));
        byte[] masterKey;
        try {
            masterKey = Wire.carregarOuCriarMasterKey(caminhoChave);
        } catch (Wire.MasterKeyException e) {
            System.err.println("ERRO FATAL NO ARRANQUE: " + e.getMessage());
            System.exit(1);
            return;
        }

        // 2. Recuperação do log (fail-fast: cauda parcial reparada, corrupção aborta)
        Path pastaDados = Path.of(System.getProperty("tpf.pasta", "dados-lider"));
        Eventos eventos;
        LogRegistos log;
        try {
            eventos = new Eventos(pastaDados);
            log = LogRegistos.abrir(pastaDados, eventos);
        } catch (IOException e) {
            System.err.println("ERRO FATAL NO ARRANQUE: não foi possível abrir "
                    + pastaDados.toAbsolutePath() + ": " + e.getMessage());
            System.exit(1);
            return;
        } catch (LogRegistos.CorrupcaoException e) {
            System.err.println("ERRO FATAL NO ARRANQUE: registos.log inválido: " + e.getMessage());
            System.exit(1);
            return;
        }

        LeaderBot lider = new LeaderBot(eventos, log, masterKey, pastaDados);
        Runtime.getRuntime().addShutdownHook(new Thread(lider::fechar, "tpf-shutdown"));

        lider.registarEvento("ARRANQUE", "master.key validada (" + masterKey.length + " bytes)");
        lider.registarEvento("ARRANQUE", "registos.log recuperado: último LSN=" + log.ultimoLsn()
                + (log.bytesDescartadosNaRecuperacao() > 0
                ? ", " + log.bytesDescartadosNaRecuperacao() + " bytes de escrita parcial descartados"
                : ", sem reparações"));

        // 3. Discord (só com chave e log válidos)
        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.GUILD_MESSAGES, GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(lider)
                .build();

        jda.awaitReady();

        Guild guild = jda.getGuildById(guildId.trim());
        if (guild == null) {
            System.err.println("O bot não pertence ao servidor " + guildId + ". Use o URL de convite do README.");
            lider.fechar();
            jda.shutdown();
            System.exit(1);
        }

        guild.updateCommands()
                .addCommands(definirComandos())
                .queue(
                        cmds -> lider.registarEvento("COMANDOS",
                                cmds.size() + " comandos registados em " + guild.getName()),
                        erro -> lider.registarEvento("ERRO",
                                "falha ao registar os comandos: " + erro.getMessage()));

        lider.registarEvento("ARRANQUE", "líder online como " + jda.getSelfUser().getName());
    }

    static String lerToken() throws IOException {
        String env = System.getenv("DISCORD_TOKEN");
        if (env != null && !env.isBlank()) return env.trim();

        Path ficheiro = Path.of(System.getProperty("tpf.tokenFile", "discord.token"));
        if (Files.isReadable(ficheiro)) return Files.readString(ficheiro).trim();

        System.err.println("Token não encontrado: defina DISCORD_TOKEN ou crie o ficheiro " + ficheiro);
        System.exit(1);
        return null;
    }

    static List<CommandData> definirComandos() {
        return List.of(
                Commands.slash("registar", "Regista um texto no arquivo")
                        .addOption(OptionType.STRING, "texto", "Texto a registar", true),

                Commands.slash("estado", "Mostra o estado das réplicas e o número de chunks subreplicados"),

                Commands.slash("config", "Altera a configuração do arquivo (admin-arquivo)")
                        .addSubcommands(new SubcommandData("r", "Altera o fator de replicação R")
                                .addOption(OptionType.INTEGER, "n", "Novo valor de R (inteiro ≥ 1)", true)),

                Commands.slash("recuperar", "Recupera os registos de um intervalo de LSN")
                        .addOption(OptionType.INTEGER, "de", "Primeiro LSN (inteiro ≥ 1)", true)
                        .addOption(OptionType.INTEGER, "ate", "Último LSN (inteiro ≥ de)", true),

                Commands.slash("replica-token", "Gera um token de registo de réplica (admin-arquivo)"),

                Commands.slash("replica-remover", "Remove uma réplica registada (admin-arquivo)")
                        .addOption(OptionType.STRING, "id", "Identificador da réplica", true),

                Commands.slash("rajada", "Injeta n registos sintéticos (admin-arquivo)")
                        .addOption(OptionType.INTEGER, "n", "Número de registos (inteiro ≥ 1)", true)
                        .addOption(OptionType.INTEGER, "intervalo_ms", "Intervalo entre registos em ms (inteiro ≥ 0)", true));
    }

    // ------------------------------------------------------------------
    // Caminho ÚNICO de escrita — duas fases
    // ------------------------------------------------------------------

    /**
     * FASE 1 (log) + FASE 2 (chunk), sob o mesmo lock.
     *
     * Contrato de erro, e é isto que evita duplicados:
     *  - `IOException` propagada  -> NADA foi gravado; o utilizador pode repetir;
     *  - retorno normal           -> o registo está durável no log e o LSN é definitivo,
     *                                mesmo que a fase 2 tenha falhado por dentro.
     */
    synchronized Registo registar(String autor, String canal, String texto) throws IOException {
        // Fase 1: fonte de verdade. Se isto falhar, nada foi escrito e o erro sobe.
        Registo registo = log.acrescentarRegisto(autor, canal, texto);

        // Fase 2: artefacto derivado. Nunca propaga — o registo já está confirmado.
        entrarNoChunk(registo);

        return registo;
    }

    /** Acrescenta ao chunk aberto e sela se o limite tiver sido atingido. */
    private void adicionarAoChunk(Registo registo) throws IOException {
        if (chunkAberto == null) {
            chunkAberto = Chunk.novo(registo);
            agendarSelagemPorTempo();
        } else {
            chunkAberto.adicionar(registo);
        }

        // "O registo que faz o chunk exceder o limite sela-o de imediato": o registo
        // entra primeiro e só depois se sela — nunca se perde o que provocou o limite.
        if (chunkAberto.bytes() >= limiteChunk) {
            selarChunkAberto();
        }
    }

    /**
     * Fase 2 isolada: uma falha do chunk NÃO invalida um registo já durável.
     *
     * O chunk aberto é abandonado (fica em memória, os registos ficam no log) para
     * que a falha não se propague a todos os registos seguintes, e o intervalo de LSN
     * afetado fica registado como ERRO — a evidência de que existe trabalho derivado
     * em falta. Reconstruir chunks a partir do log é o sprint seguinte.
     */
    private void entrarNoChunk(Registo registo) {
        try {
            adicionarAoChunk(registo);
        } catch (IOException | RuntimeException e) {
            Chunk abandonado = chunkAberto;
            chunkAberto = null;
            cancelarSelagemAgendada();

            String intervalo = (abandonado == null)
                    ? "?"
                    : abandonado.lsnMinimo() + "-" + abandonado.lsnMaximo();

            registarEvento("ERRO", "chunk abandonado (lsn " + intervalo + "): registo lsn="
                    + registo.lsn() + " está durável em registos.log mas não entrou no chunk ("
                    + e.getMessage() + "); o chunk é reconstruível a partir do log");
            System.err.println("AVISO: registo lsn=" + registo.lsn()
                    + " está durável; a falha foi apenas no chunk derivado.");
        }
    }

    /** Agenda a selagem por tempo, contada a partir do PRIMEIRO registo do chunk. */
    private void agendarSelagemPorTempo() {
        cancelarSelagemAgendada();
        final Chunk alvo = chunkAberto;
        if (alvo == null) return;

        selagemAgendada = agendador.schedule(() -> selarPorTempo(alvo),
                Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
    }

    /** Callback do agendador: sela só se o chunk aberto ainda for o mesmo. */
    private synchronized void selarPorTempo(Chunk alvo) {
        if (chunkAberto != alvo || alvo.vazio()) return;   // já foi selado por tamanho
        try {
            selarChunkAberto();
        } catch (IOException e) {
            // A selagem por tempo é derivada: falha aqui não afeta nenhum registo durável.
            registarEvento("ERRO", "selagem por tempo falhou (lsn " + alvo.lsnMinimo()
                    + "-" + alvo.lsnMaximo() + "): " + e.getMessage());
            chunkAberto = null;
            cancelarSelagemAgendada();
        }
    }

    private void cancelarSelagemAgendada() {
        if (selagemAgendada != null) {
            selagemAgendada.cancel(false);
            selagemAgendada = null;
        }
    }

    /**
     * Sela o chunk aberto: padding, cifra, escrita em `selados/` e evento SELADO.
     * O chunk só é descartado DEPOIS de estar gravado: uma falha aqui deixa-o
     * aberto (os registos continuam duráveis em registos.log).
     */
    private void selarChunkAberto() throws IOException {
        Chunk c = chunkAberto;
        if (c == null || c.vazio()) return;

        byte[] claro = c.serializarComPadding();          // múltiplo de Wire.PAD_BYTES

        ChunkCrypto.Cifrado cifrado;
        try {
            cifrado = ChunkCrypto.cifrar(chaveMestra, claro);
        } catch (ChunkCrypto.CifraException e) {
            throw new IOException("falha ao cifrar o chunk lsn=" + c.lsnMinimo() + "-"
                    + c.lsnMaximo() + ": " + e.getMessage(), e);
        }

        Sealed selado = Sealed.gravar(pastaDados, cifrado);

        chunkAberto = null;
        cancelarSelagemAgendada();

        registarEvento("SELADO", "chunkId=" + selado.chunkId()
                + " lsn=" + c.lsnMinimo() + "-" + c.lsnMaximo()
                + " bytes=" + claro.length                      // texto em claro, múltiplo de PAD_BYTES
                + " cifrados=" + selado.tamanhoCifrado());      // .bin = claro + 16 (tag GCM)
    }

    // ------------------------------------------------------------------
    // Slash commands
    // ------------------------------------------------------------------

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent ev) {
        if (!CANAL_ARQUIVO.equals(ev.getChannel().getName())) {
            //recusar(ev, "Este comando só pode ser usado no canal #" + CANAL_ARQUIVO + ".");
            return;
        }

        String nome = ev.getName();
        String autor = ev.getUser().getName();
        registarEvento("COMANDO", "/" + nome + " de " + autor);

        if (COMANDOS_ADMIN.contains(nome) && !eAdmin(ev.getMember())) {
            registarEvento("RECUSADO", "/" + nome + " de " + autor + " sem o papel " + PAPEL_ADMIN);
            recusar(ev, "Não tem permissão para usar `/" + nome + "`: é preciso o papel `" + PAPEL_ADMIN + "`.");
            return;
        }

        try {
            switch (nome) {
                case "registar"        -> cmdRegistar(ev);
                case "estado"          -> cmdEstado(ev);
                case "config"          -> cmdConfig(ev);
                case "recuperar"       -> cmdRecuperar(ev);
                case "replica-token"   -> cmdReplicaToken(ev);
                case "replica-remover" -> cmdReplicaRemover(ev);
                case "rajada"          -> cmdRajada(ev);
                default                -> recusar(ev, "Comando desconhecido: `/" + nome + "`.");
            }
        } catch (ArgumentoInvalido e) {
            registarEvento("INVALIDO", "/" + nome + ": " + e.getMessage());
            recusar(ev, e.getMessage());
        } catch (IOException e) {
            // Só chega aqui uma falha da FASE 1 (log): o registo NÃO existe.
            registarEvento("ERRO", "/" + nome + ": o registo NÃO foi gravado: " + e.getMessage());
            recusar(ev, log.estaBloqueado()
                    ? "❌ O registo **não** foi gravado: o log entrou em fail-stop depois de uma"
                    + " falha de escrita. É preciso reiniciar o líder para reparar a cauda de"
                    + " `registos.log`."
                    : "❌ O registo **não** foi gravado (falha de I/O no log). Nada ficou"
                    + " registado, por isso podes repetir sem duplicar.");
        }
    }

    // /registar <texto>
    private void cmdRegistar(SlashCommandInteractionEvent ev) throws ArgumentoInvalido, IOException {
        // O texto é guardado TAL COMO FOI SUBMETIDO: espaços e mudanças de linha no
        // início/fim podem ser significativos (código, YAML, indentação). `isBlank()`
        // rejeita o vazio sem mutar o valor.
        String texto = textoOriginal(ev, "texto");

        // Se o registo voltar daqui, está durável — mesmo que o chunk tenha falhado
        // por dentro (a falha é derivada e fica registada como ERRO em eventos-lider.txt).
        Registo registo = registar(ev.getUser().getName(), CANAL_ARQUIVO, texto);
        responder(ev, "Registado com LSN **" + registo.lsn() + "**.");
    }

    // /estado
    private void cmdEstado(SlashCommandInteractionEvent ev) {
        responder(ev, "`/estado` — último LSN: " + log.ultimoLsn()
                + " · chunks selados: " + contarSelados()
                + " · " + estadoDoChunkAberto()
                + (log.estaBloqueado() ? " · log em fail-stop (reinicie o líder)" : "")
                + " — réplicas ainda não implementadas");
    }

    // /config r <n>
    private void cmdConfig(SlashCommandInteractionEvent ev) throws ArgumentoInvalido {
        if (!"r".equals(ev.getSubcommandName()))
            throw new ArgumentoInvalido("Subcomando desconhecido. Use `/config r <n>`.");
        long n = inteiro(ev, "n", 1, Integer.MAX_VALUE);
        responder(ev, "`/config r` recebido: n=" + n + " — ainda não implementado");
    }

    // /recuperar <de> <ate>
    private void cmdRecuperar(SlashCommandInteractionEvent ev) throws ArgumentoInvalido {
        long de = inteiro(ev, "de", 1, Long.MAX_VALUE);
        long ate = inteiro(ev, "ate", 1, Long.MAX_VALUE);
        if (de > ate)
            throw new ArgumentoInvalido("Intervalo inválido: `de` (" + de + ") tem de ser ≤ `ate` (" + ate + ").");
        responder(ev, "`/recuperar` recebido: de=" + de + ", ate=" + ate + " — ainda não implementado");
    }

    // /replica-token
    private void cmdReplicaToken(SlashCommandInteractionEvent ev) {
        responder(ev, "`/replica-token` recebido — ainda não implementado");
    }

    // /replica-remover <id>
    private void cmdReplicaRemover(SlashCommandInteractionEvent ev) throws ArgumentoInvalido {
        // Aqui o valor é um TOKEN, não conteúdo: aparar é normalização.
        String id = textoObrigatorio(ev, "id");
        if (!ID_REPLICA.matcher(id).matches())
            throw new ArgumentoInvalido("Identificador de réplica inválido: \"" + eco(id)
                    + "\" (use só letras, dígitos, `_` e `-`, até 32 caracteres).");
        responder(ev, "`/replica-remover` recebido: id=" + id + " — ainda não implementado");
    }

    // /rajada <n> <intervaloMs>
    private void cmdRajada(SlashCommandInteractionEvent ev) throws ArgumentoInvalido {
        long n = inteiro(ev, "n", 1, MAX_RAJADA);
        long intervalo = inteiro(ev, "intervalo_ms", 0, Integer.MAX_VALUE);
        String autor = ev.getUser().getName();

        ev.deferReply().queue(
                ok -> agendador.execute(() -> executarRajada(ev, autor, n, intervalo)),
                erro -> registarEvento("ERRO", "falha ao adiar a resposta de /rajada: " + erro.getMessage()));
    }

    /** n registos sintéticos pelo MESMO caminho de /registar (LSN + fsync + chunk). */
    private void executarRajada(SlashCommandInteractionEvent ev, String autor, long n, long intervalo) {
        try {
            long[] extremos = injetarRajada(autor, n, intervalo);
            ev.getHook().sendMessage("Rajada concluída: " + n + " registos, LSN " + extremos[0]
                    + " a " + extremos[1] + ".").setAllowedMentions(List.of()).queue();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            registarEvento("ERRO", "/rajada interrompida");
            ev.getHook().sendMessage("❌ Rajada interrompida.").setAllowedMentions(List.of()).queue();

        } catch (IOException e) {
            registarEvento("ERRO", "/rajada: o registo NÃO foi gravado: " + e.getMessage());
            ev.getHook().sendMessage("❌ Rajada interrompida: o próximo registo falhou no log"
                    + " e podes repetir.").setAllowedMentions(List.of()).queue();
        }
    }

    /**
     * Injeta `n` registos pelo MESMO caminho de `/registar` (LSN + fsync + chunk) e
     * devolve `{primeiroLsn, ultimoLsn}`.
     *
     * Separado do envio da resposta para poder ser exercitado por testes: a resposta
     * exige um evento do Discord, a injecção não.
     */
    long[] injetarRajada(String autor, long n, long intervalo) throws IOException, InterruptedException {
        long primeiro = -1;
        long ultimo = -1;
        for (long i = 1; i <= n; i++) {
            if (i > 1 && intervalo > 0) Thread.sleep(intervalo);
            long lsn = registar(autor, CANAL_ARQUIVO, "rajada " + i + "/" + n).lsn();
            if (primeiro < 0) primeiro = lsn;
            ultimo = lsn;
        }
        return new long[]{primeiro, ultimo};
    }

    // ------------------------------------------------------------------
    // Mensagens em #arquivo
    // ------------------------------------------------------------------

    @Override
    public void onMessageReceived(MessageReceivedEvent ev) {
        if (ev.getAuthor().isBot() || ev.isWebhookMessage()) return;
        if (!ev.isFromGuild()) return;
        if (!CANAL_ARQUIVO.equals(ev.getChannel().getName())) return;

        String autor = ev.getAuthor().getName();
        // getContentRaw() não apara nada: mesma semântica de /registar, pelo que o
        // mesmo texto produz o mesmo registo por qualquer das vias.
        String texto = ev.getMessage().getContentRaw();

        try {
            Registo registo = registar(autor, CANAL_ARQUIVO, texto);
            ev.getMessage()
                    .reply("Registado com LSN **" + registo.lsn() + "**.")
                    .mentionRepliedUser(false)
                    .setAllowedMentions(List.of())
                    .queue();
        } catch (IOException e) {
            registarEvento("ERRO", "#arquivo: o registo NÃO foi gravado: " + e.getMessage());
            ev.getMessage()
                    .reply(log.estaBloqueado()
                            ? "❌ Não foi possível registar: o log entrou em fail-stop."
                            + " O líder tem de ser reiniciado para reparar a cauda."
                            : "❌ A mensagem **não** foi registada. Podes repetir sem duplicar.")
                    .mentionRepliedUser(false)
                    .setAllowedMentions(List.of())
                    .queue();
        }
    }

    // ------------------------------------------------------------------
    // Auxiliares
    // ------------------------------------------------------------------

    static final class ArgumentoInvalido extends Exception {
        ArgumentoInvalido(String mensagem) { super(mensagem); }
    }

    static boolean eAdmin(Member membro) {
        return membro != null
                && membro.getRoles().stream().anyMatch(p -> p.getName().equalsIgnoreCase(PAPEL_ADMIN));
    }

    /** Valor que é um TOKEN (ex.: id de réplica): valida e apara. */
    static String textoObrigatorio(SlashCommandInteractionEvent ev, String nome) throws ArgumentoInvalido {
        return lerTexto(ev, nome, true);
    }

    /** Valor que é CONTEÚDO (ex.: texto de /registar): valida sem aparar. */
    static String textoOriginal(SlashCommandInteractionEvent ev, String nome) throws ArgumentoInvalido {
        return lerTexto(ev, nome, false);
    }

    private static String lerTexto(SlashCommandInteractionEvent ev, String nome, boolean aparar)
            throws ArgumentoInvalido {
        OptionMapping op = ev.getOption(nome);
        if (op == null) throw new ArgumentoInvalido("Falta o argumento `" + nome + "`.");
        return validarTexto(op.getAsString(), nome, aparar);
    }

    /**
     * Regra única de validação de texto, testável sem JDA.
     *
     * `isBlank()` VALIDA (rejeita o vazio e o só-espaços) e `trim()` MUTA — só se
     * aplica quando o valor é um token. Assim `"  olá  "` em /registar fica intacto,
     * mas `"  abc  "` em /replica-remover passa a `"abc"` antes da regex do id.
     */
    static String validarTexto(String valor, String nome, boolean aparar) throws ArgumentoInvalido {
        if (valor == null || valor.isBlank()) {
            throw new ArgumentoInvalido("O argumento `" + nome + "` não pode estar vazio.");
        }
        return aparar ? valor.trim() : valor;
    }

    static long inteiro(SlashCommandInteractionEvent ev, String nome, long min, long max) throws ArgumentoInvalido {
        OptionMapping op = ev.getOption(nome);
        if (op == null) throw new ArgumentoInvalido("Falta o argumento `" + nome + "`.");

        long v = op.getAsLong();
        if (v < min || v > max) {
            throw new ArgumentoInvalido("O argumento `" + nome + "` tem de ser >= " + min
                    + (max < Long.MAX_VALUE ? " e <= " + max : "") + " (recebido: " + v + ").");
        }
        return v;
    }

    static void responder(SlashCommandInteractionEvent ev, String texto) {
        ev.reply(texto).setAllowedMentions(List.of()).queue();
    }

    static void recusar(SlashCommandInteractionEvent ev, String texto) {
        if (ev.isAcknowledged()) {
            ev.getHook().sendMessage("❌ " + texto).setEphemeral(true).setAllowedMentions(List.of()).queue();
        } else {
            ev.reply("❌ " + texto).setEphemeral(true).setAllowedMentions(List.of()).queue();
        }
    }

    static String eco(String texto) {
        String t = texto.length() > MAX_ECO ? texto.substring(0, MAX_ECO) + "…" : texto;
        return MarkdownSanitizer.escape(t);
    }

    /** Descrição do chunk aberto, lida sob o lock (não bloqueia escritas por muito tempo). */
    private synchronized String estadoDoChunkAberto() {
        if (chunkAberto == null) return "chunk aberto: nenhum";
        return "chunk aberto: " + chunkAberto.tamanho() + " registos, "
                + chunkAberto.bytes() + "/" + Wire.CHUNK_MAX_BYTES + " bytes";
    }

    /** Quantos `.bin` existem em `selados/` (-1 se a pasta não puder ser lida). */
    private long contarSelados() {
        Path pasta = pastaDados.resolve(Sealed.PASTA);
        if (!Files.isDirectory(pasta)) return 0;
        try (Stream<Path> ficheiros = Files.list(pasta)) {
            return ficheiros.filter(p -> p.getFileName().toString().endsWith(Sealed.EXT_BIN)).count();
        } catch (IOException e) {
            return -1;
        }
    }

    private void registarEvento(String evento, String detalhe) {
        eventos.registar(evento, detalhe);
        System.out.println(evento + " " + detalhe);
    }

    /** Fecho limpo: sela o chunk pendente, fecha canais e o agendador. */
    synchronized void fechar() {
        try {
            selarChunkAberto();               // não perder o chunk aberto num shutdown limpo
        } catch (IOException e) {
            registarEvento("ERRO", "falha ao selar o chunk pendente no shutdown: " + e.getMessage());
        }
        agendador.shutdownNow();

        try {
            log.close();
        } catch (IOException ignorada) {
            // a terminar
        }
        try {
            eventos.close();
        } catch (IOException ignorada) {
            // a terminar
        }
    }
}