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
 * Fluxo de um registo (único para /registar, #arquivo e /rajada):
 *   1. LSN + linha em registos.log com fsync (fonte de verdade);
 *   2. entra no chunk aberto;
 *   3. o chunk é selado quando atinge Wire.CHUNK_MAX_BYTES ou Wire.SELAGEM_MS
 *      depois do primeiro registo; selar = padding + cifra + selados/<id>.bin|.hdr
 *      + evento SELADO.
 *
 * Só depois de (1) é que o utilizador recebe o LSN: o log é a fonte de verdade,
 * o chunk é derivado e re-sealável a partir dele num sprint futuro.
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
    private final ScheduledExecutorService agendador;

    // Chunk aberto e o temporizador de selagem. Ambos só são tocados dentro de
    // métodos synchronized nesta instância (o agendador entra pelo mesmo lock).
    private Chunk chunkAberto;
    private ScheduledFuture<?> selagemAgendada;

    private LeaderBot(Eventos eventos, LogRegistos log, byte[] chaveMestra, Path pastaDados) {
        this.eventos = eventos;
        this.log = log;
        this.chaveMestra = chaveMestra.clone();
        this.pastaDados = pastaDados;
        this.agendador = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tpf-agendador");
            t.setDaemon(true);
            return t;
        });
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
    // Caminho ÚNICO de escrita
    // ------------------------------------------------------------------

    /**
     * Registo + entrada no chunk aberto, sob o mesmo lock: a ordem dos LSN no log
     * e no chunk é a mesma, e nenhuma selagem concorre com um append.
     */
    private synchronized long registar(String autor, String canal, String texto) throws IOException {
        // 1. Fonte de verdade: LSN + linha durável. O Registo devolvido já tem os
        //    valores sanitizados que ficaram escritos, para o chunk não divergir do log.
        Registo registo = log.acrescentarRegisto(autor, canal, texto);

        // 2. Entra no chunk aberto (pode selar por tamanho).
        adicionarAoChunk(registo);

        return registo.lsn();
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
        if (chunkAberto.bytes() >= Wire.CHUNK_MAX_BYTES) {
            selarChunkAberto();
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
            registarEvento("ERRO", "selagem por tempo falhou (lsn " + alvo.lsnMinimo()
                    + "-" + alvo.lsnMaximo() + "): " + e.getMessage());
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
            registarEvento("ERRO", "falha ao cifrar o chunk lsn=" + c.lsnMinimo() + "-"
                    + c.lsnMaximo() + ": " + e.getMessage());
            throw new IOException("falha ao cifrar o chunk: " + e.getMessage(), e);
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
            recusar(ev, "Este comando só pode ser usado no canal #" + CANAL_ARQUIVO + ".");
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
            registarEvento("ERRO", "/" + nome + ": falha de I/O no log/chunk: " + e.getMessage());
            recusar(ev, log.estaBloqueado()
                    ? "O arquivo está temporariamente indisponível: o log entrou em fail-stop"
                    + " depois de uma falha de escrita. É preciso reiniciar o líder"
                    + " para reparar a cauda de registos.log."
                    : "Não foi possível gravar o registo. Tente novamente.");
        }
    }

    // /registar <texto>
    private void cmdRegistar(SlashCommandInteractionEvent ev) throws ArgumentoInvalido, IOException {
        // O texto é guardado TAL COMO FOI SUBMETIDO: espaços e mudanças de linha no
        // início/fim podem ser significativos (código, YAML, indentação). `isBlank()`
        // rejeita o vazio sem mutar o valor.
        String texto = textoOriginal(ev, "texto");
        long lsn = registar(ev.getUser().getName(), CANAL_ARQUIVO, texto);
        responder(ev, "Registado com LSN **" + lsn + "**.");
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
        long primeiro = -1;
        long ultimo = -1;
        try {
            for (long i = 1; i <= n; i++) {
                if (i > 1 && intervalo > 0) Thread.sleep(intervalo);
                long lsn = registar(autor, CANAL_ARQUIVO, "rajada " + i + "/" + n);
                if (primeiro < 0) primeiro = lsn;
                ultimo = lsn;
            }
            ev.getHook().sendMessage("Rajada concluída: " + n + " registos, LSN " + primeiro
                    + " a " + ultimo + ".").setAllowedMentions(List.of()).queue();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            registarEvento("ERRO", "/rajada interrompida");
            ev.getHook().sendMessage("❌ Rajada interrompida.").setAllowedMentions(List.of()).queue();

        } catch (IOException e) {
            registarEvento("ERRO", "/rajada: falha ao gravar: " + e.getMessage());
            long feitos = ultimo < 0 ? 0 : ultimo - primeiro + 1;
            ev.getHook().sendMessage("❌ Rajada interrompida depois de " + feitos + " registos.")
                    .setAllowedMentions(List.of()).queue();
        }
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
            long lsn = registar(autor, CANAL_ARQUIVO, texto);
            ev.getMessage()
                    .reply("Registado com LSN **" + lsn + "**.")
                    .mentionRepliedUser(false)
                    .setAllowedMentions(List.of())
                    .queue();
        } catch (IOException e) {
            registarEvento("ERRO", "#arquivo: falha ao gravar a mensagem de " + autor + ": " + e.getMessage());
            ev.getMessage()
                    .reply(log.estaBloqueado()
                            ? "❌ Não foi possível registar: o log entrou em fail-stop."
                            + " O líder tem de ser reiniciado para reparar a cauda."
                            : "❌ Não foi possível registar a mensagem. Tente novamente.")
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
    private synchronized void fechar() {
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