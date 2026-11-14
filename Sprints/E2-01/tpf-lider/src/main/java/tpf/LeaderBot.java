package tpf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

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
 * Líder do arquivo distribuído (TPF) — Sprint 1: base do bot Discord.
 *
 * Neste sprint o LeaderBot apenas:
 *   - regista os 7 slash commands no servidor de teste (guild commands);
 *   - identifica cada comando, lê e valida os argumentos e responde com uma
 *     mensagem provisória que ecoa os argumentos recebidos;
 *   - restringe os comandos de administração ao papel admin-arquivo;
 *   - deteta as mensagens enviadas no canal #arquivo (ignorando bots).
 *
 * Nenhuma funcionalidade do arquivo (registo, cifra, réplicas, recuperação)
 * está implementada: os métodos cmdXxx são os pontos onde, nos próximos
 * sprints, se ligará o resto do líder.
 */
public final class LeaderBot extends ListenerAdapter {

    // nome do canal cujas mensagens são registadas (sem o '#')
    static final String CANAL_ARQUIVO = "arquivo";

    // papel exigido para os comandos de configuração e de réplicas
    static final String PAPEL_ADMIN = "admin-arquivo";

    // comandos que só membros com o papel admin-arquivo podem executar
    static final Set<String> COMANDOS_ADMIN =
            Set.of("config", "replica-token", "replica-remover", "rajada");

    // identificador de réplica aceite: letras, dígitos, '_' e '-', até 32 caracteres
    // (o id é usado no tópico tpf-r-<id> e na pasta dados-<id>, por isso não pode ter ';', '/', espaços...)
    static final Pattern ID_REPLICA = Pattern.compile("[A-Za-z0-9_-]{1,32}");

    // tamanho máximo do texto ecoado nas respostas (o Discord limita cada mensagem a 2000 caracteres)
    static final int MAX_ECO = 200;

    // ------------------------------------------------------------------
    // Arranque
    // ------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        // o token nunca está no código nem no repositório (ver lerToken)
        String token = lerToken();

        // id do servidor de teste: -Dtpf.guild=<id> ou variável de ambiente TPF_GUILD_ID
        String guildId = System.getProperty("tpf.guild", System.getenv("TPF_GUILD_ID"));
        if (guildId == null || guildId.isBlank()) {
            System.err.println("Falta o id do servidor: use -Dtpf.guild=<id> ou defina TPF_GUILD_ID.");
            System.exit(1);
        }

        // GUILD_MESSAGES para receber mensagens dos canais do servidor;
        // MESSAGE_CONTENT (privileged intent, ativado no Developer Portal) para ler o texto delas
        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.GUILD_MESSAGES, GatewayIntent.MESSAGE_CONTENT)
                .addEventListeners(new LeaderBot())
                .build();

        // espera até o bot estar ligado e com os servidores carregados
        jda.awaitReady();

        Guild guild = jda.getGuildById(guildId.trim());
        if (guild == null) {
            System.err.println("O bot não pertence ao servidor " + guildId + ". Use o URL de convite do README.");
            jda.shutdown();
            System.exit(1);
        }

        // guild commands: aparecem de imediato no servidor de teste
        // (os comandos globais podem demorar até uma hora a propagar).
        // updateCommands() substitui todos os comandos anteriores do bot neste servidor.
        guild.updateCommands()
                .addCommands(definirComandos())
                .queue(
                        cmds -> log("COMANDOS", cmds.size() + " comandos registados em " + guild.getName()),
                        erro -> log("ERRO", "falha ao registar os comandos: " + erro.getMessage()));

        log("ARRANQUE", "líder online como " + jda.getSelfUser().getName());
    }

    /**
     * Lê o token do bot da variável de ambiente DISCORD_TOKEN ou, se não existir,
     * do ficheiro indicado em -Dtpf.tokenFile (por omissão "discord.token").
     * Ambos ficam fora do repositório (o ficheiro está no .gitignore).
     */
    static String lerToken() throws IOException {
        String env = System.getenv("DISCORD_TOKEN");
        if (env != null && !env.isBlank()) return env.trim();

        Path ficheiro = Path.of(System.getProperty("tpf.tokenFile", "discord.token"));
        if (Files.isReadable(ficheiro)) return Files.readString(ficheiro).trim();

        System.err.println("Token não encontrado: defina DISCORD_TOKEN ou crie o ficheiro " + ficheiro);
        System.exit(1);
        return null; // nunca chega aqui
    }

    /**
     * Definição dos 7 slash commands e dos seus parâmetros.
     *
     * Os argumentos numéricos são declarados como STRING de propósito: se fossem
     * INTEGER, o próprio Discord bloquearia "/config r abc" antes de chegar ao bot,
     * e o critério de aceitação pede que seja o bot a recusar com uma mensagem de erro.
     *
     * Nota: o Discord só aceita nomes de opções em minúsculas, por isso o parâmetro
     * <intervaloMs> de /rajada chama-se "intervalo_ms".
     */
    static List<CommandData> definirComandos() {
        return List.of(
                Commands.slash("registar", "Regista um texto no arquivo")
                        .addOption(OptionType.STRING, "texto", "Texto a registar", true),

                Commands.slash("estado", "Mostra o estado das réplicas e o número de chunks subreplicados"),

                // /config r <n>: "r" é um subcomando, para permitir outros parâmetros no futuro
                Commands.slash("config", "Altera a configuração do arquivo (admin-arquivo)")
                        .addSubcommands(new SubcommandData("r", "Altera o fator de replicação R")
                                .addOption(OptionType.STRING, "n", "Novo valor de R (inteiro ≥ 1)", true)),

                Commands.slash("recuperar", "Recupera os registos de um intervalo de LSN")
                        .addOption(OptionType.STRING, "de", "Primeiro LSN (inteiro ≥ 1)", true)
                        .addOption(OptionType.STRING, "ate", "Último LSN (inteiro ≥ de)", true),

                Commands.slash("replica-token", "Gera um token de registo de réplica (admin-arquivo)"),

                Commands.slash("replica-remover", "Remove uma réplica registada (admin-arquivo)")
                        .addOption(OptionType.STRING, "id", "Identificador da réplica", true),

                Commands.slash("rajada", "Injeta n registos sintéticos (admin-arquivo)")
                        .addOption(OptionType.STRING, "n", "Número de registos (inteiro ≥ 1)", true)
                        .addOption(OptionType.STRING, "intervalo_ms", "Intervalo entre registos em ms (inteiro ≥ 0)", true));
    }

    // ------------------------------------------------------------------
    // Slash commands
    // ------------------------------------------------------------------

    /**
     * Ponto de entrada de todos os slash commands: verifica as permissões,
     * encaminha para o método do comando e trata os argumentos inválidos.
     */
    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent ev) {
        String nome = ev.getName();
        String autor = ev.getUser().getName();
        log("COMANDO", "/" + nome + " de " + autor);

        // 1. comandos de administração: só para quem tem o papel admin-arquivo
        if (COMANDOS_ADMIN.contains(nome) && !eAdmin(ev.getMember())) {
            log("RECUSADO", "/" + nome + " de " + autor + " sem o papel " + PAPEL_ADMIN);
            recusar(ev, "Não tem permissão para usar `/" + nome + "`: é preciso o papel `" + PAPEL_ADMIN + "`.");
            return;
        }

        // 2. encaminhamento; cada método valida os seus argumentos
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
            // 3. argumento inválido: resposta de erro visível só para quem executou o comando
            log("INVALIDO", "/" + nome + ": " + e.getMessage());
            recusar(ev, e.getMessage());
        }
    }

    // /registar <texto>
    private void cmdRegistar(SlashCommandInteractionEvent ev) throws ArgumentoInvalido {
        String texto = textoObrigatorio(ev, "texto");
        // TODO (sprints seguintes): atribuir LSN, append a registos.log com fsync, entrar no chunk aberto
        responder(ev, "`/registar` recebido: texto=\"" + eco(texto) + "\" (" + texto.length()
                + " caracteres) — ainda não implementado");
    }

    // /estado
    private void cmdEstado(SlashCommandInteractionEvent ev) {
        // TODO: estado, ocupação e nº de chunks por réplica, e nº de chunks subreplicados
        responder(ev, "`/estado` recebido — ainda não implementado");
    }

    // /config r <n>
    private void cmdConfig(SlashCommandInteractionEvent ev) throws ArgumentoInvalido {
        // por agora o único subcomando é "r"
        if (!"r".equals(ev.getSubcommandName()))
            throw new ArgumentoInvalido("Subcomando desconhecido. Use `/config r <n>`.");
        long n = inteiro(ev, "n", 1, Integer.MAX_VALUE);
        // TODO: alterar R; o reconciliador converge para o novo valor
        responder(ev, "`/config r` recebido: n=" + n + " — ainda não implementado");
    }

    // /recuperar <de> <ate>
    private void cmdRecuperar(SlashCommandInteractionEvent ev) throws ArgumentoInvalido {
        long de = inteiro(ev, "de", 1, Long.MAX_VALUE);   // os LSN começam em 1
        long ate = inteiro(ev, "ate", 1, Long.MAX_VALUE);
        if (de > ate)
            throw new ArgumentoInvalido("Intervalo inválido: `de` (" + de + ") tem de ser ≤ `ate` (" + ate + ").");
        // TODO: Recovery.recuperar(de, ate)
        responder(ev, "`/recuperar` recebido: de=" + de + ", ate=" + ate + " — ainda não implementado");
    }

    // /replica-token
    private void cmdReplicaToken(SlashCommandInteractionEvent ev) {
        // TODO: PubSubHub.gerarToken() (uso único, válido 10 minutos)
        responder(ev, "`/replica-token` recebido — ainda não implementado");
    }

    // /replica-remover <id>
    private void cmdReplicaRemover(SlashCommandInteractionEvent ev) throws ArgumentoInvalido {
        String id = textoObrigatorio(ev, "id");
        if (!ID_REPLICA.matcher(id).matches())
            throw new ArgumentoInvalido("Identificador de réplica inválido: \"" + eco(id)
                    + "\" (use só letras, dígitos, `_` e `-`, até 32 caracteres).");
        // TODO: remover a réplica de replicas.txt e repor R nos seus chunks
        responder(ev, "`/replica-remover` recebido: id=" + id + " — ainda não implementado");
    }

    // /rajada <n> <intervaloMs>
    private void cmdRajada(SlashCommandInteractionEvent ev) throws ArgumentoInvalido {
        long n = inteiro(ev, "n", 1, Integer.MAX_VALUE);
        long intervalo = inteiro(ev, "intervalo_ms", 0, Integer.MAX_VALUE);
        // TODO: injetar n registos sintéticos pelo mesmo caminho dos registos reais
        responder(ev, "`/rajada` recebido: n=" + n + ", intervaloMs=" + intervalo + " — ainda não implementado");
    }

    // ------------------------------------------------------------------
    // Mensagens no canal #arquivo
    // ------------------------------------------------------------------

    /**
     * Deteta as mensagens enviadas no canal #arquivo. Ignora bots (incluindo o
     * próprio líder, para não responder a si mesmo), webhooks, mensagens diretas
     * e mensagens de outros canais.
     */
    @Override
    public void onMessageReceived(MessageReceivedEvent ev) {
        if (ev.getAuthor().isBot() || ev.isWebhookMessage()) return;  // só pessoas
        if (!ev.isFromGuild()) return;                                 // ignora mensagens diretas
        if (!CANAL_ARQUIVO.equals(ev.getChannel().getName())) return;  // só #arquivo

        String autor = ev.getAuthor().getName();
        String texto = ev.getMessage().getContentRaw();
        log("MENSAGEM", "#" + CANAL_ARQUIVO + " de " + autor + ": " + texto.length() + " caracteres");

        // TODO: registar(autor, canal, texto), como o /registar
        ev.getMessage()
                .reply("Mensagem recebida em #" + CANAL_ARQUIVO + " de " + autor + ": \"" + eco(texto)
                        + "\" — registo ainda não implementado")
                .mentionRepliedUser(false)   // não notifica o autor
                .setAllowedMentions(List.of()) // o eco não pode fazer @everyone nem @menções
                .queue();
    }

    // ------------------------------------------------------------------
    // Auxiliares
    // ------------------------------------------------------------------

    // exceção usada para recusar um argumento inválido com uma mensagem para o utilizador
    static final class ArgumentoInvalido extends Exception {
        ArgumentoInvalido(String mensagem) { super(mensagem); }
    }

    // true se o membro tem o papel admin-arquivo (os slash commands são de servidor, por isso há sempre membro)
    static boolean eAdmin(Member membro) {
        return membro != null
                && membro.getRoles().stream().anyMatch(p -> p.getName().equalsIgnoreCase(PAPEL_ADMIN));
    }

    // lê uma opção de texto obrigatória e recusa-a se estiver vazia
    static String textoObrigatorio(SlashCommandInteractionEvent ev, String nome) throws ArgumentoInvalido {
        OptionMapping op = ev.getOption(nome);
        if (op == null || op.getAsString().isBlank())
            throw new ArgumentoInvalido("Falta o argumento `" + nome + "`.");
        return op.getAsString().trim();
    }

    // lê uma opção que tem de ser um inteiro no intervalo [min, max]
    static long inteiro(SlashCommandInteractionEvent ev, String nome, long min, long max) throws ArgumentoInvalido {
        String bruto = textoObrigatorio(ev, nome);
        long v;
        try {
            v = Long.parseLong(bruto);
        } catch (NumberFormatException e) {
            throw new ArgumentoInvalido("O argumento `" + nome + "` tem de ser um número inteiro (recebido: \""
                    + eco(bruto) + "\").");
        }
        if (v < min || v > max)
            throw new ArgumentoInvalido("O argumento `" + nome + "` tem de ser ≥ " + min
                    + (max < Integer.MAX_VALUE ? " e ≤ " + max : "") + " (recebido: " + v + ").");
        return v;
    }

    // resposta provisória normal, sem menções
    static void responder(SlashCommandInteractionEvent ev, String texto) {
        ev.reply(texto).setAllowedMentions(List.of()).queue();
    }

    // resposta de erro, visível apenas para quem executou o comando (ephemeral)
    static void recusar(SlashCommandInteractionEvent ev, String texto) {
        ev.reply("❌ " + texto).setEphemeral(true).setAllowedMentions(List.of()).queue();
    }

    // texto do utilizador preparado para ser ecoado: sem formatação markdown e truncado
    static String eco(String texto) {
        String t = texto.length() > MAX_ECO ? texto.substring(0, MAX_ECO) + "…" : texto;
        return MarkdownSanitizer.escape(t);
    }

    // registo provisório na consola (nos próximos sprints passa a eventos-lider.txt)
    static void log(String evento, String detalhe) {
        System.out.println(Instant.now().toEpochMilli() + ";" + evento + ";" + detalhe);
    }
}
