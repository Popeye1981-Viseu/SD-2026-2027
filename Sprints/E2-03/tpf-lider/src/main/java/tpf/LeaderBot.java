package tpf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
 * Líder do arquivo distribuído (TPF).
 *
 * E2-02: caminho de escrita local durável (LSN, log com fsync, chunk selado
 * e cifrado).
 *
 * E2-03: depois de selar e gravar o chunk em `selados/`, o líder insere-o no
 * catálogo como SELADO, escolhe R réplicas por rendezvous hashing ponderado
 * pelo espaço livre (com reserva atómica), publica STORE nos tópicos
 * privados e espera pelos ACK. Um ACK só conta se o hash for o chunkId, o id
 * da mensagem for o id AUTENTICADO da ligação e essa réplica tiver recebido o
 * STORE desse chunk. O chunk passa a DURAVEL quando os ACK válidos atingem
 * max(W_MIN, ⌊R/2⌋+1). Ao fim de ACK_TIMEOUT_MS há nova ronda, excluindo as
 * réplicas já tentadas, até R cópias ou MAX_RONDAS.
 *
 * Fluxo de um registo (único para /registar, #arquivo e /rajada), com DUAS
 * fases e semânticas de erro DIFERENTES:
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

    /** Publicação de uma mensagem num tópico (o hub real ou um fake de teste). */
    @FunctionalInterface
    interface Publicador {
        void publicar(String topico, String mensagem);
    }

    // ------------------------------------------------------------------
    // Estado do líder
    // ------------------------------------------------------------------

    private final Eventos eventos;
    private final LogRegistos log;
    private final byte[] chaveMestra;      // 32 bytes; usada para a DEK de cada chunk
    private final Path pastaDados;
    private final long limiteChunk;        // bytes em claro que provocam a selagem
    private final ScheduledExecutorService agendador;
    // /rajada pode durar minutos; não pode bloquear selagem, ACK nem o monitor.
    private final ExecutorService rajadaExecutor;

    private final Catalog catalogo;
    private final ReplicaRegistry replicas;
    private final Publicador publicador;   // null => usa o hub, se estiver ligado
    private final long ackTimeoutMs;
    private final int maxRondas;
    private final int fatorR;

    private volatile PubSubHub hub;

    // Chunk aberto e o temporizador de selagem. Ambos só são tocados dentro de
    // métodos synchronized nesta instância (o agendador entra pelo mesmo lock).
    private Chunk chunkAberto;
    private ScheduledFuture<?> selagemAgendada;

    // Estado das colocações em curso, por chunkId. Protegido pelo lock da instância.
    private final Map<String, Colocacao> colocacoes = new HashMap<>();

    private LeaderBot(Eventos eventos, LogRegistos log, byte[] chaveMestra, Path pastaDados,
                      long limiteChunk, Catalog catalogo, ReplicaRegistry replicas,
                      Publicador publicador, long ackTimeoutMs, int maxRondas, int fatorR) {
        if (limiteChunk <= 0) throw new IllegalArgumentException("limiteChunk tem de ser > 0");
        if (catalogo == null) throw new IllegalArgumentException("catálogo nulo");
        if (replicas == null) throw new IllegalArgumentException("registo de réplicas nulo");
        this.eventos = eventos;
        this.log = log;
        this.chaveMestra = chaveMestra.clone();
        this.pastaDados = pastaDados;
        this.limiteChunk = limiteChunk;
        this.catalogo = catalogo;
        this.replicas = replicas;
        this.publicador = publicador;
        this.ackTimeoutMs = ackTimeoutMs;
        this.maxRondas = maxRondas;
        this.fatorR = fatorR;
        this.agendador = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "tpf-agendador");
            t.setDaemon(true);
            return t;
        });
        this.rajadaExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "tpf-rajada");
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
        return paraTestes(eventos, log, chaveMestra, pastaDados, Wire.CHUNK_MAX_BYTES);
    }

    /**
     * Como acima, mas com o limite de chunk injectado. Existe porque
     * `Wire.CHUNK_MAX_BYTES` é `static final` e é lido uma única vez por JVM: sem este
     * parâmetro, um teste não consegue provocar vários chunks sem dormir por megabytes.
     */
    static LeaderBot paraTestes(Eventos eventos, LogRegistos log, byte[] chaveMestra, Path pastaDados,
                                long limiteChunk) {
        return new LeaderBot(eventos, log, chaveMestra, pastaDados, limiteChunk,
                Catalog.vazio(pastaDados), new ReplicaRegistry(eventos), null,
                Wire.ACK_TIMEOUT_MS, Wire.MAX_RONDAS, Wire.R);
    }

    /** Instância de teste com catálogo, registo de réplicas e publicador injetados. */
    static LeaderBot paraTestesComReplicas(Eventos eventos, LogRegistos log, byte[] chaveMestra,
                                           Path pastaDados, long limiteChunk, Catalog catalogo,
                                           ReplicaRegistry replicas, Publicador publicador,
                                           long ackTimeoutMs, int maxRondas, int fatorR) {
        return new LeaderBot(eventos, log, chaveMestra, pastaDados, limiteChunk, catalogo,
                replicas, publicador, ackTimeoutMs, maxRondas, fatorR);
    }

    /** Instância de produção: catálogo carregado e registo de réplicas vazio. */
    static LeaderBot paraProducao(Eventos eventos, LogRegistos log, byte[] chaveMestra,
                                  Path pastaDados, Catalog catalogo) {
        return new LeaderBot(eventos, log, chaveMestra, pastaDados, Wire.CHUNK_MAX_BYTES,
                catalogo, new ReplicaRegistry(eventos), null,
                Wire.ACK_TIMEOUT_MS, Wire.MAX_RONDAS, Wire.R);
    }

    /** Liga o hub ao líder. A partir daqui, STORE/ACK passam a usar a rede. */
    void ligarHub(PubSubHub hub) {
        this.hub = hub;
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
        Path pastaDados   = Path.of(System.getProperty("tpf.pasta", "dados-lider"));
        Path caminhoChave = Path.of(System.getProperty("tpf.masterKey", "master.key"));

        // 1a. NUNCA criar uma chave nova por cima de um arquivo que já existe.
        String recusa = recusaPorFaltaDeChave(caminhoChave, pastaDados,
                Boolean.getBoolean("tpf.novaChave"));
        if (recusa != null) {
            System.err.println("ERRO FATAL NO ARRANQUE: " + recusa);
            System.exit(1);
            return;
        }

        byte[] masterKey;
        try {
            masterKey = Wire.carregarOuCriarMasterKey(caminhoChave);   // só agora pode criar
        } catch (Wire.MasterKeyException e) {
            System.err.println("ERRO FATAL NO ARRANQUE: " + e.getMessage());
            System.exit(1);
            return;
        }

        // 2. Recuperação do log (fail-fast: cauda parcial reparada, corrupção aborta)
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

        // 3. Catálogo (fail-fast: um catálogo ilegível não pode ser adivinhado)
        Catalog catalogo;
        try {
            catalogo = Catalog.carregar(pastaDados);
        } catch (IOException e) {
            System.err.println("ERRO FATAL NO ARRANQUE: catalogo.txt inválido: " + e.getMessage());
            System.exit(1);
            return;
        }

        LeaderBot lider = LeaderBot.paraProducao(eventos, log, masterKey, pastaDados, catalogo);
        Runtime.getRuntime().addShutdownHook(new Thread(lider::fechar, "tpf-shutdown"));

        lider.registarEvento("ARRANQUE", "master.key validada (" + masterKey.length + " bytes)");
        lider.registarEvento("ARRANQUE", "registos.log recuperado: último LSN=" + log.ultimoLsn()
                + (log.bytesDescartadosNaRecuperacao() > 0
                ? ", " + log.bytesDescartadosNaRecuperacao() + " bytes de escrita parcial descartados"
                : ", sem reparações"));
        lider.registarEvento("ARRANQUE", "catálogo: " + catalogo.total() + " chunks ("
                + catalogo.contar(Catalog.Estado.DURAVEL) + " DURAVEL, "
                + catalogo.subreplicados() + " subreplicados)");

        // 4. Hub de publish/subscribe (antes do Discord, para as réplicas poderem ligar-se)
        try {
            PubSubHub hub = new PubSubHub(Wire.PORTO, eventos, lider.replicas(), pastaDados,
                    lider::receberDoHub);
            lider.ligarHub(hub);
            hub.start();
            if (!hub.aguardarArranque(5_000)) {
                System.err.println("ERRO FATAL NO ARRANQUE: o hub não conseguiu abrir o porto "
                        + Wire.PORTO + " (ver eventos-lider.txt).");
                System.exit(1);
                return;
            }
        } catch (IOException | RuntimeException e) {
            System.err.println("ERRO FATAL NO ARRANQUE: hub WebSocket no porto " + Wire.PORTO
                    + ": " + e.getMessage());
            System.exit(1);
            return;
        }
        lider.iniciarMonitorDeReplicas();

        // 5. Discord (só com chave, log, catálogo e hub válidos)
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
     * Sela o chunk aberto: padding, cifra, escrita em `selados/`, evento SELADO
     * e entrada no catálogo/colocação. O chunk só é descartado DEPOIS de estar
     * gravado: uma falha aqui deixa-o aberto (os registos continuam duráveis em
     * registos.log).
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

        try {
            catalogo.adicionar(selado, c.lsnMinimo(), c.lsnMaximo());
        } catch (IOException e) {
            registarEvento("ERRO", "chunk " + selado.chunkId()
                    + " selado mas NÃO entrou no catálogo: " + e.getMessage());
            return;
        }

        try {
            colocarChunk(selado);
        } catch (IOException e) {
            registarEvento("ERRO", "colocação do chunk " + selado.chunkId()
                    + " falhou: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Colocação replicada (E2-03)
    // ------------------------------------------------------------------

    /** Estado de uma colocação em curso (uma por chunk). */
    private static final class Colocacao {
        final String chunkId;
        final long tamanho;
        final Set<String> tentadas = new HashSet<>();        // já receberam (ou tentaram) STORE
        final Set<String> storeEnviado = new HashSet<>();    // validação de ACK
        final Set<String> reservasPendentes = new HashSet<>(); // reservadas sem ACK
        int ronda;
        String binB64;
        String hdrB64;

        Colocacao(String chunkId, long tamanho) {
            this.chunkId = chunkId;
            this.tamanho = tamanho;
        }
    }

    /** Entra no catálogo e arranca a primeira ronda de colocação. */
    private synchronized void colocarChunk(Sealed selado) throws IOException {
        Colocacao c = new Colocacao(selado.chunkId(), selado.tamanhoCifrado());
        colocacoes.put(c.chunkId, c);
        colocarRonda(c);
    }

    /**
     * Executa uma ronda: exclui as réplicas já tentadas, reserva espaço para as
     * que faltam e publica STORE. Agenda ACK_TIMEOUT_MS para a ronda.
     */
    private void colocarRonda(Colocacao c) {
        c.ronda++;
        registarEvento("RONDA", c.chunkId + " " + c.ronda);

        Set<String> copias = catalogo.copias(c.chunkId);
        if (copias.size() >= fatorR) {
            concluir(c);
            return;
        }

        int faltam = fatorR - copias.size();
        Set<String> excluir = new HashSet<>(c.tentadas);
        List<String> escolhidas = replicas.reserveAndChoose(c.chunkId, c.tamanho,
                faltam, excluir);

        int previstas = copias.size() + escolhidas.size();
        if (escolhidas.size() < faltam) {
            // Formato: SUBREPLICADO <chunkId> <n>/<R>
            registarEvento("SUBREPLICADO", c.chunkId + " " + previstas + "/" + fatorR);
        }
        if (escolhidas.isEmpty()) {
            concluir(c);
            return;
        }

        try {
            carregarBytes(c);
        } catch (IOException e) {
            registarEvento("ERRO", "não foi possível ler o chunk " + c.chunkId
                    + " de " + Sealed.PASTA + ": " + e.getMessage());
            for (String id : escolhidas) replicas.libertar(id, c.tamanho);
            concluir(c);
            return;
        }

        String mensagem = new Wire.Store(c.chunkId, c.hdrB64, c.binB64).encode();
        for (String id : escolhidas) {
            c.reservasPendentes.add(id);
            c.tentadas.add(id);
            c.storeEnviado.add(id);
            registarEvento("STORE", c.chunkId + " -> " + id);
            try {
                publicar(Wire.topicReplica(id), mensagem);
            } catch (RuntimeException e) {
                registarEvento("ERRO", "STORE " + c.chunkId + " -> " + id + ": " + e.getMessage());
            }
        }

        if (c.reservasPendentes.isEmpty()) {
            concluir(c);
            return;
        }

        final String chunkId = c.chunkId;
        final int ronda = c.ronda;
        agendador.schedule(() -> expirar(chunkId, ronda), ackTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Fim de prazo de uma ronda: liberta reservas e tenta repor as cópias que faltam. */
    private synchronized void expirar(String chunkId, int ronda) {
        Colocacao c = colocacoes.get(chunkId);
        if (c == null || c.ronda != ronda) return;            // ronda já ultrapassada

        for (String id : new ArrayList<>(c.reservasPendentes)) {
            replicas.libertar(id, c.tamanho);
        }
        c.reservasPendentes.clear();

        int copias = catalogo.copias(chunkId).size();
        if (copias >= fatorR) {
            concluir(c);
            return;
        }
        if (c.ronda >= maxRondas) {
            registarEvento("SUBREPLICADO", chunkId + " " + copias + "/" + fatorR);
            concluir(c);
            return;
        }

        try {
            colocarRonda(c);
        } catch (RuntimeException e) {
            registarEvento("ERRO", "ronda " + (c.ronda + 1) + " do chunk " + chunkId
                    + " falhou: " + e.getMessage());
            concluir(c);
        }
    }

    /** Liberta os bytes em Base64 quando a colocação termina (o catálogo guarda o resto). */
    private void concluir(Colocacao c) {
        c.binB64 = null;
        c.hdrB64 = null;
    }

    private void carregarBytes(Colocacao c) throws IOException {
        Path pasta = pastaDados.resolve(Sealed.PASTA);
        byte[] bin = Files.readAllBytes(pasta.resolve(c.chunkId + Sealed.EXT_BIN));
        byte[] hdr = Files.readAllBytes(pasta.resolve(c.chunkId + Sealed.EXT_HDR));
        c.binB64 = Base64.getEncoder().encodeToString(bin);
        c.hdrB64 = Base64.getEncoder().encodeToString(hdr);
    }

    private void publicar(String topico, String mensagem) {
        Publicador p = publicador;
        if (p != null) {
            p.publicar(topico, mensagem);
            return;
        }
        PubSubHub h = hub;
        if (h != null) h.publish(topico, mensagem);
    }

    /**
     * Callback do hub: entrega uma mensagem publicada por uma réplica em
     * tpf-lider, já com o id AUTENTICADO da ligação (não o id escrito na
     * mensagem). Só HB, ACK e INVENTARIO são esperados neste sprint.
     */
    void receberDoHub(String idAutenticado, String mensagem) {
        if (idAutenticado == null) return;

        // Uma mensagem em voo de uma ligação já revogada por /replica-remover
        // não pode ressuscitar a réplica no registo.
        PubSubHub h = hub;
        if (h != null && !h.registada(idAutenticado)) {
            registarEvento("ERRO", "mensagem de réplica não registada (" + idAutenticado
                    + ") ignorada: " + abreviar(mensagem));
            return;
        }

        try {
            switch (Wire.tipo(mensagem)) {
                case "HB" -> {
                    Wire.Hb hb = Wire.Hb.decode(mensagem);
                    if (!hb.id().equals(idAutenticado)) {
                        registarEvento("ERRO", "HB de " + idAutenticado
                                + " com id de outra réplica (" + hb.id() + ")");
                        return;
                    }
                    replicas.onHeartbeat(hb);
                }
                case "ACK" -> receberAck(idAutenticado, Wire.Ack.decode(mensagem));
                case "INVENTARIO" -> {
                    // Anti-entropia e reconciliação: sprint seguinte.
                }
                default -> registarEvento("ERRO", "mensagem inesperada de " + idAutenticado
                        + " em " + Wire.TOPICO_LIDER + ": " + abreviar(mensagem));
            }
        } catch (Wire.FormatoInvalido e) {
            registarEvento("ERRO", "mensagem inválida de " + idAutenticado
                    + " em " + Wire.TOPICO_LIDER + ": " + e.getMessage());
        }
    }

    /**
     * Valida e aplica um ACK. As três condições do enunciado, por esta ordem:
     * (1) hash == chunkId; (2) id da mensagem == id autenticado da ligação;
     * (3) a réplica recebeu o STORE deste chunk. Um ACK rejeitado não altera
     * o catálogo nem as reservas.
     */
    private synchronized void receberAck(String idLigacao, Wire.Ack ack) {
        String motivo;
        Colocacao c = colocacoes.get(ack.chunkId());

        if (!ack.chunkId().equals(ack.sha256())) {
            motivo = "hash " + abreviar(ack.sha256()) + " diferente do chunkId";
        } else if (!ack.id().equals(idLigacao)) {
            motivo = "id da mensagem (" + abreviar(ack.id())
                    + ") diferente do id autenticado da ligação (" + idLigacao + ")";
        } else if (c == null) {
            motivo = "chunk desconhecido (não houve STORE nesta execução)";
        } else if (!c.storeEnviado.contains(idLigacao)) {
            motivo = "a réplica não recebeu o STORE deste chunk";
        } else if (catalogo.copias(ack.chunkId()).contains(idLigacao)) {
            motivo = "cópia já confirmada";
        } else {
            motivo = null;
        }

        if (motivo != null) {
            registarEvento("ACK-REJEITADO", ack.chunkId() + " " + idLigacao + " " + motivo);
            return;
        }

        boolean eraDuravel = catalogo.estado(ack.chunkId()) == Catalog.Estado.DURAVEL;
        try {
            catalogo.confirmar(ack.chunkId(), idLigacao);
        } catch (IOException e) {
            registarEvento("ERRO", "falha ao gravar o catálogo depois do ACK de "
                    + idLigacao + " para " + ack.chunkId() + ": " + e.getMessage());
        }

        // A reserva passa a usados: o espaço livre não fica sobrestimado até ao
        // próximo heartbeat. Depois do ACK, a réplica deixa de estar pendente.
        replicas.confirmarReserva(idLigacao, c.tamanho);
        c.reservasPendentes.remove(idLigacao);

        registarEvento("ACK", ack.chunkId() + " " + idLigacao);

        if (!eraDuravel && catalogo.estado(ack.chunkId()) == Catalog.Estado.DURAVEL) {
            registarEvento("DURAVEL", ack.chunkId());
        }

        if (catalogo.copias(ack.chunkId()).size() >= fatorR) {
            concluir(c);
        }
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
        StringBuilder sb = new StringBuilder();
        sb.append("`/estado` — último LSN: ").append(log.ultimoLsn()).append('\n');

        List<ReplicaRegistry.ReplicaInfo> infos = replicas.listar();
        sb.append("**Réplicas** (").append(infos.size()).append(")\n");
        if (infos.isEmpty()) {
            sb.append("· nenhuma réplica registada\n");
        } else {
            int limite = Math.min(infos.size(), 15);
            for (int i = 0; i < limite; i++) {
                ReplicaRegistry.ReplicaInfo r = infos.get(i);
                sb.append("· **").append(r.id()).append("** — ").append(r.estado())
                        .append(" · ").append(formatarBytes(r.usados())).append('/')
                        .append(formatarBytes(r.quota())).append(" · ")
                        .append(catalogo.chunksPorReplica(r.id())).append(" chunks\n");
            }
            if (infos.size() > limite) {
                sb.append("· … e ").append(infos.size() - limite).append(" outras\n");
            }
        }

        sb.append("**Catálogo**: ").append(catalogo.total()).append(" chunks · SELADO ")
                .append(catalogo.contar(Catalog.Estado.SELADO)).append(" · DURAVEL ")
                .append(catalogo.contar(Catalog.Estado.DURAVEL)).append(" · subreplicados ")
                .append(catalogo.subreplicados()).append('\n');
        sb.append(estadoDoChunkAberto());
        if (log.estaBloqueado()) sb.append(" · log em fail-stop (reinicie o líder)");
        responder(ev, limitar(sb.toString()));
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
        PubSubHub h = hub;
        if (h == null) {
            recusar(ev, "O hub WebSocket não está ativo; o token não pode ser gerado.");
            return;
        }
        String token = h.gerarToken();
        // A resposta é EFÉMERA: só quem executou o comando vê o token. O token
        // nunca é escrito em eventos-lider.txt.
        responderEfemero(ev, "Token de registo (uso único, válido "
                + (Wire.TOKEN_VALIDADE_MS / 60000) + " min):\n```\n" + token
                + "\n```\nEntregue-o apenas a quem vai arrancar a réplica, no argumento opcional de "
                + "`ReplicaService <id> <url> <quotaBytes> <token>`.");
    }

    // /replica-remover <id>
    private void cmdReplicaRemover(SlashCommandInteractionEvent ev) throws ArgumentoInvalido {
        // Aqui o valor é um TOKEN, não conteúdo: aparar é normalização.
        String id = textoObrigatorio(ev, "id");
        if (!ID_REPLICA.matcher(id).matches())
            throw new ArgumentoInvalido("Identificador de réplica inválido: \"" + eco(id)
                    + "\" (use só letras, dígitos, `_` e `-`, até 32 caracteres).");

        PubSubHub h = hub;
        if (h == null) {
            recusar(ev, "O hub WebSocket não está ativo; não há chaves para revogar.");
            return;
        }
        try {
            boolean existia = h.removerReplica(id);
            replicas.remover(id);
            if (existia) {
                registarEvento("REMOVIDA", id);
                responder(ev, "Réplica **" + id + "** removida: chave revogada, ligação fechada e"
                        + " excluída da colocação. As cópias que tinha ficam no catálogo até à"
                        + " reconciliação do próximo sprint.");
            } else {
                recusar(ev, "Não existe réplica registada com o id \"" + eco(id) + "\".");
            }
        } catch (IOException e) {
            registarEvento("ERRO", "/replica-remover " + id + ": " + e.getMessage());
            recusar(ev, "❌ Não foi possível gravar `replicas.txt`: " + e.getMessage());
        }
    }

    // /rajada <n> <intervaloMs>
    private void cmdRajada(SlashCommandInteractionEvent ev) throws ArgumentoInvalido {
        long n = inteiro(ev, "n", 1, MAX_RAJADA);
        long intervalo = inteiro(ev, "intervalo_ms", 0, Integer.MAX_VALUE);
        String autor = ev.getUser().getName();

        ev.deferReply().queue(
                ok -> rajadaExecutor.execute(() -> executarRajada(ev, autor, n, intervalo)),
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

    static void responderEfemero(SlashCommandInteractionEvent ev, String texto) {
        ev.reply(texto).setEphemeral(true).setAllowedMentions(List.of()).queue();
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

    private static String abreviar(String valor) {
        if (valor == null) return "?";
        return valor.length() <= MAX_ECO ? valor : valor.substring(0, MAX_ECO) + "…";
    }

    /** Descrição do chunk aberto, lida sob o lock (não bloqueia escritas por muito tempo). */
    private synchronized String estadoDoChunkAberto() {
        if (chunkAberto == null) return "chunk aberto: nenhum";
        return "chunk aberto: " + chunkAberto.tamanho() + " registos, "
                + chunkAberto.bytes() + "/" + limiteChunk + " bytes";
    }

    private static String formatarBytes(long bytes) {
        if (bytes < 0) return "-";
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024L * 1024L) return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
        return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024.0));
    }

    private static String limitar(String texto) {
        final int max = 1900;            // margem para o limite de 2000 do Discord
        return texto.length() <= max ? texto : texto.substring(0, max) + "…";
    }

    /** Agenda a atualização periódica dos estados (VIVA/SUSPEITA/MORTA). */
    void iniciarMonitorDeReplicas() {
        long periodo = Math.max(200, Wire.PERIODO_HB_MS / 2);
        agendador.scheduleAtFixedRate(() -> {
            try {
                replicas.atualizarEstados(System.currentTimeMillis());
            } catch (RuntimeException e) {
                registarEvento("ERRO", "monitor de réplicas: " + e.getMessage());
            }
        }, periodo, periodo, TimeUnit.MILLISECONDS);
    }

    private void registarEvento(String evento, String detalhe) {
        eventos.registar(evento, detalhe);
        System.out.println(evento + " " + detalhe);
    }

    /** Fecho limpo: sela o chunk pendente, fecha hub, canais e o agendador. */
    synchronized void fechar() {
        // Primeiro impede novas rajadas: /rajada pode estar a dormir entre registos.
        rajadaExecutor.shutdownNow();

        try {
            selarChunkAberto();               // não perder o chunk aberto num shutdown limpo
        } catch (IOException e) {
            registarEvento("ERRO", "falha ao selar o chunk pendente no shutdown: " + e.getMessage());
        }

        PubSubHub h = hub;
        if (h != null) {
            try {
                h.parar();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                registarEvento("ERRO", "falha ao parar o hub: " + e.getMessage());
            }
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

    // ------------------------------------------------------------------
    // Acesso para testes e para o arranque
    // ------------------------------------------------------------------

    Catalog catalogo() { return catalogo; }

    ReplicaRegistry replicas() { return replicas; }

    int quorum() { return catalogo == null ? Wire.quorumDurabilidade() : Wire.quorumDurabilidade(fatorR, catalogo.wMin()); }

    /**
     * Devolve a mensagem de recusa se existirem dados do arquivo sem master.key, ou null se
     * o arranque pode prosseguir. Chamado ANTES de Wire.carregarOuCriarMasterKey.
     */
    static String recusaPorFaltaDeChave(Path caminhoChave, Path pastaDados, boolean aceitarNovaChave) {
        if (aceitarNovaChave || Files.exists(caminhoChave)) return null;

        boolean temLog     = Files.exists(pastaDados.resolve(LogRegistos.FICHEIRO));
        boolean temEventos = Files.exists(pastaDados.resolve(LogRegistos.FICHEIRO_EVENTOS));
        boolean temSelados = Files.isDirectory(pastaDados.resolve(Sealed.PASTA));
        boolean temCatalogo = Files.exists(pastaDados.resolve(Catalog.FICHEIRO));
        boolean temReplicas = Files.exists(pastaDados.resolve(PubSubHub.FICHEIRO_REPLICAS));
        if (!temLog && !temEventos && !temSelados && !temCatalogo && !temReplicas) return null;

        return "existem dados do arquivo em " + pastaDados.toAbsolutePath()
                + " mas falta a chave mestra " + caminhoChave.toAbsolutePath() + "."
                + " Criar uma chave nova tornaria os chunks existentes indecifráveis."
                + " Reponha a master.key a partir de uma cópia de segurança ou arranque com"
                + " -Dtpf.novaChave=true se aceita começar de novo.";
    }
}
