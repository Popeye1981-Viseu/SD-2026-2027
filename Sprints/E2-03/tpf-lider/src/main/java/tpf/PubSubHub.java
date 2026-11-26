package tpf;

import org.java_websocket.WebSocket;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/**
 * Hub de publish/subscribe sobre WebSocket do líder (E2-03, objetivo 3).
 *
 * Cada ligação passa por três fases:
 *
 *   NOVA --OLA--> DESAFIADA --PROVA válida--> AUTENTICADA
 *
 * Regras:
 *  - só depois de autenticada pode subscrever {@code tpf-r-<o seu id>} e
 *    {@code tpf-ctl}, e publicar em {@code tpf-lider};
 *  - uma réplica nova só entra com um token de uso único gerado por
 *    /replica-token (consumido apenas depois de uma PROVA válida);
 *  - mensagens fora de ordem, um segundo OLA, subscrições ou publicações no
 *    tópico de outra réplica desligam a ligação e registam ERRO.
 *
 * O desafio tem 32 bytes de SecureRandom, é novo em cada ligação e é
 * descartado após uma tentativa (mesmo que a assinatura seja inválida).
 */
public final class PubSubHub extends WebSocketServer {

    /** Ficheiro com as réplicas registadas: {@code id;chavePublicaB64}. */
    public static final String FICHEIRO_REPLICAS = "replicas.txt";

    /** Prefixo assinado pela réplica: "tpf-registo|" + id + "|" + nonceB64. */
    public static final String PREFIXO_ASSINATURA = "tpf-registo|";

    /** Uma trama STORE leva um chunk cifrado em Base64 (~87 KB com 64 KiB). */
    private static final int TAMANHO_MAX_TRAMA = 4 * 1024 * 1024;

    private static final int NONCE_BYTES = 32;
    private static final int TOKEN_BYTES = 24;

    private static final Pattern ID_REPLICA = Pattern.compile("[A-Za-z0-9_-]{1,32}");

    /** Canal mínimo de uma ligação: permite testar as regras sem abrir sockets. */
    public interface CanalLigacao {
        void enviar(String mensagem);
        void fechar(int codigo, String motivo);
    }

    /** Fase do protocolo de uma ligação. */
    public enum Fase { NOVA, DESAFIADA, AUTENTICADA }

    /** Estado de uma ligação ao hub. */
    public static final class Ligacao {
        final CanalLigacao canal;
        Fase fase = Fase.NOVA;
        String id;
        String nonceB64;
        String chavePendenteB64;
        String tokenPendente;
        boolean fechada;
        final Set<String> topicos = new HashSet<>();

        Ligacao(CanalLigacao canal) {
            this.canal = canal;
        }

        public Fase fase() { return fase; }
        public String id() { return id; }
        public boolean fechada() { return fechada; }
        public Set<String> topicos() { return Set.copyOf(topicos); }

        void enviar(String mensagem) {
            if (!fechada) canal.enviar(mensagem);
        }
    }

    private final Eventos eventos;
    private final ReplicaRegistry registry;
    private final BiConsumer<String, String> aoReceber;
    private final LongSupplier relogio;
    private final Path ficheiroReplicas;
    private final SecureRandom rng = new SecureRandom();

    /** id da réplica -> chave pública Ed25519 em X.509. */
    private final Map<String, byte[]> replicas = new HashMap<>();

    /** token -> instante de criação (epoch millis). */
    private final Map<String, Long> tokens = new HashMap<>();

    /** tópico -> ligações subscritas. */
    private final Map<String, Set<Ligacao>> subscricoes = new HashMap<>();

    /** Ligações abertas (reais e de teste), para as poder desligar. */
    private final Set<Ligacao> ligacoes = ConcurrentHashMap.newKeySet();

    /** True depois de o servidor ter feito bind (onStart). */
    private volatile boolean arrancado;

    /** Liberta quando o bind termina (com sucesso ou erro), para o main falhar rápido. */
    private final CountDownLatch arranque = new CountDownLatch(1);

    /** Constructor de produção: porto do sprint e relógio do sistema. */
    public PubSubHub(int porto, Eventos eventos, ReplicaRegistry registry,
                     Path pastaDados, BiConsumer<String, String> aoReceber) throws IOException {
        this(porto, eventos, registry, pastaDados, aoReceber, System::currentTimeMillis);
    }

    /** Constructor com relógio injetável (testes de expiração de token). */
    PubSubHub(int porto, Eventos eventos, ReplicaRegistry registry,
              Path pastaDados, BiConsumer<String, String> aoReceber, LongSupplier relogio)
            throws IOException {
        super(new InetSocketAddress(porto),
                new ArrayList<>(List.of(new Draft_6455(List.of(), TAMANHO_MAX_TRAMA))));
        if (eventos == null) throw new IllegalArgumentException("eventos nulo");
        if (registry == null) throw new IllegalArgumentException("registry nulo");
        if (pastaDados == null) throw new IllegalArgumentException("pasta nula");
        if (aoReceber == null) throw new IllegalArgumentException("callback nulo");
        if (relogio == null) throw new IllegalArgumentException("relógio nulo");
        this.eventos = eventos;
        this.registry = registry;
        this.aoReceber = aoReceber;
        this.relogio = relogio;
        Files.createDirectories(pastaDados);
        this.ficheiroReplicas = pastaDados.resolve(FICHEIRO_REPLICAS);
        carregarReplicas();
        for (String id : replicas.keySet()) registry.conhecida(id);
    }

    // ------------------------------------------------------------------
    // Ciclo de vida WebSocket
    // ------------------------------------------------------------------

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        Ligacao l = new Ligacao(new CanalWebSocket(conn));
        conn.setAttachment(l);
        ligacoes.add(l);
    }

    @Override
    public void onMessage(WebSocket conn, String mensagem) {
        Ligacao l = conn.getAttachment();
        if (l == null) {
            conn.close(1008, "ligação sem estado");
            return;
        }
        processar(l, mensagem);
    }

    @Override
    public void onClose(WebSocket conn, int codigo, String motivo, boolean remoto) {
        Ligacao l = conn.getAttachment();
        if (l != null) removerLigacao(l);
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        Ligacao l = conn == null ? null : conn.getAttachment();
        registarErro("hub: erro de WebSocket"
                + (l != null && l.id != null ? " na ligação de " + l.id : "")
                + ": " + (ex == null ? "desconhecido" : ex.getMessage()));
        if (!arrancado && conn == null) {
            // Erro de bind: liberta o main para abortar em vez de ficar sem hub.
            arranque.countDown();
        }
    }

    @Override
    public void onStart() {
        arrancado = true;
        arranque.countDown();
        registarEvento("ARRANQUE", "hub pub/sub à escuta no porto " + getPort());
    }

    /** Para o servidor e fecha as ligações (shutdown do líder). */
    public void parar() throws InterruptedException {
        // Sem start (testes sem sockets) não há selector thread para parar.
        if (!arrancado) return;
        stop(1000);
    }

    /**
     * Espera até {@code ms} pelo bind. Devolve true se o hub está à escuta.
     * O main usa isto para não continuar com um hub que falhou o porto.
     */
    public boolean aguardarArranque(long ms) throws InterruptedException {
        if (!arranque.await(ms, TimeUnit.MILLISECONDS)) return false;
        return arrancado;
    }

    // ------------------------------------------------------------------
    // Máquina de estados do protocolo
    // ------------------------------------------------------------------

    /** Processa uma trama de uma ligação. Visível para testes sem sockets. */
    void processar(Ligacao l, String mensagem) {
        if (l == null || l.fechada) return;
        // Cada ligação tem um lock próprio: duas mensagens da mesma ligação
        // nunca se cruzam (o hub pode usar mais do que uma worker thread).
        synchronized (l) {
            if (l.fechada) return;
            processarSemLock(l, mensagem);
        }
    }

    private void processarSemLock(Ligacao l, String mensagem) {
        try {
            switch (Wire.tipo(mensagem)) {
                case "OLA" -> {
                    if (l.fase != Fase.NOVA) {
                        recusar(l, "segundo OLA na mesma ligação");
                        return;
                    }
                    tratarOla(l, Wire.Ola.decode(mensagem));
                }
                case "PROVA" -> {
                    if (l.fase != Fase.DESAFIADA) {
                        recusar(l, "PROVA sem desafio pendente");
                        return;
                    }
                    tratarProva(l, Wire.Prova.decode(mensagem));
                }
                case "SUB" -> {
                    if (l.fase != Fase.AUTENTICADA) {
                        recusar(l, "SUB antes de uma PROVA válida");
                        return;
                    }
                    tratarSub(l, Wire.Sub.decode(mensagem));
                }
                case "PUB" -> {
                    if (l.fase != Fase.AUTENTICADA) {
                        recusar(l, "PUB antes de uma PROVA válida");
                        return;
                    }
                    tratarPub(l, Wire.Pub.decode(mensagem));
                }
                default -> recusar(l, "mensagem desconhecida: " + abreviar(mensagem));
            }
        } catch (Wire.FormatoInvalido e) {
            recusar(l, "formato inválido: " + e.getMessage());
        } catch (RuntimeException e) {
            recusar(l, "erro ao processar a mensagem: " + e.getMessage());
        }
    }

    private void tratarOla(Ligacao l, Wire.Ola ola) {
        String id = ola.id();
        if (!ID_REPLICA.matcher(id).matches()) {
            recusar(l, "identificador de réplica inválido: " + abreviar(id));
            return;
        }

        byte[] chaveNova = null;
        if (ola.chavePublicaB64() != null) {
            try {
                chaveNova = decodificarChave(ola.chavePublicaB64());
            } catch (IllegalArgumentException e) {
                recusar(l, "chave pública inválida no OLA: " + e.getMessage());
                return;
            }
        }

        synchronized (this) {
            byte[] registada = replicas.get(id);

            if (registada == null) {
                // Primeiro registo: exige chave e token válido.
                if (chaveNova == null) {
                    recusar(l, "réplica nova " + id + " sem chave pública (OLA;<id>;<chave>;<token>)");
                    return;
                }
                if (ola.token() == null) {
                    recusar(l, "réplica nova " + id + " sem token de registo");
                    return;
                }
                if (!tokenValido(ola.token())) {
                    recusar(l, "token inválido, expirado ou já usado para a réplica " + id);
                    return;
                }
                l.chavePendenteB64 = ola.chavePublicaB64();
                l.tokenPendente = ola.token();
            } else {
                // Réplica já registada: OLA;<id> basta; a chave não pode mudar.
                if (chaveNova != null && !java.util.Arrays.equals(chaveNova, registada)) {
                    recusar(l, "réplica " + id + " já registada com outra chave pública");
                    return;
                }
                if (chaveNova == null && ola.token() != null) {
                    recusar(l, "OLA de réplica registada com token mas sem chave pública");
                    return;
                }
            }

            l.id = id;
            byte[] nonce = new byte[NONCE_BYTES];
            rng.nextBytes(nonce);
            l.nonceB64 = Base64.getEncoder().encodeToString(nonce);
            l.fase = Fase.DESAFIADA;
            l.enviar(new Wire.Desafio(l.nonceB64).encode());
        }
    }

    private void tratarProva(Ligacao l, Wire.Prova prova) {
        String nonce = l.nonceB64;
        // O desafio é de tentativa única: é consumido mesmo que a prova falhe.
        l.nonceB64 = null;

        if (nonce == null) {
            recusar(l, "PROVA sem desafio pendente");
            return;
        }
        if (l.id == null || !l.id.equals(prova.id())) {
            recusar(l, "PROVA para o id " + abreviar(prova.id())
                    + " numa ligação anunciada como " + abreviar(l.id));
            return;
        }

        String chaveB64;
        boolean primeiroRegisto;
        synchronized (this) {
            byte[] registada = replicas.get(l.id);
            primeiroRegisto = registada == null;
            if (primeiroRegisto) {
                if (l.chavePendenteB64 == null || l.tokenPendente == null) {
                    recusar(l, "PROVA de primeiro registo sem chave/token no OLA");
                    return;
                }
                chaveB64 = l.chavePendenteB64;
            } else {
                chaveB64 = Base64.getEncoder().encodeToString(registada);
            }
        }

        if (!verificarProva(l.id, nonce, prova.assinaturaB64(), chaveB64)) {
            recusar(l, "PROVA inválida para a réplica " + l.id);
            return;
        }

        if (primeiroRegisto) {
            // A chave pública só é aceite e o token só é consumido DEPOIS de
            // uma PROVA válida. A gravação em replicas.txt é atómica e precede
            // a alteração do estado em memória.
            synchronized (this) {
                if (replicas.containsKey(l.id)) {
                    recusar(l, "a réplica " + l.id + " foi registada entretanto por outra ligação");
                    return;
                }
                if (!tokenValido(l.tokenPendente)) {
                    recusar(l, "token expirado ou usado antes da PROVA para a réplica " + l.id);
                    return;
                }
                try {
                    Map<String, byte[]> novo = new HashMap<>(replicas);
                    novo.put(l.id, Base64.getDecoder().decode(l.chavePendenteB64));
                    gravarReplicas(novo);
                    replicas.put(l.id, novo.get(l.id));
                } catch (IOException e) {
                    recusar(l, "falha ao gravar " + FICHEIRO_REPLICAS + ": " + e.getMessage());
                    return;
                }
                tokens.remove(l.tokenPendente);
                l.tokenPendente = null;
                l.chavePendenteB64 = null;
                l.fase = Fase.AUTENTICADA;
            }
            registry.autenticada(l.id);
            registarEvento("REGISTO-REPLICA",
                    l.id + " registada em " + FICHEIRO_REPLICAS);
        } else {
            synchronized (this) {
                l.fase = Fase.AUTENTICADA;
                l.chavePendenteB64 = null;
                l.tokenPendente = null;
            }
            registry.autenticada(l.id);
        }
    }

    private void tratarSub(Ligacao l, Wire.Sub sub) {
        String topico = sub.topico();
        boolean permitido = topico.equals(Wire.topicReplica(l.id))
                || topico.equals(Wire.TOPICO_CTL);
        if (!permitido) {
            recusar(l, "réplica " + l.id + " tentou subscrever o tópico " + abreviar(topico));
            return;
        }
        synchronized (subscricoes) {
            subscricoes.computeIfAbsent(topico, t -> new HashSet<>()).add(l);
        }
        l.topicos.add(topico);
    }

    private void tratarPub(Ligacao l, Wire.Pub pub) {
        if (!Wire.TOPICO_LIDER.equals(pub.topico())) {
            recusar(l, "réplica " + l.id + " tentou publicar no tópico " + abreviar(pub.topico()));
            return;
        }
        try {
            aoReceber.accept(l.id, pub.mensagem());
        } catch (RuntimeException e) {
            registarErro("hub: falha ao entregar mensagem de " + l.id + " ao líder: " + e.getMessage());
        }
    }

    private void recusar(Ligacao l, String motivo) {
        registarErro("hub: " + motivo);
        fecharLigacao(l, motivo);
    }

    private void fecharLigacao(Ligacao l, String motivo) {
        if (l.fechada) return;
        l.fechada = true;
        removerLigacao(l);
        try {
            l.canal.fechar(1008, motivo);
        } catch (RuntimeException e) {
            registarErro("hub: falha ao fechar a ligação: " + e.getMessage());
        }
    }

    private void removerLigacao(Ligacao l) {
        synchronized (l) {
            ligacoes.remove(l);
            synchronized (subscricoes) {
                for (Set<Ligacao> s : subscricoes.values()) s.remove(l);
                subscricoes.values().removeIf(Set::isEmpty);
            }
            l.topicos.clear();
        }
    }

    // ------------------------------------------------------------------
    // Publicação (líder -> réplicas)
    // ------------------------------------------------------------------

    /**
     * Envia {@code PUB;<topico>;<mensagem>} apenas às ligações autenticadas
     * que subscreveram o tópico. Devolve o número de ligações alcançadas.
     */
    public int publish(String topico, String mensagem) {
        if (topico == null || topico.isEmpty()) throw new IllegalArgumentException("tópico vazio");
        if (mensagem == null) throw new IllegalArgumentException("mensagem nula");

        List<Ligacao> destinos = new ArrayList<>();
        synchronized (subscricoes) {
            Set<Ligacao> s = subscricoes.get(topico);
            if (s != null) destinos.addAll(s);
        }

        String trama = new Wire.Pub(topico, mensagem).encode();
        int enviadas = 0;
        for (Ligacao l : destinos) {
            if (l.fechada || l.fase != Fase.AUTENTICADA) continue;
            try {
                l.enviar(trama);
                enviadas++;
            } catch (RuntimeException e) {
                registarErro("hub: falha ao publicar em " + topico + ": " + e.getMessage());
            }
        }
        return enviadas;
    }

    // ------------------------------------------------------------------
    // Tokens e chaves
    // ------------------------------------------------------------------

    /** Gera um token de uso único, válido durante {@link Wire#TOKEN_VALIDADE_MS}. */
    public String gerarToken() {
        synchronized (tokens) {
            long agora = relogio.getAsLong();
            tokens.values().removeIf(criado -> agora - criado > Wire.TOKEN_VALIDADE_MS);

            byte[] bytes = new byte[TOKEN_BYTES];
            String token;
            do {
                rng.nextBytes(bytes);
                token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
            } while (tokens.containsKey(token));

            tokens.put(token, agora);
            return token;
        }
    }

    private boolean tokenValido(String token) {
        // Chamado com o lock do hub (this) ou de tokens; a leitura é curta.
        synchronized (tokens) {
            Long criado = tokens.get(token);
            if (criado == null) return false;
            return relogio.getAsLong() - criado <= Wire.TOKEN_VALIDADE_MS;
        }
    }

    /**
     * Verifica a assinatura Ed25519 de
     * {@code "tpf-registo|" + id + "|" + nonceB64} com a chave X.509 dada.
     * Qualquer erro de descodificação ou de criptografia devolve false.
     */
    public boolean verificarProva(String id, String nonceB64, String assinaturaB64, String chaveB64) {
        try {
            PublicKey chave = KeyFactory.getInstance("Ed25519")
                    .generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(chaveB64)));
            Signature s = Signature.getInstance("Ed25519");
            s.initVerify(chave);
            s.update((PREFIXO_ASSINATURA + id + "|" + nonceB64).getBytes(StandardCharsets.UTF_8));
            return s.verify(Base64.getDecoder().decode(assinaturaB64));
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            return false;
        }
    }

    private static byte[] decodificarChave(String chaveB64) {
        byte[] bytes = Base64.getDecoder().decode(chaveB64);
        try {
            KeyFactory.getInstance("Ed25519")
                    .generatePublic(new X509EncodedKeySpec(bytes));
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("não é uma chave Ed25519 X.509 válida");
        }
        return bytes;
    }

    /**
     * Revoga a chave de uma réplica: remove-a de replicas.txt, desliga as
     * ligações autenticadas com esse id e exclui-a da colocação. As cópias
     * que tinha ficam no catálogo (reconciliação no sprint seguinte).
     */
    public boolean removerReplica(String id) throws IOException {
        if (id == null || id.isEmpty()) return false;

        synchronized (this) {
            if (!replicas.containsKey(id)) return false;
            Map<String, byte[]> novo = new HashMap<>(replicas);
            novo.remove(id);
            gravarReplicas(novo);
            replicas.remove(id);
        }

        for (Ligacao l : new ArrayList<>(ligacoes)) {
            if (id.equals(l.id)) fecharLigacao(l, "réplica removida por /replica-remover");
        }
        registry.remover(id);
        return true;
    }

    /** True se a chave da réplica está registada. */
    public synchronized boolean registada(String id) {
        return replicas.containsKey(id);
    }

    /** Chave pública registada (cópia), ou null. */
    public synchronized byte[] chavePublica(String id) {
        byte[] k = replicas.get(id);
        return k == null ? null : k.clone();
    }

    // ------------------------------------------------------------------
    // replicas.txt
    // ------------------------------------------------------------------

    private void carregarReplicas() throws IOException {
        if (!Files.exists(ficheiroReplicas)) return;

        List<String> linhas = Files.readAllLines(ficheiroReplicas, StandardCharsets.UTF_8);
        int numero = 0;
        for (String linha : linhas) {
            numero++;
            if (linha.isEmpty()) {
                if (numero == linhas.size()) continue;
                throw new IOException(FICHEIRO_REPLICAS + ", linha " + numero + ": linha vazia");
            }
            String[] p = linha.split(";", -1);
            if (p.length != 2) {
                throw new IOException(FICHEIRO_REPLICAS + ", linha " + numero
                        + ": esperava id;chavePublicaB64");
            }
            if (!ID_REPLICA.matcher(p[0]).matches()) {
                throw new IOException(FICHEIRO_REPLICAS + ", linha " + numero
                        + ": identificador inválido: " + p[0]);
            }
            try {
                if (replicas.put(p[0], decodificarChave(p[1])) != null) {
                    throw new IOException(FICHEIRO_REPLICAS + ", linha " + numero
                            + ": réplica repetida (" + p[0] + ")");
                }
            } catch (IllegalArgumentException e) {
                throw new IOException(FICHEIRO_REPLICAS + ", linha " + numero + ": " + e.getMessage(), e);
            }
        }
    }

    private void gravarReplicas(Map<String, byte[]> mapa) throws IOException {
        Files.createDirectories(ficheiroReplicas.getParent());

        List<String> ids = new ArrayList<>(mapa.keySet());
        ids.sort(Comparator.naturalOrder());

        StringBuilder sb = new StringBuilder();
        for (String id : ids) {
            sb.append(id).append(';')
                    .append(Base64.getEncoder().encodeToString(mapa.get(id)))
                    .append('\n');
        }

        Path pasta = ficheiroReplicas.getParent();
        Path temporario = pasta.resolve("." + FICHEIRO_REPLICAS + "." + UUID.randomUUID() + ".tmp");
        try {
            try (FileChannel ch = FileChannel.open(temporario, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE)) {
                ByteBuffer buf = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
                while (buf.hasRemaining()) ch.write(buf);
                ch.force(true);
            }
            try {
                Files.move(temporario, ficheiroReplicas, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporario, ficheiroReplicas, StandardCopyOption.REPLACE_EXISTING);
            }
            LogRegistos.sincronizarPasta(pasta);
        } finally {
            Files.deleteIfExists(temporario);
        }
    }

    // ------------------------------------------------------------------
    // Auxiliares
    // ------------------------------------------------------------------

    /** Cria uma ligação de teste (sem socket) e regista-a na lista de ligações. */
    Ligacao novaLigacao(CanalLigacao canal) {
        if (canal == null) throw new IllegalArgumentException("canal nulo");
        Ligacao l = new Ligacao(canal);
        ligacoes.add(l);
        return l;
    }

    Set<Ligacao> subscritores(String topico) {
        synchronized (subscricoes) {
            Set<Ligacao> s = subscricoes.get(topico);
            return s == null ? Set.of() : Set.copyOf(s);
        }
    }

    int tokensAtivos() {
        synchronized (tokens) {
            return tokens.size();
        }
    }

    private void registarEvento(String evento, String detalhe) {
        eventos.registar(evento, detalhe);
        System.out.println(evento + " " + detalhe);
    }

    private void registarErro(String detalhe) {
        registarEvento("ERRO", detalhe);
    }

    private static String abreviar(String valor) {
        if (valor == null) return "?";
        return valor.length() <= 80 ? valor : valor.substring(0, 80) + "…";
    }

    /** Adaptador da API do Java-WebSocket ao canal mínimo usado pelo hub. */
    private static final class CanalWebSocket implements CanalLigacao {
        private final WebSocket conn;

        CanalWebSocket(WebSocket conn) { this.conn = conn; }

        @Override
        public void enviar(String mensagem) {
            if (conn.isOpen()) conn.send(mensagem);
        }

        @Override
        public void fechar(int codigo, String motivo) {
            if (!conn.isClosed()) conn.close(codigo, motivo);
        }
    }
}
