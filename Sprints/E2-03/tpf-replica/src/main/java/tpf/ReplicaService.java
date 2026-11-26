package tpf;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Réplica do arquivo distribuído (E2-03, objetivo 4).
 *
 *   ReplicaService <id> <url> <quotaBytes> [token]
 *
 * A réplica guarda apenas bytes cifrados. Nunca recebe nem guarda a master
 * key, o catálogo em claro ou registos em claro.
 *
 * Ciclo de vida:
 *  - lê ou cria o par Ed25519 em dados-<id>/ (PKCS#8 + X.509, permissões 600);
 *  - liga-se ao hub com java.net.http.WebSocket e envia OLA (com chave e
 *    token no primeiro registo);
 *  - responde ao DESAFIO com PROVA Ed25519 e subscreve tpf-r-<id> e tpf-ctl;
 *  - publica INVENTARIO depois de cada ligação e HB a cada PERIODO_HB_MS;
 *  - em STORE verifica quota e hash, grava .bin/.hdr com ATOMIC_MOVE e só
 *    depois publica ACK;
 *  - se a ligação cair, volta a ligar com backoff exponencial 1 s -> 30 s.
 *
 * Eventos em eventos-<id>.txt:
 *   LIGADO, DESLIGADO, RELIGAR <ms>, STORE, QUOTA, ERRO (e ARRANQUE).
 */
public final class ReplicaService {

    private static final Pattern ID_REPLICA = Pattern.compile("[A-Za-z0-9_-]{1,32}");

    private final String id;
    private final URI url;
    private final long quota;
    private final String token;                 // null => OLA;<id> (já registada)
    private final KeyPair chaves;
    private final Armazem armazem;
    private final EventosReplica eventos;

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private final ScheduledExecutorService agendador = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "tpf-replica-agendador");
        t.setDaemon(true);
        return t;
    });

    private final ExecutorService processador = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "tpf-replica-processador");
        t.setDaemon(true);
        return t;
    });

    private final AtomicLong atraso = new AtomicLong(Wire.BACKOFF_MIN_MS);
    private final AtomicBoolean reagendado = new AtomicBoolean(false);
    private final CountDownLatch terminado = new CountDownLatch(1);

    private volatile WebSocket webSocket;
    private volatile SessaoEnvio envio;
    private volatile boolean ligado;
    private volatile boolean parado;

    ReplicaService(String id, URI url, long quota, String token,
                   KeyPair chaves, Armazem armazem, EventosReplica eventos) {
        this.id = id;
        this.url = url;
        this.quota = quota;
        this.token = token;
        this.chaves = chaves;
        this.armazem = armazem;
        this.eventos = eventos;
    }

    // ------------------------------------------------------------------
    // Arranque
    // ------------------------------------------------------------------

    public static void main(String[] args) {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Uso: ReplicaService <id> <url> <quotaBytes> [token]");
            System.err.println("Exemplo: ReplicaService A ws://localhost:8765 8388608 <token>");
            System.exit(2);
            return;
        }

        String id = args[0];
        if (!ID_REPLICA.matcher(id).matches()) {
            System.err.println("Identificador inválido: use só letras, dígitos, '_' e '-', até 32 caracteres.");
            System.exit(2);
            return;
        }

        URI url;
        try {
            url = URI.create(args[1]);
        } catch (IllegalArgumentException e) {
            System.err.println("URL inválido: " + args[1]);
            System.exit(2);
            return;
        }
        if (!"ws".equalsIgnoreCase(url.getScheme()) && !"wss".equalsIgnoreCase(url.getScheme())) {
            System.err.println("O URL tem de usar ws:// ou wss:// (recebido: " + args[1] + ").");
            System.exit(2);
            return;
        }

        long quota;
        try {
            quota = Long.parseLong(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("quotaBytes não é um inteiro: " + args[2]);
            System.exit(2);
            return;
        }
        if (quota < 0) {
            System.err.println("quotaBytes tem de ser >= 0 (recebido: " + quota + ").");
            System.exit(2);
            return;
        }

        String token = args.length == 4 ? args[3] : null;
        if (token != null && token.isBlank()) token = null;

        Path pastaDados = Path.of("dados-" + id);
        try {
            EventosReplica eventos = new EventosReplica(Path.of("eventos-" + id + ".txt"));
            Chaves.Par par = Chaves.carregarOuCriar(pastaDados);
            Armazem armazem = new Armazem(pastaDados, quota, eventos);

            eventos.registarEImprimir("ARRANQUE", "id=" + id + " url=" + url + " quota=" + quota
                    + " chaves=" + (par.criado() ? "criadas" : "carregadas")
                    + " usados=" + armazem.usados() + " chunks=" + armazem.inventario().size());

            if (par.criado() && token == null) {
                System.err.println("AVISO: chaves novas e sem token. Se este id ainda não está em"
                        + " replicas.txt, o hub recusa o OLA. Gere um token com /replica-token.");
            }
            if (!par.criado() && token != null) {
                System.err.println("AVISO: está a enviar token com chaves já existentes; o hub só o"
                        + " usa se o id ainda não estiver registado.");
            }

            ReplicaService servico = new ReplicaService(id, url, quota, token,
                    par.chaves(), armazem, eventos);
            Runtime.getRuntime().addShutdownHook(new Thread(servico::fechar, "tpf-replica-shutdown"));
            servico.arrancar();
            servico.aguardar();

        } catch (IOException | GeneralSecurityException e) {
            System.err.println("ERRO FATAL NO ARRANQUE: " + e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void arrancar() {
        // Heartbeat periódico: só é enviado enquanto a ligação estiver aberta.
        agendador.scheduleAtFixedRate(() -> {
            if (!ligado || parado) return;
            try {
                enviar(Wire.pub(Wire.TOPICO_LIDER,
                        Wire.hb(id, armazem.usados(), quota)));
            } catch (RuntimeException e) {
                eventos.registarEImprimir("ERRO", "heartbeat: " + e.getMessage());
            }
        }, Wire.PERIODO_HB_MS, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);

        ligar();
    }

    private void aguardar() throws InterruptedException {
        terminado.await();
    }

    // ------------------------------------------------------------------
    // Ligação e religação
    // ------------------------------------------------------------------

    private void ligar() {
        if (parado) return;
        try {
            http.newWebSocketBuilder()
                    .connectTimeout(Duration.ofSeconds(5))
                    .buildAsync(url, new Ouvinte())
                    .whenComplete((ws, erro) -> {
                        if (erro != null) {
                            eventos.registarEImprimir("ERRO",
                                    "falha ao ligar a " + url + ": " + mensagem(erro));
                            programarReligacao();
                        }
                    });
        } catch (RuntimeException e) {
            eventos.registarEImprimir("ERRO", "falha ao ligar a " + url + ": " + mensagem(e));
            programarReligacao();
        }
    }

    /**
     * Backoff exponencial 1 s -> 30 s. Uma ligação bem-sucedida (onOpen) repõe
     * o atraso em BACKOFF_MIN_MS. A flag `reagendado` garante que onClose e
     * onError não originam duas ligações em paralelo.
     */
    private void programarReligacao() {
        if (parado) return;
        if (!reagendado.compareAndSet(false, true)) return;

        long espera = atraso.get();
        atraso.set(Math.min(espera * 2, Wire.BACKOFF_MAX_MS));
        eventos.registarEImprimir("RELIGAR", String.valueOf(espera));

        agendador.schedule(() -> {
            reagendado.set(false);
            ligar();
        }, espera, TimeUnit.MILLISECONDS);
    }

    private void desligar(String motivo) {
        WebSocket ws;
        SessaoEnvio s;
        synchronized (this) {
            if (!ligado && webSocket == null) return;   // já desligado
            ws = webSocket;
            s = envio;
            webSocket = null;
            envio = null;
            ligado = false;
        }
        if (s != null) s.fechar();
        if (ws != null) {
            try {
                ws.abort();
            } catch (RuntimeException ignorada) {
                // a fechar
            }
        }
        eventos.registarEImprimir("DESLIGADO", motivo);
        programarReligacao();
    }

    /** A ligação está aberta: envia OLA (chave+token no primeiro registo). */
    private void aoAbrir(WebSocket ws) {
        synchronized (this) {
            if (parado) {
                ws.abort();
                return;
            }
            this.webSocket = ws;
            this.envio = new SessaoEnvio(ws, this::falhaEnvio);
            this.ligado = true;
            this.atraso.set(Wire.BACKOFF_MIN_MS);      // ligação bem-sucedida
        }

        eventos.registarEImprimir("LIGADO", url.toString());

        String chaveB64 = Base64.getEncoder().encodeToString(chaves.getPublic().getEncoded());
        enviar(Wire.ola(id, token == null ? null : chaveB64, token));
    }

    private void falhaEnvio(Throwable erro) {
        eventos.registarEImprimir("ERRO", "falha de envio: " + mensagem(erro));
        desligar("falha de envio");
    }

    private static Throwable raiz(Throwable t) {
        Throwable atual = t;
        while (atual.getCause() != null && atual.getCause() != atual) atual = atual.getCause();
        return atual;
    }

    /** Mensagem legível mesmo quando a exceção raiz não tem mensagem (ex.: ConnectException). */
    private static String mensagem(Throwable t) {
        Throwable r = raiz(t);
        String m = r.getMessage();
        return m == null || m.isBlank() ? r.getClass().getSimpleName() : m;
    }

    private void enviar(String mensagem) {
        SessaoEnvio s = envio;
        if (s != null && ligado) s.enviar(mensagem);
    }

    // ------------------------------------------------------------------
    // Receção de mensagens
    // ------------------------------------------------------------------

    private void processar(String mensagem) {
        try {
            switch (Wire.tipo(mensagem)) {
                case "DESAFIO" -> tratarDesafio(Wire.Desafio.decode(mensagem));
                case "PUB" -> tratarPub(Wire.Pub.decode(mensagem));
                default -> eventos.registarEImprimir("ERRO",
                        "mensagem inesperada do hub: " + Wire.tipo(mensagem));
            }
        } catch (Wire.FormatoInvalido e) {
            eventos.registarEImprimir("ERRO", "mensagem inválida do hub: " + e.getMessage());
        } catch (RuntimeException e) {
            eventos.registarEImprimir("ERRO", "falha ao processar mensagem: " + e.getMessage());
        }
    }

    private void tratarDesafio(Wire.Desafio desafio) {
        try {
            String assinatura = Chaves.assinar(chaves.getPrivate(), id, desafio.nonceB64());

            // Ordem importante: o hub processa as tramas pela ordem de chegada,
            // por isso PROVA tem de preceder SUB e PUB.
            enviar(Wire.prova(id, assinatura));
            enviar(Wire.sub(Wire.topicReplica(id)));
            enviar(Wire.sub(Wire.TOPICO_CTL));

            // O inventário completo e o primeiro heartbeat vão depois da PROVA.
            enviar(Wire.pub(Wire.TOPICO_LIDER,
                    Wire.inventario(id, armazem.inventario())));
            enviar(Wire.pub(Wire.TOPICO_LIDER,
                    Wire.hb(id, armazem.usados(), quota)));

        } catch (GeneralSecurityException | IOException e) {
            eventos.registarEImprimir("ERRO", "não foi possível responder ao desafio: "
                    + e.getMessage());
        }
    }

    private void tratarPub(Wire.Pub pub) {
        if (!pub.topico().equals(Wire.topicReplica(id))) {
            if (pub.topico().equals(Wire.TOPICO_CTL)) {
                // Difusão futura (INVENTARIO? / MANIFESTO?). Neste sprint não há ações.
                return;
            }
            eventos.registarEImprimir("ERRO",
                    "PUB para o tópico de outra réplica: " + pub.topico());
            return;
        }

        try {
            if (!"STORE".equals(Wire.tipo(pub.mensagem()))) {
                eventos.registarEImprimir("ERRO",
                        "mensagem inesperada no tópico privado: " + Wire.tipo(pub.mensagem()));
                return;
            }
            tratarStore(Wire.Store.decode(pub.mensagem()));
        } catch (Wire.FormatoInvalido e) {
            eventos.registarEImprimir("ERRO", "STORE inválido: " + e.getMessage());
        }
    }

    private void tratarStore(Wire.Store store) {
        byte[] hdr;
        byte[] bin;
        try {
            hdr = Base64.getDecoder().decode(store.hdrB64());
            bin = Base64.getDecoder().decode(store.binB64());
        } catch (IllegalArgumentException e) {
            eventos.registarEImprimir("ERRO",
                    "STORE com Base64 inválido para " + store.chunkId());
            return;
        }

        Armazem.Rececao r = armazem.receber(store.chunkId(), hdr, bin);
        if (r.resultado() == Armazem.Resultado.OK) {
            // SÓ DEPOIS de os ficheiros estarem duráveis se publica o ACK.
            enviar(Wire.pub(Wire.TOPICO_LIDER,
                    Wire.ack(id, store.chunkId(), r.sha256())));
        }
    }

    // ------------------------------------------------------------------
    // Fecho
    // ------------------------------------------------------------------

    void fechar() {
        synchronized (this) {
            if (parado) return;
            parado = true;
        }
        SessaoEnvio s = envio;
        if (s != null) s.fechar();
        WebSocket ws = webSocket;
        if (ws != null) {
            try {
                ws.abort();
            } catch (RuntimeException ignorada) {
                // a terminar
            }
        }
        eventos.registarEImprimir("DESLIGADO", "shutdown");
        agendador.shutdownNow();
        processador.shutdownNow();
        try {
            eventos.close();
        } catch (IOException ignorada) {
            // a terminar
        }
        terminado.countDown();
    }

    // ------------------------------------------------------------------
    // WebSocket do JDK
    // ------------------------------------------------------------------

    private final class Ouvinte implements WebSocket.Listener {
        private final StringBuilder acumulado = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            aoAbrir(webSocket);
            webSocket.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            acumulado.append(data);
            if (last) {
                String mensagem = acumulado.toString();
                acumulado.setLength(0);
                processador.execute(() -> processar(mensagem));
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable erro) {
            eventos.registarEImprimir("ERRO", "WebSocket: " + mensagem(erro));
            try {
                webSocket.abort();
            } catch (RuntimeException ignorada) {
                // já fechado
            }
            desligar("erro de WebSocket: " + mensagem(erro));
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            desligar("ligação fechada (" + statusCode
                    + (reason == null || reason.isBlank() ? "" : ": " + reason) + ")");
            return null;
        }
    }

    /**
     * Fila de saída com uma única thread: o JDK só permite um sendText
     * pendente de cada vez e a ordem (PROVA, SUB, PUB) tem de ser preservada.
     * A thread não pode ser a do Listener, para o join() não bloquear o
     * seletor que completa os envios.
     */
    private static final class SessaoEnvio {
        private final BlockingQueue<String> fila = new LinkedBlockingQueue<>();
        private final Thread thread;

        SessaoEnvio(WebSocket ws, Consumer<Throwable> aoFalhar) {
            this.thread = new Thread(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        String mensagem = fila.take();
                        ws.sendText(mensagem, true).join();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    aoFalhar.accept(e);
                }
            }, "tpf-replica-envio");
            thread.setDaemon(true);
            thread.start();
        }

        void enviar(String mensagem) {
            if (!thread.isInterrupted()) fila.offer(mensagem);
        }

        void fechar() {
            thread.interrupt();
        }
    }
}
