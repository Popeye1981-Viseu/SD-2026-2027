package tpf;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.Set;

/**
 * Classe utilitária que centraliza a configuração do sistema, a gestão da
 * master.key e o protocolo textual do arquivo distribuído.
 *
 * Todos os parâmetros podem ser alterados por propriedades do sistema com o
 * prefixo {@code tpf.} (ex.: {@code -Dtpf.r=3}).
 */
public final class Wire {

    // ------------------------------------------------------------------
    // E2-02: escrita local (chunk, padding e selagem)
    // ------------------------------------------------------------------

    // Parâmetros configuráveis via propriedades do sistema (ex: -Dtpf.chunkMax=1024)
    public static final long CHUNK_MAX_BYTES = Long.parseLong(System.getProperty("tpf.chunkMax", "65536"));
    public static final long PAD_BYTES = Long.parseLong(System.getProperty("tpf.pad", "4096"));
    public static final long SELAGEM_MS = Long.parseLong(System.getProperty("tpf.selagem", "5000"));

    // ------------------------------------------------------------------
    // E2-03: hub, réplicas, colocação e catálogo
    // ------------------------------------------------------------------

    /** Porto por omissão do hub WebSocket. */
    public static final int PORTO = Integer.parseInt(System.getProperty("tpf.porto", "8765"));

    /** Período entre heartbeats de cada réplica. */
    public static final long PERIODO_HB_MS = Long.parseLong(System.getProperty("tpf.periodo", "2000"));

    /** Silêncio a partir do qual uma réplica passa a SUSPEITA (3 heartbeats). */
    public static final long SUSPEITA_MS = Long.parseLong(System.getProperty("tpf.suspeita", "6000"));

    /** Silêncio a partir do qual uma réplica passa a MORTA (5 heartbeats). */
    public static final long MORTA_MS = Long.parseLong(System.getProperty("tpf.morta", "10000"));

    /** Validade de um token de registo (10 minutos). */
    public static final long TOKEN_VALIDADE_MS =
            Long.parseLong(System.getProperty("tpf.tokenValidade", "600000"));

    /** Fator de replicação: cada chunk tem exatamente R cópias. */
    public static final int R = Integer.parseInt(System.getProperty("tpf.r", "3"));

    /** Mínimo absoluto de ACK válidos para um chunk ser DURAVEL. */
    public static final int W_MIN = Integer.parseInt(System.getProperty("tpf.w", "2"));

    /** Prazo para os ACK de uma ronda de STORE. */
    public static final long ACK_TIMEOUT_MS = Long.parseLong(System.getProperty("tpf.ackTimeout", "4000"));

    /** Número máximo de rondas de colocação por chunk (a primeira conta como 1). */
    public static final int MAX_RONDAS = Integer.parseInt(System.getProperty("tpf.maxRondas", "3"));

    /** Margem de segurança exigida a cada réplica para receber um chunk. */
    public static final long MARGEM_BYTES = Long.parseLong(System.getProperty("tpf.margem", "65536"));

    /** Expoente da ponderação pelo espaço livre. */
    public static final double ALFA = Double.parseDouble(System.getProperty("tpf.alfa", "1.0"));

    /** Espera mínima de religação de uma réplica. */
    public static final long BACKOFF_MIN_MS = Long.parseLong(System.getProperty("tpf.backoffMin", "1000"));

    /** Espera máxima de religação de uma réplica. */
    public static final long BACKOFF_MAX_MS = Long.parseLong(System.getProperty("tpf.backoffMax", "30000"));

    /** Tópico em que as réplicas publicam e o líder recebe em processo. */
    public static final String TOPICO_LIDER = "tpf-lider";

    /** Tópico de controlo, subscrito por todas as réplicas. */
    public static final String TOPICO_CTL = "tpf-ctl";

    // Construtor privado para impedir a instanciação (classe puramente estática)
    private Wire() {}

    /** Tópico privado de uma réplica: {@code tpf-r-<id>}. */
    public static String topicReplica(String id) {
        return "tpf-r-" + id;
    }

    /**
     * Quórum de durabilidade: {@code max(W_MIN, ⌊R/2⌋ + 1)}.
     * É 2 com R = 3 e 3 com R = 5; nunca é uma constante isolada.
     */
    public static int quorumDurabilidade() {
        return quorumDurabilidade(R, W_MIN);
    }

    /** Variante paramétrica de {@link #quorumDurabilidade()} (usada pelos testes). */
    public static int quorumDurabilidade(int r, int wMin) {
        if (r < 1) throw new IllegalArgumentException("R tem de ser >= 1");
        if (wMin < 1) throw new IllegalArgumentException("W_MIN tem de ser >= 1");
        return Math.max(wMin, r / 2 + 1);
    }

    // ------------------------------------------------------------------
    // Mensagens do protocolo (E2-03)
    // ------------------------------------------------------------------
    //
    // Formato comum: texto numa trama WebSocket, campos separados por ';',
    // bytes binários em Base64. O hub acrescenta SUB e PUB para encaminhar.

    /** A mensagem não respeita o formato do protocolo. */
    public static final class FormatoInvalido extends Exception {
        public FormatoInvalido(String mensagem) { super(mensagem); }
    }

    /** Primeiro campo de uma mensagem (o seu tipo), sem validar o resto. */
    public static String tipo(String mensagem) {
        if (mensagem == null) return "";
        int i = mensagem.indexOf(';');
        return i < 0 ? mensagem : mensagem.substring(0, i);
    }

    /**
     * Divide a mensagem e valida o tipo e o número de campos.
     * {@code max == -1} significa "sem limite".
     */
    private static String[] partes(String linha, String tipo, int min, int max)
            throws FormatoInvalido {
        if (linha == null) throw new FormatoInvalido("mensagem nula");
        String[] p = linha.split(";", -1);
        if (p.length == 0 || !tipo.equals(p[0])) {
            throw new FormatoInvalido("esperava " + tipo + ", recebi "
                    + (p.length == 0 ? "" : p[0]));
        }
        if (p.length < min || (max >= 0 && p.length > max)) {
            throw new FormatoInvalido("mensagem " + tipo + " com " + p.length
                    + " campos (esperava " + min + (max == min ? "" : ".." + (max < 0 ? "n" : max)) + ")");
        }
        return p;
    }

    private static long inteiroNaoNegativo(String valor, String campo) throws FormatoInvalido {
        try {
            long v = Long.parseLong(valor);
            if (v < 0) throw new FormatoInvalido("campo " + campo + " negativo: " + v);
            return v;
        } catch (NumberFormatException e) {
            throw new FormatoInvalido("campo " + campo + " não é inteiro: " + valor);
        }
    }

    private static String obrigatorio(String valor, String campo) throws FormatoInvalido {
        if (valor == null || valor.isEmpty()) {
            throw new FormatoInvalido("campo " + campo + " vazio");
        }
        return valor;
    }

    /**
     * {@code OLA;<id>} (réplica já registada) ou
     * {@code OLA;<id>;<chavePublicaB64>;<token>} (primeiro registo).
     */
    public record Ola(String id, String chavePublicaB64, String token) {

        public boolean primeiroRegisto() {
            return chavePublicaB64 != null;
        }

        public String encode() {
            StringBuilder sb = new StringBuilder("OLA;").append(id);
            if (chavePublicaB64 != null) {
                sb.append(';').append(chavePublicaB64);
                if (token != null) sb.append(';').append(token);
            }
            return sb.toString();
        }

        public static Ola decode(String linha) throws FormatoInvalido {
            String[] p = partes(linha, "OLA", 2, 4);
            String id = obrigatorio(p[1], "id");
            String chave = p.length >= 3 && !p[2].isEmpty() ? p[2] : null;
            String token = p.length >= 4 && !p[3].isEmpty() ? p[3] : null;
            if (chave == null && token != null) {
                throw new FormatoInvalido("OLA com token mas sem chave pública");
            }
            return new Ola(id, chave, token);
        }
    }

    /** {@code DESAFIO;<nonceB64>} — enviado pelo hub, diretamente à ligação. */
    public record Desafio(String nonceB64) {

        public String encode() {
            return "DESAFIO;" + nonceB64;
        }

        public static Desafio decode(String linha) throws FormatoInvalido {
            String[] p = partes(linha, "DESAFIO", 2, 2);
            return new Desafio(obrigatorio(p[1], "nonce"));
        }
    }

    /** {@code PROVA;<id>;<assinaturaB64>} — assinatura de "tpf-registo|id|nonce". */
    public record Prova(String id, String assinaturaB64) {

        public String encode() {
            return "PROVA;" + id + ";" + assinaturaB64;
        }

        public static Prova decode(String linha) throws FormatoInvalido {
            String[] p = partes(linha, "PROVA", 3, 3);
            return new Prova(obrigatorio(p[1], "id"), obrigatorio(p[2], "assinatura"));
        }
    }

    /** {@code SUB;<topico>} — subscrição feita por uma réplica autenticada. */
    public record Sub(String topico) {

        public String encode() {
            return "SUB;" + topico;
        }

        public static Sub decode(String linha) throws FormatoInvalido {
            String[] p = partes(linha, "SUB", 2, 2);
            return new Sub(obrigatorio(p[1], "tópico"));
        }
    }

    /**
     * {@code PUB;<topico>;<mensagem>} — mensagem encaminhada pelo hub.
     * A mensagem interna pode conter ';' (ex.: HB e ACK), por isso o split é
     * feito em 3 partes, não em todas.
     */
    public record Pub(String topico, String mensagem) {

        public String encode() {
            return "PUB;" + topico + ";" + mensagem;
        }

        public static Pub decode(String linha) throws FormatoInvalido {
            if (linha == null) throw new FormatoInvalido("mensagem nula");
            String[] p = linha.split(";", 3);
            if (p.length != 3 || !"PUB".equals(p[0])) {
                throw new FormatoInvalido("esperava PUB;<topico>;<mensagem>");
            }
            return new Pub(obrigatorio(p[1], "tópico"), p[2]);
        }
    }

    /** {@code HB;<id>;<usados>;<quota>} — dentro de {@code PUB;tpf-lider;...}. */
    public record Hb(String id, long usados, long quota) {

        public String encode() {
            return "HB;" + id + ";" + usados + ";" + quota;
        }

        public static Hb decode(String linha) throws FormatoInvalido {
            String[] p = partes(linha, "HB", 4, 4);
            return new Hb(obrigatorio(p[1], "id"),
                    inteiroNaoNegativo(p[2], "usados"),
                    inteiroNaoNegativo(p[3], "quota"));
        }
    }

    /** {@code ACK;<id>;<chunkId>;<sha256>} — dentro de {@code PUB;tpf-lider;...}. */
    public record Ack(String id, String chunkId, String sha256) {

        public String encode() {
            return "ACK;" + id + ";" + chunkId + ";" + sha256;
        }

        public static Ack decode(String linha) throws FormatoInvalido {
            String[] p = partes(linha, "ACK", 4, 4);
            return new Ack(obrigatorio(p[1], "id"),
                    obrigatorio(p[2], "chunkId"),
                    obrigatorio(p[3], "sha256"));
        }
    }

    /** {@code STORE;<chunkId>;<hdrB64>;<binB64>} — pedido dirigido a uma réplica. */
    public record Store(String chunkId, String hdrB64, String binB64) {

        public String encode() {
            return "STORE;" + chunkId + ";" + hdrB64 + ";" + binB64;
        }

        public static Store decode(String linha) throws FormatoInvalido {
            String[] p = partes(linha, "STORE", 4, 4);
            return new Store(obrigatorio(p[1], "chunkId"),
                    obrigatorio(p[2], "hdr"),
                    obrigatorio(p[3], "bin"));
        }
    }

    // ------------------------------------------------------------------
    // master.key (E2-02)
    // ------------------------------------------------------------------

    /**
     * Exceção personalizada para isolar falhas relacionadas com a master.key.
     * Permite que os testes unitários capturem o erro sem que a JVM seja terminada.
     */
    public static class MasterKeyException extends Exception {
        public MasterKeyException(String message) { super(message); }
        public MasterKeyException(String message, Throwable cause) { super(message, cause); }
    }

    /**
     * Carrega ou cria a master.key no caminho indicado.
     * @param caminho O caminho para o ficheiro (permite testes com pastas temporárias).
     * @return Um array de 32 bytes com a chave.
     * @throws MasterKeyException Se houver falha de I/O, se o ficheiro for inválido ou se a proteção falhar.
     */
    public static byte[] carregarOuCriarMasterKey(Path caminho) throws MasterKeyException {
        boolean criadaNesteArranque = false;

        // 1. Se o ficheiro já existe, apenas o lemos e validamos (NUNCA o apagamos).
        if (Files.exists(caminho)) {
            return lerChaveValida(caminho);
        }

        // 2. Se não existe, tentamos criá-lo atomicamente com permissões restritas.
        try {
            // Tenta usar POSIX (Linux/macOS) para garantir permissões "rw-------" (só o dono).
            Set<java.nio.file.attribute.PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");
            Files.createFile(caminho, PosixFilePermissions.asFileAttribute(perms));
            criadaNesteArranque = true;
        } catch (FileAlreadyExistsException e) {
            // CONDIÇÃO DE CORRIDA: Outro processo criou o ficheiro entre o Files.exists e o createFile.
            // Não abortamos. Assumimos que a chave é válida e tentamos lê-la.
            return lerChaveValida(caminho);
        } catch (UnsupportedOperationException e) {
            // FALLBACK WINDOWS: O sistema não suporta POSIX.
            try {
                Files.createFile(caminho);
                criadaNesteArranque = true;
                // Aplica permissões manuais para Windows e verifica se foram aceites.
                aplicarPermissoesWindows(caminho.toFile());
            } catch (FileAlreadyExistsException ex) {
                // Mesma condição de corrida, mas no Windows.
                return lerChaveValida(caminho);
            } catch (IOException ex) {
                // Se falhou a proteção, e o ficheiro foi criado por nós, limpamos o rasto.
                if (criadaNesteArranque) apagarFicheiroSeguro(caminho);
                throw new MasterKeyException("Falha ao criar ou proteger master.key no Windows.", ex);
            }
        } catch (IOException e) {
            throw new MasterKeyException("Falha ao criar a master.key.", e);
        }

        // 3. O ficheiro é nosso e está protegido. Geramos os 32 bytes e escrevemos.
        try {
            byte[] novaChave = new byte[32];
            new SecureRandom().nextBytes(novaChave); // Gerador criptograficamente seguro
            Files.write(caminho, novaChave);
            return novaChave;
        } catch (IOException e) {
            // Se a escrita falhar, o ficheiro fica a 0 bytes. Limpamos se fomos nós a criá-lo.
            if (criadaNesteArranque) apagarFicheiroSeguro(caminho);
            throw new MasterKeyException("Falha ao escrever na master.key.", e);
        }
    }

    /**
     * Lê o ficheiro e valida se tem exatamente 32 bytes.
     * Usa um ciclo de retry para lidar com processos que criaram o ficheiro mas ainda não escreveram os dados.
     */
    private static byte[] lerChaveValida(Path caminho) throws MasterKeyException {
        for (int i = 0; i < 5; i++) {
            try {
                byte[] chave = Files.readAllBytes(caminho);
                if (chave.length == 32) {
                    return chave;
                }
                // Se a leitura funcionou mas o tamanho é errado, pode ser uma escrita em curso.
                // Dormimos 50ms e tentamos novamente.
                Thread.sleep(50);
            } catch (IOException e) {
                // Falhas de I/O (ex: ficheiro apagado, sem permissões) abortam imediatamente. Não há retry.
                throw new MasterKeyException("Falha de I/O ao ler a master.key.", e);
            } catch (InterruptedException e) {
                // Restaura a flag de interrupção e aborta. O sleep foi interrompido.
                Thread.currentThread().interrupt();
                throw new MasterKeyException("Leitura da master.key interrompida.", e);
            }
        }
        // Se após 5 tentativas o ficheiro continuar a ter o tamanho errado, é inválido.
        throw new MasterKeyException("A master.key existente não tem 32 bytes após 5 tentativas.");
    }

    /**
     * Fallback de segurança para Windows.
     * O java.io.File não garante a aplicação das permissões, por isso verificamos os booleanos.
     */
    private static void aplicarPermissoesWindows(File ficheiro) throws IOException {
        // Remove acesso a todos, depois concede apenas ao dono.
        boolean outrosNaoLeem = ficheiro.setReadable(false, false);
        boolean outrosNaoEscrevem = ficheiro.setWritable(false, false);
        boolean donoLe = ficheiro.setReadable(true, true);
        boolean donoEscreve = ficheiro.setWritable(true, true);

        // Se o SO recusou alguma das operações, a segurança está comprometida.
        if (!outrosNaoLeem || !outrosNaoEscrevem || !donoLe || !donoEscreve) {
            throw new IOException("O sistema operativo recusou a aplicação de permissões restritas.");
        }
    }

    /**
     * Apaga o ficheiro de forma segura, alertando o administrador se não for possível.
     */
    private static void apagarFicheiroSeguro(Path caminho) {
        try {
            Files.deleteIfExists(caminho);
        } catch (IOException e) {
            // Imprime um bloco de erro altamente visível para não passar despercebido.
            System.err.println("==================================================");
            System.err.println("ERRO CRÍTICO: NÃO FOI POSSÍVEL APAGAR A MASTER.KEY COMPROMETIDA!");
            System.err.println("Caminho: " + caminho.toAbsolutePath());
            System.err.println("Causa: " + e.getMessage());
            System.err.println("AÇÃO OBRIGATÓRIA: Apague este ficheiro manualmente antes de reiniciar.");
            System.err.println("==================================================");
        }
    }
}
