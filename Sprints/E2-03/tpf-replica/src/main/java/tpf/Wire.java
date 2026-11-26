package tpf;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Cópia mínima da {@code Wire} do líder para o processo da réplica.
 *
 * Os dois projetos são Maven independentes (o enunciado pede dois projetos),
 * por isso o protocolo é partilhado por contrato e não por dependência de
 * código: a réplica não deve arrastar JDA nem o resto do líder.
 *
 * Mensagens do E2-03:
 *
 *   OLA;<id>                                  réplica já registada
 *   OLA;<id>;<chavePublicaB64>;<token>        primeiro registo
 *   DESAFIO;<nonceB64>                        hub -> réplica (direto)
 *   PROVA;<id>;<assinaturaB64>                réplica -> hub
 *   SUB;<topico>                              réplica -> hub
 *   PUB;<topico>;<mensagem>                   hub -> réplica / réplica -> hub
 *   HB;<id>;<usados>;<quota>                  dentro de PUB;tpf-lider
 *   INVENTARIO;<id>;<chunkId>,...             dentro de PUB;tpf-lider
 *   ACK;<id>;<chunkId>;<sha256>               dentro de PUB;tpf-lider
 *   STORE;<chunkId>;<hdrB64>;<binB64>         dentro de PUB;tpf-r-<id>
 */
public final class Wire {

    public static final long PERIODO_HB_MS = Long.parseLong(System.getProperty("tpf.periodo", "2000"));
    public static final long BACKOFF_MIN_MS = Long.parseLong(System.getProperty("tpf.backoffMin", "1000"));
    public static final long BACKOFF_MAX_MS = Long.parseLong(System.getProperty("tpf.backoffMax", "30000"));

    public static final String TOPICO_LIDER = "tpf-lider";
    public static final String TOPICO_CTL = "tpf-ctl";

    private Wire() {}

    public static String topicReplica(String id) {
        return "tpf-r-" + id;
    }

    /** Prefixo da assinatura de registo: "tpf-registo|" + id + "|" + nonceB64. */
    public static String conteudoAssinatura(String id, String nonceB64) {
        return "tpf-registo|" + id + "|" + nonceB64;
    }

    // ------------------------------------------------------------------
    // Construção de mensagens
    // ------------------------------------------------------------------

    public static String ola(String id, String chavePublicaB64, String token) {
        if (chavePublicaB64 == null) return "OLA;" + id;
        StringBuilder sb = new StringBuilder("OLA;").append(id).append(';').append(chavePublicaB64);
        if (token != null) sb.append(';').append(token);
        return sb.toString();
    }

    public static String prova(String id, String assinaturaB64) {
        return "PROVA;" + id + ";" + assinaturaB64;
    }

    public static String sub(String topico) {
        return "SUB;" + topico;
    }

    public static String pub(String topico, String mensagem) {
        return "PUB;" + topico + ";" + mensagem;
    }

    public static String hb(String id, long usados, long quota) {
        return "HB;" + id + ";" + usados + ";" + quota;
    }

    public static String inventario(String id, Collection<String> chunkIds) {
        return "INVENTARIO;" + id + ";" + String.join(",", chunkIds);
    }

    public static String ack(String id, String chunkId, String sha256) {
        return "ACK;" + id + ";" + chunkId + ";" + sha256;
    }

    /** Primeiro campo de uma mensagem (o tipo). */
    public static String tipo(String mensagem) {
        if (mensagem == null) return "";
        int i = mensagem.indexOf(';');
        return i < 0 ? mensagem : mensagem.substring(0, i);
    }

    public static final class FormatoInvalido extends Exception {
        public FormatoInvalido(String mensagem) { super(mensagem); }
    }

    /** {@code DESAFIO;<nonceB64>}. */
    public record Desafio(String nonceB64) {
        public String encode() { return "DESAFIO;" + nonceB64; }

        public static Desafio decode(String linha) throws FormatoInvalido {
            String[] p = linha == null ? new String[0] : linha.split(";", -1);
            if (p.length != 2 || !"DESAFIO".equals(p[0]) || p[1].isEmpty()) {
                throw new FormatoInvalido("esperava DESAFIO;<nonceB64>");
            }
            return new Desafio(p[1]);
        }
    }

    /** {@code PUB;<topico>;<mensagem>} — a mensagem interna pode conter ';'. */
    public record Pub(String topico, String mensagem) {
        public String encode() { return "PUB;" + topico + ";" + mensagem; }

        public static Pub decode(String linha) throws FormatoInvalido {
            String[] p = linha == null ? new String[0] : linha.split(";", 3);
            if (p.length != 3 || !"PUB".equals(p[0]) || p[1].isEmpty()) {
                throw new FormatoInvalido("esperava PUB;<topico>;<mensagem>");
            }
            return new Pub(p[1], p[2]);
        }
    }

    /** {@code STORE;<chunkId>;<hdrB64>;<binB64>}. */
    public record Store(String chunkId, String hdrB64, String binB64) {
        public String encode() { return "STORE;" + chunkId + ";" + hdrB64 + ";" + binB64; }

        public static Store decode(String linha) throws FormatoInvalido {
            String[] p = linha == null ? new String[0] : linha.split(";", -1);
            if (p.length != 4 || !"STORE".equals(p[0])
                    || p[1].isEmpty() || p[2].isEmpty() || p[3].isEmpty()) {
                throw new FormatoInvalido("esperava STORE;<chunkId>;<hdrB64>;<binB64>");
            }
            return new Store(p[1], p[2], p[3]);
        }
    }

    /** Conjunto de chunks sem repetições, preservando a ordem (para o INVENTARIO). */
    public static Set<String> semRepeticoes(Collection<String> ids) {
        return new LinkedHashSet<>(ids);
    }
}
