package raft04;

/**
 * VoteRequest = a mensagem PEDIDO_VOTO que um CANDIDATO publica no tópico da eleição.
 *
 *   PEDIDO_VOTO;<termo>;<candidato>;<ultimoSeq>;<ultimoTermo>
 *
 * O candidato diz em que termo se candidata e qual é o seu último registo
 * (seq e termo), para os outros nós aplicarem a restrição de voto.
 */
public final class VoteRequest {
    public final long term;        // termo da candidatura
    public final int candidate;    // identificador (1..G) do candidato
    public final long lastSeq;     // seq do último registo do candidato
    public final long lastTerm;    // termo do último registo do candidato

    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    // objeto -> texto
    public String encode() {
        return "PEDIDO_VOTO;" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }

    // texto -> objeto (lança IllegalArgumentException/NumberFormatException se estiver malformado)
    public static VoteRequest decode(String msg) {
        String[] f = msg.trim().split(";");
        if (f.length != 5 || !f[0].equals("PEDIDO_VOTO")) {
            throw new IllegalArgumentException("PEDIDO_VOTO malformado: " + msg);
        }
        return new VoteRequest(Long.parseLong(f[1]), Integer.parseInt(f[2]),
                Long.parseLong(f[3]), Long.parseLong(f[4]));
    }
}
