package raft04;

/**
 * VoteReply = a mensagem VOTO que um nó publica em resposta a um PEDIDO_VOTO.
 *
 *   VOTO;<termo>;<eleitor>;<candidato>;<S|N>
 *
 * S = voto concedido, N = voto recusado. O termo é o do eleitor DEPOIS de tratar o
 * pedido (pode ser superior ao do candidato, o que obriga o candidato a demitir-se).
 */
public final class VoteReply {
    public final long term;        // termo do eleitor ao responder
    public final int voter;        // identificador do eleitor
    public final int candidate;    // candidato a quem a resposta se dirige
    public final boolean granted;  // true = S, false = N

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    // objeto -> texto
    public String encode() {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";" + (granted ? "S" : "N");
    }

    // texto -> objeto (lança IllegalArgumentException/NumberFormatException se estiver malformado)
    public static VoteReply decode(String msg) {
        String[] f = msg.trim().split(";");
        if (f.length != 5 || !f[0].equals("VOTO") || !(f[4].equals("S") || f[4].equals("N"))) {
            throw new IllegalArgumentException("VOTO malformado: " + msg);
        }
        return new VoteReply(Long.parseLong(f[1]), Integer.parseInt(f[2]),
                Integer.parseInt(f[3]), f[4].equals("S"));
    }
}
