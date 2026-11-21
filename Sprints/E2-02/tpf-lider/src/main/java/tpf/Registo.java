package tpf;

import java.time.Instant;

/**
 * Um registo durável do arquivo distribuído.
 *
 * Um registo só existe depois de a sua linha estar em `registos.log` com
 * fsync concluído (E2-02). O lsn é atribuído pelo líder, em sequência,
 * a começar em 1.
 *
 * @param lsn      número de sequência do log (>= 1, sem repetições nem saltos)
 * @param instante instante de atribuição do LSN (UTC)
 * @param autor    nome do utilizador que originou o registo
 * @param canal    canal de onde veio (#arquivo)
 * @param texto    texto original, já descodificado do Base64
 */
public record Registo(long lsn, Instant instante, String autor, String canal, String texto) {

    // Construtor compacto: garante os invariantes logo na construção,
    // para que nenhum objeto inválido circule pelo resto do sistema.
    public Registo {
        if (lsn < 1) throw new IllegalArgumentException("lsn tem de ser >= 1 (recebido: " + lsn + ")");
        if (instante == null) throw new IllegalArgumentException("instante não pode ser nulo");
        if (autor == null || autor.isEmpty()) throw new IllegalArgumentException("autor não pode ser vazio");
        if (canal == null || canal.isEmpty()) throw new IllegalArgumentException("canal não pode ser vazio");
        if (texto == null) throw new IllegalArgumentException("texto não pode ser nulo");
    }
}