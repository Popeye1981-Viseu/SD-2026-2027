package tpf;

/**
 * Detetor de falhas por heartbeat, adaptado das fichas anteriores (E2-03,
 * objetivo 5). O líder mantém **um monitor por réplica**: este objeto só
 * guarda o instante do último heartbeat e calcula o estado a partir de
 * {@link Wire#SUSPEITA_MS} e {@link Wire#MORTA_MS}.
 *
 * O monitor não conhece quotas nem reservas — isso é responsabilidade do
 * {@link ReplicaRegistry}. Separar as duas coisas permite testar as
 * transições de estado sem simular espaço em disco.
 */
public final class HeartbeatMonitor {

    private final long suspeitaMs;
    private final long mortaMs;

    private long ultimoHb;
    private boolean recebeuAlgum;

    /** Monitor com os limites do sprint ({@link Wire#SUSPEITA_MS} e {@link Wire#MORTA_MS}). */
    public HeartbeatMonitor() {
        this(Wire.SUSPEITA_MS, Wire.MORTA_MS);
    }

    /** Monitor com limites explícitos (usado pelos testes). */
    public HeartbeatMonitor(long suspeitaMs, long mortaMs) {
        if (suspeitaMs <= 0) throw new IllegalArgumentException("suspeitaMs tem de ser > 0");
        if (mortaMs <= suspeitaMs) {
            throw new IllegalArgumentException("mortaMs tem de ser > suspeitaMs");
        }
        this.suspeitaMs = suspeitaMs;
        this.mortaMs = mortaMs;
    }

    /** Regista um heartbeat recebido no instante {@code agora} (epoch millis). */
    public synchronized void onHeartbeat(long agora) {
        this.ultimoHb = agora;
        this.recebeuAlgum = true;
    }

    /** Instante do último heartbeat (0 se nunca houve). */
    public synchronized long ultimoHb() {
        return ultimoHb;
    }

    /**
     * Estado no instante {@code agora}.
     *
     * Sem nenhum heartbeat a réplica está MORTA (uma réplica registada mas
     * desligada não deve ser candidata a colocação). A ordem das comparações
     * garante que MORTA prevalece sobre SUSPEITA.
     */
    public synchronized Estado estado(long agora) {
        if (!recebeuAlgum) return Estado.MORTA;
        long silencio = Math.max(0, agora - ultimoHb);
        if (silencio >= mortaMs) return Estado.MORTA;
        if (silencio >= suspeitaMs) return Estado.SUSPEITA;
        return Estado.VIVA;
    }
}
