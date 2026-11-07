package raft04;

import java.util.function.Consumer;

/**
 * HeartbeatMonitor = o "detetor de falhas" do PRODUTOR e do PROCESSADOR.
 * Cada um tem UM monitor por partição. (Os nós não o usam: têm o prazo de eleição do RaftState.)
 *
 * Ideia: cada vez que chega um lote do líder dessa partição, o monitor regista "ouvi-o agora".
 * Periodicamente alguém chama check(); se já passou mais de TIMEOUT sem ouvir nada,
 * o estado passa a SUSPEITO.
 *
 * ALTERADO nesta ficha: o líder já não é identificado pela incarnação mas pelo TERMO.
 * onHeartbeat(term, leader, hb):
 *   - termo inferior ao conhecido -> ignora (líder obsoleto);
 *   - termo superior              -> NOVO_LIDER (mudou a liderança; o hb recomeça);
 *   - mesmo termo e hb que salta  -> HB_PERDIDO;
 *   - mesmo termo e hb repetido/atrasado -> ignora.
 *
 * Estados: DESCONHECIDO (nunca ouvimos o líder) | ATIVO | SUSPEITO.
 * Eventos escritos (formato  EVENTO;p<k>;detalhe):
 *   NOVO_LIDER, HB_PERDIDO, RETOMA (estava SUSPEITO e voltámos a ouvir), SUSPEITA (silêncio > timeout).
 */
public final class HeartbeatMonitor {
    public enum State { DESCONHECIDO, ATIVO, SUSPEITO }

    private final int partition;       // partição que este monitor vigia
    private final long timeoutNanos;   // tempo de silêncio a partir do qual se suspeita (ns)
    private final Consumer<String> log; // para onde vão os eventos (cada processo passa o seu)

    private State state = State.DESCONHECIDO;
    private long lastNanos;   // instante local da última receção
    private long term = 0;    // maior termo observado (0 = nenhum)
    private long hb = 0;      // último heartbeat observado nesse termo

    // timeout = período * fator (ex.: 2000 ms * 3 = 6 s), convertido para nanossegundos.
    public HeartbeatMonitor(int partition, long periodMs, int factor, Consumer<String> log) {
        this.partition = partition;
        this.timeoutNanos = periodMs * factor * 1_000_000L;
        this.log = log;
        // O relógio começa a contar JÁ na construção: se o líder nunca aparecer, suspeita-se ao fim do timeout.
        this.lastNanos = System.nanoTime();
    }

    // thread de receção: chegou um LOTE da partição
    public synchronized void onHeartbeat(long term, int leader, long hb) {
        if (term < this.term) return;                      // líder obsoleto: ignora
        if (term == this.term && hb <= this.hb) return;    // duplicado ou atrasado no mesmo termo: ignora
        if (term > this.term) {
            // termo superior: há um líder novo (também é o que acontece com o 1.º lote de todos)
            log.accept("NOVO_LIDER;p" + partition + ";termo=" + term + ";lider=n" + leader);
        } else if (hb != this.hb + 1) {
            // mesmo termo mas o hb não é o seguinte: perderam-se heartbeats pelo caminho
            log.accept("HB_PERDIDO;p" + partition + ";esperado=" + (this.hb + 1) + ";recebido=" + hb);
        }
        if (state == State.SUSPEITO) {
            // estávamos a suspeitar e voltámos a ouvir um líder
            log.accept("RETOMA;p" + partition + ";termo=" + term + ";hb=" + hb);
        }
        // atualiza o que se sabe e "reinicia o cronómetro"
        this.term = term;
        this.hb = hb;
        this.lastNanos = System.nanoTime();
        this.state = State.ATIVO;
    }

    // thread de verificação: chamada a cada VERIFICACAO_MS
    public synchronized void check() {
        long elapsed = System.nanoTime() - lastNanos;   // há quanto tempo (ns) não ouvimos o líder
        if (state != State.SUSPEITO && elapsed > timeoutNanos) {
            // só regista a SUSPEITA uma vez (enquanto não houver RETOMA)
            state = State.SUSPEITO;
            log.accept("SUSPEITA;p" + partition + ";silencio_ms=" + elapsed / 1_000_000);
        }
    }

    // estado atual (usado para avisar "partição sem líder conhecido")
    public synchronized State state() {
        return state;
    }
}
