package raft04;

/**
 * SensorRecord = UMA leitura de um sensor já "carimbada" pelo líder.
 * É o registo que fica guardado nos ficheiros e que viaja nos lotes.
 *
 * NOVO nesta ficha: cada registo guarda o TERMO em que foi criado.
 * Dois registos com o mesmo seq e o mesmo termo são o mesmo registo;
 * com termos diferentes, prevalece o do líder atual (ver divergência).
 *
 * Linha de texto (7 campos separados por ';'):
 *   seq;termo;sensor;temperatura;instante;L;V
 * Exemplo:  5;2;S01;21.6;1793794888844;5;5,0,0
 */
public class SensorRecord {
    public final long seq;            // nº de sequência DENTRO da partição (1, 2, 3, ...)
    public final long termo;          // termo do líder que criou o registo
    public final String sensor;       // identificador do sensor (ex.: S01)
    public final double temperatura;  // valor lido
    public final long instante;       // instante (ms) em que o líder criou o registo
    public final long lamport;        // L: relógio de Lamport
    public final VectorClock vector;  // V: relógio vetorial

    public SensorRecord(long seq, long termo, String sensor, double temperatura,
                        long instante, long lamport, VectorClock vector) {
        this.seq = seq;
        this.termo = termo;
        this.sensor = sensor;
        this.temperatura = temperatura;
        this.instante = instante;
        this.lamport = lamport;
        this.vector = vector;
    }

    // Cria um registo com o instante atual e os carimbos dados.
    public static SensorRecord now(long seq, long termo, String sensor, double temperatura,
                                   long lamport, VectorClock vector) {
        return new SensorRecord(seq, termo, sensor, temperatura, System.currentTimeMillis(), lamport, vector);
    }

    // Registo -> linha de texto (é o formato dos ficheiros e dos lotes)
    public String toLine() {
        return seq + ";" + termo + ";" + sensor + ";" + temperatura + ";" + instante + ";" + lamport + ";" + vector.toString();
    }

    // Linha de texto -> registo. n = nº de partições (para validar o vetor).
    public static SensorRecord fromLine(String line, int n) {
        String[] f = line.trim().split(";");
        if (f.length != 7) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        long seq = Long.parseLong(f[0]);
        long termo = Long.parseLong(f[1]);
        String sensor = f[2];
        double temp = Double.parseDouble(f[3]);
        long instante = Long.parseLong(f[4]);
        long lamport = Long.parseLong(f[5]);
        VectorClock vector = VectorClock.parse(f[6], n);
        return new SensorRecord(seq, termo, sensor, temp, instante, lamport, vector);
    }

    // "getters" usados por outras classes
    public long getSeq() { return seq; }
    public long getTermo() { return termo; }
    public String getSensorId() { return sensor; }
    public double getTemperature() { return temperatura; }
    public String getTimestamp() { return String.valueOf(instante); }
}
