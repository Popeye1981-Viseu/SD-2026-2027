package rep04;

import java.time.LocalDateTime;

public class SensorRecord {
    public final long seq;
    public final String sensor;
    public final double temperatura;
    public final long instante;
    public final long lamport;       // L
    public final VectorClock vector; // V

    public SensorRecord(long seq, String sensor, double temperatura, long instante, long lamport, VectorClock vector) {
        this.seq = seq;
        this.sensor = sensor;
        this.temperatura = temperatura;
        this.instante = instante;
        this.lamport = lamport;
        this.vector = vector;
    }

    // RESTAURADO E ADAPTADO: cria um registo com o instante atual e os carimbos
    public static SensorRecord now(long seq, String sensor, double temperatura, long lamport, VectorClock vector) {
        // Usa System.currentTimeMillis() para long, ou podes adaptar para String se preferires
        return new SensorRecord(seq, sensor, temperatura, System.currentTimeMillis(), lamport, vector);
    }

    public String toLine() {
        return seq + ";" + sensor + ";" + temperatura + ";" + instante + ";" + lamport + ";" + vector.toString();
    }

    public static SensorRecord fromLine(String line, int n) {
        String[] f = line.trim().split(";");
        if (f.length != 6) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        long seq = Long.parseLong(f[0]);
        String sensor = f[1];
        double temp = Double.parseDouble(f[2]);
        long instante = Long.parseLong(f[3]);
        long lamport = Long.parseLong(f[4]);
        VectorClock vector = VectorClock.parse(f[5], n);
        return new SensorRecord(seq, sensor, temp, instante, lamport, vector);
    }

    public long getSeq() { return seq; }
    public String getSensorId() { return sensor; }
    public double getTemperature() { return temperatura; }
    public String getTimestamp() { return String.valueOf(instante); }
}