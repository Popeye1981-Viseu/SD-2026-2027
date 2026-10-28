package rep04;

import java.util.ArrayList;
import java.util.List;

public class Batch {
    public String address;
    public int port;
    public long inc;
    public long hb;
    public long lastSeq;
    public List<SensorRecord> records;

    public Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.records = records;
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        // Cabeçalho: LOTE;<endereço>;<porto>;<inc>;<hb>;<ultimoSeq>;<n>
        sb.append("LOTE;").append(address).append(";").append(port).append(";")
                .append(inc).append(";").append(hb).append(";")
                .append(lastSeq).append(";").append(records.size()).append("\n");

        for (SensorRecord r : records) {
            sb.append(r.toLine()).append("\n");
        }
        return sb.toString();
    }

    public static Batch decode(String msg, int nParticoes) {
        String[] lines = msg.split("\n");
        if (lines.length < 1) throw new IllegalArgumentException("Mensagem vazia");

        String[] header = lines[0].split(";");
        if (header.length != 7 || !header[0].equals("LOTE")) {
            throw new IllegalArgumentException("Cabeçalho LOTE malformado");
        }

        String address = header[1];
        int port = Integer.parseInt(header[2]);
        long inc = Long.parseLong(header[3]);
        long hb = Long.parseLong(header[4]);
        long lastSeq = Long.parseLong(header[5]);
        int nHeader = Integer.parseInt(header[6]);

        // Validação do número de linhas
        int numLinhasRegistos = lines.length - 1;
        if (nHeader != numLinhasRegistos) {
            throw new IllegalArgumentException("Inconsistência: n declarado (" + nHeader + ") != linhas de registo (" + numLinhasRegistos + ")");
        }

        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            String linha = lines[i].trim();
            if (!linha.isEmpty()) {
                // Usa o método fromLine da tua classe (passa o nParticoes para o VectorClock)
                records.add(SensorRecord.fromLine(linha, nParticoes));
            }
        }

        return new Batch(address, port, inc, hb, lastSeq, records);
    }
}