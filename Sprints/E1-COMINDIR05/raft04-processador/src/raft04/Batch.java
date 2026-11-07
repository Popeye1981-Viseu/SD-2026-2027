package raft04;

import java.util.ArrayList;
import java.util.List;

/**
 * Batch = um LOTE. É a mensagem que o líder publica de Δ em Δ,
 * HAJA OU NÃO registos novos. Faz duas coisas ao mesmo tempo:
 *   1) transporta os registos novos desde o último lote;
 *   2) serve de HEARTBEAT: o simples facto de chegar prova que o líder está vivo.
 *
 * NOVO nesta ficha: o cabeçalho deixa de ter a incarnação (inc) e passa a identificar
 * a liderança pelo TERMO e pelo LÍDER, e ainda diz o termo do último registo (lastTerm):
 *
 *   LOTE;<termo>;<lider>;<endereço>;<porto>;<hb>;<ultimoSeq>;<ultimoTermo>;<n>   <- cabeçalho (9 campos)
 *   <seq>;<termo>;<sensor>;<temperatura>;<instante>;<L>;<V>                      <- n linhas de registo
 *   ...
 */
public class Batch {
    public long term;               // termo do líder que publicou o lote
    public int leader;              // identificador (1..G) do nó líder
    public String address;          // endereço onde o líder atende recuperações (TCP)
    public int port;                // porto onde o líder atende recuperações (TCP)
    public long hb;                 // nº do heartbeat dentro deste termo (1, 2, 3, ...)
    public long lastSeq;            // último seq que o líder tem (mesmo que este lote não o leve)
    public long lastTerm;           // termo do registo lastSeq (0 se ainda não há registos)
    public List<SensorRecord> records; // registos novos deste lote (pode estar vazia)

    // Construtor: apenas guarda os campos.
    public Batch(long term, int leader, String address, int port, long hb,
                 long lastSeq, long lastTerm, List<SensorRecord> records) {
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.records = records;
    }

    // Transforma o lote em texto para ser publicado no PubSub.
    public String encode() {
        StringBuilder sb = new StringBuilder();
        // Cabeçalho: LOTE;<termo>;<lider>;<endereço>;<porto>;<hb>;<ultimoSeq>;<ultimoTermo>;<n>
        sb.append("LOTE;").append(term).append(";").append(leader).append(";")
                .append(address).append(";").append(port).append(";")
                .append(hb).append(";").append(lastSeq).append(";")
                .append(lastTerm).append(";").append(records.size()).append("\n");

        // Uma linha por registo (formato do SensorRecord.toLine()).
        for (SensorRecord r : records) {
            sb.append(r.toLine()).append("\n");
        }
        return sb.toString();
    }

    // Faz o caminho inverso: lê o texto recebido e reconstrói o Batch.
    // nParticoes é preciso para interpretar o vetor de cada registo (tem N componentes).
    public static Batch decode(String msg, int nParticoes) {
        String[] lines = msg.split("\n");
        if (lines.length < 1) throw new IllegalArgumentException("Mensagem vazia");

        // A primeira linha é o cabeçalho e tem de ter exatamente 9 campos.
        String[] header = lines[0].trim().split(";");
        if (header.length != 9 || !header[0].equals("LOTE")) {
            throw new IllegalArgumentException("Cabeçalho LOTE malformado");
        }

        long term = Long.parseLong(header[1]);
        int leader = Integer.parseInt(header[2]);
        String address = header[3];
        int port = Integer.parseInt(header[4]);
        long hb = Long.parseLong(header[5]);
        long lastSeq = Long.parseLong(header[6]);
        long lastTerm = Long.parseLong(header[7]);
        int nHeader = Integer.parseInt(header[8]);   // n declarado no cabeçalho

        // Validação do número de linhas: o n declarado tem de bater com as linhas realmente presentes.
        // (Protege contra lotes truncados ou corrompidos.)
        int numLinhasRegistos = lines.length - 1;
        if (nHeader != numLinhasRegistos) {
            throw new IllegalArgumentException("Inconsistência: n declarado (" + nHeader
                    + ") != linhas de registo (" + numLinhasRegistos + ")");
        }

        // Converte cada linha restante num SensorRecord.
        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            String linha = lines[i].trim();
            if (!linha.isEmpty()) {
                records.add(SensorRecord.fromLine(linha, nParticoes));
            }
        }

        return new Batch(term, leader, address, port, hb, lastSeq, lastTerm, records);
    }
}
