package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class RecordFile {
    private final Path path;
    private final int n; // NOVO: guarda o número de partições

    // ALTERADO: recebe o n no construtor
    public RecordFile(String fileName, int n) {
        this.path = Paths.get(fileName);
        this.n = n;
    }

    // acrescenta uma linha ao fim do ficheiro (cria-o se não existir)
    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // número de sequência do último registo guardado (0 se o ficheiro não existir)
    public synchronized long lastSeq() throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }
        long last = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                last = SensorRecord.fromLine(line, n).getSeq(); // ALTERADO: passa o n
            }
        }
        return last;
    }

    // NOVO: devolve o último registo ou null (exigido pela ficha)
    public synchronized SensorRecord last() throws IOException {
        if (!Files.exists(path)) {
            return null;
        }
        SensorRecord lastRecord = null;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                lastRecord = SensorRecord.fromLine(line, n);
            }
        }
        return lastRecord;
    }

    // registos com from <= seq <= to, pela ordem do ficheiro
    public synchronized List<SensorRecord> range(long from, long to) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        if (!Files.exists(path)) {
            return out;
        }
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            SensorRecord r = SensorRecord.fromLine(line, n); // ALTERADO: passa o n
            if (r.getSeq() >= from && r.getSeq() <= to) {
                out.add(r);
            }
        }
        return out;
    }
}