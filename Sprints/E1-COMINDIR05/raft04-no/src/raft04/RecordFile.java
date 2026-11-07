package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * RecordFile = o ficheiro de registos de UM nó de UMA partição (no-p<k>-n<i>.txt).
 * Uma linha por registo (seq;termo;sensor;temperatura;instante;L;V).
 *
 * Normalmente só se acrescenta ao fim (append). A única exceção é a TRUNCAGEM de um
 * seguidor divergente, que reescreve o ficheiro inteiro (rewrite) de forma ATÓMICA:
 * escreve um ficheiro temporário e só depois o troca pelo original.
 * Os métodos são synchronized porque várias threads podem aceder ao mesmo ficheiro.
 */
public class RecordFile {
    private final Path path;
    private final int n; // número de partições (para interpretar o vetor de cada linha)

    public RecordFile(String fileName, int n) {
        this.path = Paths.get(fileName);
        this.n = n;
    }

    // acrescenta uma linha ao fim do ficheiro (cria-o se não existir)
    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // todos os registos, pela ordem do ficheiro (lista vazia se o ficheiro não existir)
    public synchronized List<SensorRecord> readAll() throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        if (!Files.exists(path)) {
            return out;
        }
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                out.add(SensorRecord.fromLine(line, n));
            }
        }
        return out;
    }

    // substitui o conteúdo do ficheiro pelos registos dados, de forma atómica
    public synchronized void rewrite(List<SensorRecord> registos) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (SensorRecord r : registos) {
            sb.append(r.toLine()).append(System.lineSeparator());
        }
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8);
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
