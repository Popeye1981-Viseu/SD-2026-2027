package rep02;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Verificação do particionamento: lê os ficheiros dos líderes e das réplicas reunidos
// numa pasta e escreve um relatório por partição.
// Execução: PartitionCheck <N> <pasta>
public class PartitionCheck {

    private static final int R = 2;   // fator de replicação esperado nesta ficha

    // nomes dos ficheiros: lider-p<k>.txt e replica-<n>-p<k>.txt
    private static final Pattern LIDER = Pattern.compile("lider-p(\\d+)\\.txt");
    private static final Pattern REPLICA = Pattern.compile("replica-(\\d+)-p(\\d+)\\.txt");

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: PartitionCheck <N> <pasta>");
            return;
        }
        Partitioner part = new Partitioner(Integer.parseInt(args[0]));
        Path pasta = Paths.get(args[1]);

        // 1) encontrar os ficheiros na pasta
        Map<Integer, Path> lideres = new TreeMap<>();                  // partição -> ficheiro do líder
        Map<Integer, Map<Integer, Path>> replicas = new TreeMap<>();   // partição -> (id da réplica -> ficheiro)
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(pasta, "*.txt")) {
            for (Path f : ds) {
                String nome = f.getFileName().toString();
                Matcher ml = LIDER.matcher(nome);
                Matcher mr = REPLICA.matcher(nome);
                if (ml.matches()) {
                    lideres.put(Integer.parseInt(ml.group(1)), f);
                } else if (mr.matches()) {
                    int id = Integer.parseInt(mr.group(1));
                    int k = Integer.parseInt(mr.group(2));
                    replicas.computeIfAbsent(k, x -> new TreeMap<>()).put(id, f);
                }
            }
        }

        // 2) relatório de cada partição k = 0 .. N-1
        long somaParticoes = 0;   // soma dos registos dos líderes das partições 0..N-1
        for (int k = 0; k < part.count(); k++) {
            System.out.println("=== Partição p" + k + " ===");

            Path pl = lideres.get(k);
            if (pl == null) {
                System.out.println("  !! falta lider-p" + k + ".txt");
                continue;
            }
            List<SensorRecord> lider = ler(pl);

            // (a) todos os registos do líder são de sensores com of(sensor) == k?
            int errados = 0;
            for (SensorRecord r : lider) {
                int j = part.of(r.getSensorId());
                if (j != k) {
                    errados++;
                    if (errados <= 5) {   // mostra só os primeiros 5
                        System.out.println("  !! seq " + r.getSeq() + ": " + r.getSensorId() + " pertence a p" + j);
                    }
                }
            }
            System.out.println("  Partição dos sensores: "
                    + (errados == 0 ? "OK" : "!! " + errados + " registos de outra partição"));

            // (b) sequência do ficheiro do líder: começa em 1, sem saltos nem repetições
            verificarSequencia(pl.getFileName().toString(), lider);

            // (c) réplicas que guardam esta partição (deve ser R = 2)
            Map<Integer, Path> reps = replicas.getOrDefault(k, Collections.emptyMap());
            System.out.println("  Réplicas com p" + k + ": " + reps.size()
                    + (reps.size() == R ? " (OK, R = " + R + ")" : " (!! esperado R = " + R + ")"));
            for (Map.Entry<Integer, Path> e : reps.entrySet()) {
                List<SensorRecord> rep = ler(e.getValue());
                String nome = e.getValue().getFileName().toString();
                verificarSequencia(nome, rep);           // sequência do ficheiro da réplica
                comparar(nome, lider, rep);              // igual, prefixo ou divergente
            }

            // (d) número de registos e de sensores distintos da partição
            Set<String> sensores = new HashSet<>();
            for (SensorRecord r : lider) {
                sensores.add(r.getSensorId());
            }
            System.out.println("  Registos: " + lider.size() + " | sensores distintos: " + sensores.size());
            somaParticoes += lider.size();
        }

        // 3) total do sistema: todos os ficheiros de líder encontrados na pasta.
        //    Se houver um lider-p<k>.txt com k >= N (configuração errada), os dois valores diferem.
        long totalSistema = 0;
        for (Path pl : lideres.values()) {
            totalSistema += ler(pl).size();
        }
        System.out.println("=== Total ===");
        System.out.println("  Registos no sistema: " + totalSistema
                + " | soma das partições: " + somaParticoes
                + (totalSistema == somaParticoes ? " (OK)" : " (!! diferentes)"));
    }

    // lê um ficheiro de registos (ignora linhas em branco)
    private static List<SensorRecord> ler(Path f) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        for (String linha : Files.readAllLines(f, StandardCharsets.UTF_8)) {
            if (linha.isBlank()) {
                continue;
            }
            try {
                out.add(SensorRecord.fromLine(linha));
            } catch (IllegalArgumentException e) {
                // indica em que ficheiro está a linha mal formada
                throw new IllegalArgumentException(f.getFileName() + ": " + e.getMessage());
            }
        }
        return out;
    }

    // verifica que os seq são 1, 2, 3, ... sem saltos nem repetições
    private static void verificarSequencia(String nome, List<SensorRecord> regs) {
        long esperado = 1;
        for (SensorRecord r : regs) {
            if (r.getSeq() != esperado) {
                String tipo = r.getSeq() < esperado ? "repetição" : "salto";
                System.out.println("  " + nome + ": !! sequência com " + tipo
                        + " (esperado " + esperado + ", encontrado " + r.getSeq() + ")");
                return;
            }
            esperado++;
        }
        System.out.println("  " + nome + ": sequência OK (1.." + regs.size() + ")");
    }

    // compara o ficheiro de uma réplica com o do líder: igual, prefixo ou divergente
    private static void comparar(String nome, List<SensorRecord> lider, List<SensorRecord> rep) {
        int n = Math.min(lider.size(), rep.size());
        for (int i = 0; i < n; i++) {
            // compara a linha completa: seq, sensor, temperatura e instante
            if (!lider.get(i).toLine().equals(rep.get(i).toLine())) {
                System.out.println("  " + nome + ": !! DIVERGE do líder na linha " + (i + 1));
                return;
            }
        }
        if (rep.size() == lider.size()) {
            System.out.println("  " + nome + ": IGUAL ao líder");
        } else if (rep.size() < lider.size()) {
            System.out.println("  " + nome + ": PREFIXO do líder, faltam " + (lider.size() - rep.size()) + " registos");
        } else {
            // a réplica tem mais registos do que o líder (por exemplo, ficheiro do líder apagado)
            System.out.println("  " + nome + ": !! DIVERGE, tem mais " + (rep.size() - lider.size()) + " registos do que o líder");
        }
    }
}
