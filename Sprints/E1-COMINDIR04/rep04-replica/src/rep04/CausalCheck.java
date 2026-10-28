package rep04;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class CausalCheck {

    private static class RecParticao {
        final int k;
        final SensorRecord r;

        RecParticao(int k, SensorRecord r) {
            this.k = k;
            this.r = r;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: CausalCheck <N> <pasta>");
            return;
        }
        int n = Integer.parseInt(args[0]);
        String pasta = args[1];

        List<RecParticao> todos = new ArrayList<>();
        int violacoesInvariante = 0;

        // 1. Ler os ficheiros e verificar V[k] == seq
        for (int k = 0; k < n; k++) {
            String nomeFicheiro = pasta + File.separator + "lider-p" + k + ".txt";

            File f = new File(nomeFicheiro);
            if (!f.exists()) {
                throw new IOException("Erro fatal: O ficheiro '" + nomeFicheiro + "' não existe. Relatório abortado.");
            }

            RecordFile file = new RecordFile(nomeFicheiro, n);
            List<SensorRecord> regs;

            try {
                regs = file.range(1, Long.MAX_VALUE);
            } catch (IOException e) {
                throw new IOException("Erro fatal: Falha ao ler o ficheiro do líder '" + nomeFicheiro + "'. Relatório abortado.", e);
            }

            for (SensorRecord r : regs) {
                if (r.vector.get(k) != r.seq) {
                    System.err.println("VIOLAÇÃO V[k]=seq no ficheiro p" + k + ": seq=" + r.seq + ", V[k]=" + r.vector.get(k));
                    violacoesInvariante++;
                }
                todos.add(new RecParticao(k, r));
            }
        }

        // 2. Comparar todos os pares de partições diferentes
        long aAntesB = 0, bAntesA = 0, concorrentes = 0, concorrentesLDistinto = 0;
        long violacoesLamport = 0;
        long errosEstruturais = 0; // NOVO: Contador para vetores iguais entre partições

        for (int i = 0; i < todos.size(); i++) {
            for (int j = i + 1; j < todos.size(); j++) {
                RecParticao a = todos.get(i);
                RecParticao b = todos.get(j);

                if (a.k == b.k) continue; // Só comparamos partições diferentes

                VectorClock.Order ordem = a.r.vector.compare(b.r.vector);

                if (ordem == VectorClock.Order.BEFORE) {
                    aAntesB++;
                    if (a.r.lamport >= b.r.lamport) {
                        violacoesLamport++;
                        System.err.println("VIOLAÇÃO LAMPORT: a->b mas L(a)=" + a.r.lamport + " >= L(b)=" + b.r.lamport);
                    }
                } else if (ordem == VectorClock.Order.AFTER) {
                    bAntesA++;
                    if (b.r.lamport >= a.r.lamport) {
                        violacoesLamport++;
                        System.err.println("VIOLAÇÃO LAMPORT: b->a mas L(b)=" + b.r.lamport + " >= L(a)=" + a.r.lamport);
                    }
                } else if (ordem == VectorClock.Order.CONCURRENT) {
                    concorrentes++;
                    if (a.r.lamport != b.r.lamport) {
                        concorrentesLDistinto++;
                    }
                } else if (ordem == VectorClock.Order.EQUAL) {
                    // NOVO: Estado inválido
                    errosEstruturais++;
                    System.err.println("ERRO ESTRUTURAL: Vetores iguais entre partições diferentes! " +
                            "p" + a.k + "#" + a.r.seq + " e p" + b.k + "#" + b.r.seq);
                }
            }
        }

        // 3. Imprimir o relatório
        double percentagem = concorrentes == 0 ? 0.0 : (double) concorrentesLDistinto / concorrentes * 100;

        System.out.println("=== Relatório CausalCheck ===");
        System.out.println("Total de registos lidos: " + todos.size());
        System.out.println("Violações do invariante V[k]=seq: " + violacoesInvariante);
        System.out.println("Pares a -> b: " + aAntesB);
        System.out.println("Pares b -> a: " + bAntesA);
        System.out.println("Pares concorrentes: " + concorrentes);
        System.out.printf("Pares concorrentes com L distinto: %d (%.2f%%)%n", concorrentesLDistinto, percentagem);
        System.out.println("Violações da condição de Lamport: " + violacoesLamport);
        // NOVO: Linha explícita para os erros estruturais
        System.out.println("Erros estruturais (vetores iguais entre partições): " + errosEstruturais);

        // 4. Escrever vista-lamport.txt
        todos.sort(Comparator
                .comparingLong((RecParticao x) -> x.r.lamport)
                .thenComparingInt(x -> x.k)
                .thenComparingLong(x -> x.r.seq));

        try (PrintWriter pw = new PrintWriter(pasta + File.separator + "vista-lamport.txt", StandardCharsets.UTF_8)) {
            for (RecParticao rp : todos) {
                pw.println(rp.r.toLine() + ";p" + rp.k);
            }
        }
        System.out.println("Ficheiro vista-lamport.txt escrito com sucesso.");
    }
}