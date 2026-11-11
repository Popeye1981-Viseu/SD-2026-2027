package raft04;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * CausalCheck = verificador CAUSAL. Confirma que o relógio de Lamport (L) e o relógio
 * vetorial (V) são coerentes entre si E que o invariante V[k] = seq se mantém através
 * das mudanças de líder.
 *
 * ALTERADO nesta ficha: já não há lider-p<k>.txt. Lê os ficheiros no-p<k>-n<i>.txt dos três nós
 * de cada partição:
 *   - o invariante V[k] = seq é verificado em TODOS os registos de TODOS os nós
 *     (um novo líder que começasse L e V a zero quebrá-lo logo no primeiro registo);
 *   - a comparação de pares entre partições usa o registo FINAL de cada partição, que é o
 *     ficheiro do nó mais atualizado (maior termo do último registo e, em empate, maior seq).
 * Execução:  CausalCheck <N> <pasta>
 * Escreve ainda vista-lamport.txt: todos os registos ordenados por (L, partição, seq).
 */
public class CausalCheck {

    // Par (partição, registo): precisamos de saber de que partição veio cada registo.
    private static class RecParticao {
        final int k;
        final SensorRecord r;

        RecParticao(int k, SensorRecord r) {
            this.k = k;
            this.r = r;
        }
    }

    // Ponto de entrada: args = N e a pasta onde estão os ficheiros dos líderes.
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: CausalCheck <N> <pasta>");
            return;
        }
        int n = Integer.parseInt(args[0]);
        String pasta = args[1];

        // Lista com os registos de TODAS as partições juntos.
        List<RecParticao> todos = new ArrayList<>();   // registos FINAIS de todas as partições
        int violacoesInvariante = 0;

        // 1. Ler os ficheiros e verificar V[k] == seq
        for (int k = 0; k < n; k++) {
            List<SensorRecord> final_ = null;   // registo final da partição k (o nó mais atualizado)

            for (int i = 1; i <= Wire.GRUPO; i++) {
                String nomeFicheiro = pasta + File.separator + "no-p" + k + "-n" + i + ".txt";
                File f = new File(nomeFicheiro);
                if (!f.exists()) {
                    // um nó pode não ter registos (ficheiro por criar); só é erro se NENHUM existir
                    continue;
                }
                List<SensorRecord> regs;
                try {
                    regs = new RecordFile(nomeFicheiro, n).readAll();
                } catch (IOException e) {
                    throw new IOException("Erro fatal: Falha ao ler o ficheiro '" + nomeFicheiro + "'. Relatório abortado.", e);
                }

                // Invariante do sistema: a componente k do vetor tem de ser igual ao seq do registo.
                for (SensorRecord r : regs) {
                    if (r.vector.get(k) != r.seq) {
                        System.err.println("VIOLAÇÃO V[k]=seq em no-p" + k + "-n" + i + ": seq=" + r.seq
                                + " termo=" + r.termo + ", V[k]=" + r.vector.get(k));
                        violacoesInvariante++;
                    }
                }

                // o registo final é o do nó com o último registo mais atual: maior termo e, em empate, maior seq
                if (!regs.isEmpty()) {
                    SensorRecord u = regs.get(regs.size() - 1);
                    SensorRecord uf = (final_ == null || final_.isEmpty()) ? null : final_.get(final_.size() - 1);
                    if (uf == null || u.termo > uf.termo || (u.termo == uf.termo && u.seq > uf.seq)) {
                        final_ = regs;
                    }
                }
            }
            if (final_ == null) {
                throw new IOException("Erro fatal: nenhum ficheiro no-p" + k + "-n*.txt com registos em '" + pasta + "'. Relatório abortado.");
            }
            for (SensorRecord r : final_) {
                todos.add(new RecParticao(k, r));
            }
        }

        // 2. Comparar todos os pares de partições diferentes
        // Contadores do relatório:
        //   aAntesB / bAntesA = pares com ordem causal (um aconteceu antes do outro);
        //   concorrentes = pares sem ordem causal; concorrentesLDistinto = desses, os que têm L diferente;
        //   violacoesLamport = pares com a -> b mas L(a) >= L(b) (NÃO deve haver nenhum).
        long aAntesB = 0, bAntesA = 0, concorrentes = 0, concorrentesLDistinto = 0;
        long violacoesLamport = 0;
        long errosEstruturais = 0; // NOVO: Contador para vetores iguais entre partições

        for (int i = 0; i < todos.size(); i++) {
            for (int j = i + 1; j < todos.size(); j++) {
                RecParticao a = todos.get(i);
                RecParticao b = todos.get(j);

                // Só interessam pares de partições diferentes (dentro da mesma partição a ordem é o seq).
                if (a.k == b.k) continue; // Só comparamos partições diferentes

                // Compara os vetores: BEFORE / AFTER / EQUAL / CONCURRENT.
                VectorClock.Order ordem = a.r.vector.compare(b.r.vector);

                // a aconteceu antes de b: então tem de ser L(a) < L(b) (condição de Lamport).
                if (ordem == VectorClock.Order.BEFORE) {
                    aAntesB++;
                    if (a.r.lamport >= b.r.lamport) {
                        violacoesLamport++;
                        System.err.println("VIOLAÇÃO LAMPORT: a->b mas L(a)=" + a.r.lamport + " >= L(b)=" + b.r.lamport);
                    }
                // b aconteceu antes de a: então tem de ser L(b) < L(a).
                } else if (ordem == VectorClock.Order.AFTER) {
                    bAntesA++;
                    if (b.r.lamport >= a.r.lamport) {
                        violacoesLamport++;
                        System.err.println("VIOLAÇÃO LAMPORT: b->a mas L(b)=" + b.r.lamport + " >= L(a)=" + a.r.lamport);
                    }
                // Concorrentes: não há ordem causal, por isso L pode ser qualquer um (só se conta quantos têm L distinto).
                } else if (ordem == VectorClock.Order.CONCURRENT) {
                    concorrentes++;
                    if (a.r.lamport != b.r.lamport) {
                        concorrentesLDistinto++;
                    }
                // Dois registos de partições diferentes nunca podem ter o vetor igual: cada um incrementa a sua componente.
                } else if (ordem == VectorClock.Order.EQUAL) {
                    // NOVO: Estado inválido
                    errosEstruturais++;
                    System.err.println("ERRO ESTRUTURAL: Vetores iguais entre partições diferentes! " +
                            "p" + a.k + "#" + a.r.seq + " e p" + b.k + "#" + b.r.seq);
                }
            }
        }

        // 3. Imprimir o relatório
        // Percentagem de pares concorrentes que têm L distinto.
        double percentagem = concorrentes == 0 ? 0.0 : (double) concorrentesLDistinto / concorrentes * 100;

        // Escreve o relatório na consola.
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
        // Ordena pela ordem total de Lamport (desempate por partição e por seq).
        todos.sort(Comparator
                .comparingLong((RecParticao x) -> x.r.lamport)
                .thenComparingInt(x -> x.k)
                .thenComparingLong(x -> x.r.seq));

        // Escreve o ficheiro vista-lamport.txt (um registo por linha, com a partição no fim).
        try (PrintWriter pw = new PrintWriter(pasta + File.separator + "vista-lamport.txt", StandardCharsets.UTF_8)) {
            for (RecParticao rp : todos) {
                pw.println(rp.r.toLine() + ";p" + rp.k);
            }
        }
        System.out.println("Ficheiro vista-lamport.txt escrito com sucesso.");
    }
}