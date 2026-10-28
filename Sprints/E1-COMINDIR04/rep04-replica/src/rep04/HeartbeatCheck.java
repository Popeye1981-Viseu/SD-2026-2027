package rep04;

import java.nio.file.*;
import java.util.*;

public class HeartbeatCheck {

    static class Stats {
        long totalLotes = 0;
        long lotesVazios = 0;
        long totalRegistos = 0;
        long maxRegistos = 0;
        long somaIntervalos = 0;
        long maxIntervalo = 0;
        long qtdIntervalos = 0;
        long ultimoInstante = -1;

        // NOVO: Para a métrica de bytes
        long bytesLotesComRegistos = 0;
        long registosLotesComRegistos = 0;
        long totalBytesGlobal = 0;
    }

    static class LatStats {
        long count = 0;
        long soma = 0;
        long max = 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: HeartbeatCheck <N> <pasta> [falhas.txt]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        String pasta = args[1];
        String falhasPath = args.length > 2 ? args[2] : null;

        // 1. Estatísticas de Lotes (hb-p<k>.txt)
        Map<String, Stats> est = new HashMap<>();
        for (int k = 0; k < n; k++) {
            Path path = Paths.get(pasta, "hb-p" + k + ".txt");
            if (!Files.exists(path)) {
                System.out.println("AVISO: Ficheiro hb-p" + k + ".txt não encontrado.");
                continue;
            }
            int numLinha = 1;
            for (String linha : Files.readAllLines(path)) {
                if (linha.isBlank()) { numLinha++; continue; }
                String[] f = linha.split(";");
                if (f.length != 6) {
                    System.out.println("ERRO: hb-p" + k + ".txt, linha " + numLinha + ": esperados 6 campos.");
                    numLinha++; continue;
                }
                long inc, hb, instante, ultimoSeq, n_registos, bytes;
                try {
                    inc = Long.parseLong(f[0]); hb = Long.parseLong(f[1]);
                    instante = Long.parseLong(f[2]); ultimoSeq = Long.parseLong(f[3]);
                    n_registos = Long.parseLong(f[4]); bytes = Long.parseLong(f[5]);
                } catch (NumberFormatException e) {
                    System.out.println("ERRO: hb-p" + k + ".txt, linha " + numLinha + ": valores malformados.");
                    numLinha++; continue;
                }
                if (n_registos < 0 || bytes <= 0 || hb < 1 || ultimoSeq < 0 || inc <= 0 || instante <= 0) {
                    System.out.println("ERRO: hb-p" + k + ".txt, linha " + numLinha + ": valores de domínio inválidos.");
                    numLinha++; continue;
                }
                String chave = "p" + k + "-inc" + inc;
                Stats st = est.computeIfAbsent(chave, key -> new Stats());
                st.totalLotes++;
                st.totalBytesGlobal += bytes; // Total global (incluindo vazios)

                if (n_registos == 0) st.lotesVazios++;
                st.totalRegistos += n_registos;
                if (n_registos > st.maxRegistos) st.maxRegistos = n_registos;

                // NOVO: Apenas acumula para a métrica de bytes por registo se o lote tiver dados
                if (n_registos > 0) {
                    st.bytesLotesComRegistos += bytes;
                    st.registosLotesComRegistos += n_registos;
                }

                if (st.ultimoInstante != -1) {
                    if (instante >= st.ultimoInstante) {
                        long intervalo = instante - st.ultimoInstante;
                        st.somaIntervalos += intervalo; st.qtdIntervalos++;
                        if (intervalo > st.maxIntervalo) st.maxIntervalo = intervalo;
                        st.ultimoInstante = instante;
                    }
                } else { st.ultimoInstante = instante; }
                numLinha++;
            }
        }

        System.out.println("=== 1. Estatísticas por Partição/Incarnação ===");
        for (Map.Entry<String, Stats> entry : est.entrySet()) {
            Stats st = entry.getValue();
            double mediaRegistos = st.totalLotes > 0 ? (double) st.totalRegistos / st.totalLotes : 0;
            double mediaIntervalo = st.qtdIntervalos > 0 ? (double) st.somaIntervalos / st.qtdIntervalos : 0;

            // NOVO: Média de bytes por registo considerando o cabeçalho rateado
            double mediaBytesPorRegisto = st.registosLotesComRegistos > 0 ? (double) st.bytesLotesComRegistos / st.registosLotesComRegistos : 0;

            System.out.printf("%s: Lotes=%d, Vazios=%.1f%%, Reg/Lote=%.2f, Int/ms=%.2f, Bytes/Registo=%.2f%n",
                    entry.getKey(), st.totalLotes, (st.totalLotes > 0 ? (double) st.lotesVazios / st.totalLotes * 100 : 0),
                    mediaRegistos, mediaIntervalo, mediaBytesPorRegisto);
        }

        calcularLatencias(pasta, n);
        analisarEventosEfalhas(pasta, n, falhasPath);
        // NOVO: Comparação de tamanho com REG da COMINDIR03
        calcularTamanhoREG(pasta, n);
    }

    private static void calcularLatencias(String pasta, int n) throws Exception {
        Map<String, Long> instantesLider = new HashMap<>();
        for (int k = 0; k < n; k++) {
            Path pLider = Paths.get(pasta, "lider-p" + k + ".txt");
            if (Files.exists(pLider)) {
                int numLinha = 1;
                for (String linha : Files.readAllLines(pLider)) {
                    if (linha.isBlank()) { numLinha++; continue; }
                    String[] f = linha.split(";");
                    if (f.length != 6) { numLinha++; continue; }
                    try {
                        long seq = Long.parseLong(f[0]);
                        long instante = Long.parseLong(f[3]);
                        instantesLider.put("p" + k + "-" + seq, instante);
                    } catch (NumberFormatException e) {
                        System.out.println("ERRO: lider-p" + k + ".txt, linha " + numLinha + ": valores malformados.");
                    }
                    numLinha++;
                }
            }
        }

        Map<Integer, LatStats> latPorParticao = new HashMap<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(pasta), "monitor-*.txt")) {
            for (Path pMonitor : ds) {
                Set<String> seqsContados = new HashSet<>();
                int numLinha = 1;
                for (String linha : Files.readAllLines(pMonitor)) {
                    if (linha.isBlank() || !linha.contains(";RECEBIDO;p")) { numLinha++; continue; }
                    String[] partes = linha.split(";", 4);
                    if (partes.length < 4) { numLinha++; continue; }

                    long tempoRecebido; int partition;
                    try {
                        tempoRecebido = Long.parseLong(partes[0]);
                        partition = Integer.parseInt(partes[2].replace("p", ""));
                    } catch (NumberFormatException e) {
                        System.out.println("ERRO: " + pMonitor.getFileName() + ", linha " + numLinha + ": timestamp/partição inválidos.");
                        numLinha++; continue;
                    }

                    String detalhe = partes[3];
                    int idx = detalhe.indexOf("seqs=");
                    if (idx != -1) {
                        String seqsStr = detalhe.substring(idx + 5);
                        if (!seqsStr.equals("vazio")) {
                            String[] bounds = seqsStr.split("\\.\\.");
                            if (bounds.length == 2) {
                                try {
                                    long inicio = Long.parseLong(bounds[0]);
                                    long fim = Long.parseLong(bounds[1]);
                                    for (long s = inicio; s <= fim; s++) {
                                        String chaveSeq = "p" + partition + "-" + s;
                                        if (seqsContados.add(chaveSeq)) {
                                            Long tLider = instantesLider.get(chaveSeq);
                                            if (tLider != null) {
                                                long latencia = tempoRecebido - tLider;
                                                if (latencia >= 0) {
                                                    LatStats ls = latPorParticao.computeIfAbsent(partition, x -> new LatStats());
                                                    ls.count++; ls.soma += latencia;
                                                    if (latencia > ls.max) ls.max = latencia;
                                                }
                                            }
                                        }
                                    }
                                } catch (NumberFormatException ignored) {}
                            }
                        }
                    }
                    numLinha++;
                }
            }
        }
        System.out.println("\n=== 2. Latência de Lote por Partição ===");
        for (Map.Entry<Integer, LatStats> entry : latPorParticao.entrySet()) {
            LatStats ls = entry.getValue();
            double media = ls.count > 0 ? (double) ls.soma / ls.count : 0;
            System.out.printf("p%d: Média=%.2f ms (Max=%d ms, Amostras=%d)%n", entry.getKey(), media, ls.max, ls.count);
        }
    }

    private static void analisarEventosEfalhas(String pasta, int n, String falhasPath) throws Exception {
        Map<String, List<Long>> hbTimes = new HashMap<>();
        for (int k = 0; k < n; k++) {
            Path p = Paths.get(pasta, "hb-p" + k + ".txt");
            if (Files.exists(p)) {
                for (String l : Files.readAllLines(p)) {
                    if (l.isBlank()) continue;
                    String[] f = l.split(";");
                    if (f.length == 6) hbTimes.computeIfAbsent("p" + k + "-inc" + f[0], x -> new ArrayList<>()).add(Long.parseLong(f[2]));
                }
            }
        }

        Map<Integer, List<Long>> falhasPorParticao = new HashMap<>();
        if (falhasPath != null && Files.exists(Paths.get(falhasPath))) {
            int numLinhaFalha = 1;
            for (String l : Files.readAllLines(Paths.get(falhasPath))) {
                if (l.isBlank()) { numLinhaFalha++; continue; }
                String[] f = l.split(";");
                if (f.length == 2 && f[0].startsWith("p")) {
                    try {
                        int k = Integer.parseInt(f[0].substring(1));
                        long instante = Long.parseLong(f[1]);
                        falhasPorParticao.computeIfAbsent(k, x -> new ArrayList<>()).add(instante);
                    } catch (NumberFormatException e) {
                        System.out.println("ERRO: falhas.txt, linha " + numLinhaFalha + ": formato numérico inválido.");
                    }
                } else {
                    System.out.println("ERRO: falhas.txt, linha " + numLinhaFalha + ": formato esperado p<k>;<instante>.");
                }
                numLinhaFalha++;
            }
            for (List<Long> list : falhasPorParticao.values()) Collections.sort(list);
        }

        System.out.println("\n=== 3. Análise de Eventos, Suspeitas e Falhas ===");

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(pasta), "monitor-*.txt")) {
            for (Path pMonitor : ds) {
                List<String> lines = Files.readAllLines(pMonitor);
                Map<Integer, Long> currentIncPorParticao = new HashMap<>();
                Map<Integer, List<Long>> suspeitasReais = new HashMap<>();

                // NOVO: Contadores por partição para este participante
                Map<Integer, Integer> hbPerdidosPorParticao = new HashMap<>();
                Map<Integer, Integer> recuperacoesPorParticao = new HashMap<>();

                System.out.println("\nParticipante: " + pMonitor.getFileName());

                for (int i = 0; i < lines.size(); i++) {
                    String line = lines.get(i);
                    if (line.isBlank()) continue;

                    // Parse rigoroso: instante;evento;p<k>;detalhe
                    String[] partes = line.split(";", 4);
                    if (partes.length < 3) {
                        System.out.println("ERRO: " + pMonitor.getFileName() + ", linha " + (i+1) + ": malformada (campos insuficientes).");
                        continue;
                    }

                    String evento = partes[1];
                    String pStr = partes[2];

                    if (!pStr.startsWith("p")) {
                        System.out.println("ERRO: " + pMonitor.getFileName() + ", linha " + (i+1) + ": partição malformada (" + pStr + ").");
                        continue;
                    }

                    int k;
                    try {
                        k = Integer.parseInt(pStr.replace("p", ""));
                    } catch (NumberFormatException e) {
                        System.out.println("ERRO: " + pMonitor.getFileName() + ", linha " + (i+1) + ": número de partição inválido (" + pStr + ").");
                        continue;
                    }

                    // Contabilização exata dos eventos
                    if (evento.equals("HB_PERDIDO")) {
                        hbPerdidosPorParticao.put(k, hbPerdidosPorParticao.getOrDefault(k, 0) + 1);
                    } else if (evento.equals("RECUPERACAO")) {
                        recuperacoesPorParticao.put(k, recuperacoesPorParticao.getOrDefault(k, 0) + 1);
                    } else if (evento.equals("RECEBIDO")) {
                        if (partes.length == 4) {
                            int idx = partes[3].indexOf("inc=");
                            if (idx != -1) {
                                int end = partes[3].indexOf(",", idx);
                                if (end == -1) end = partes[3].length();
                                try {
                                    currentIncPorParticao.put(k, Long.parseLong(partes[3].substring(idx + 4, end).trim()));
                                } catch (NumberFormatException e) {
                                    System.out.println("ERRO: " + pMonitor.getFileName() + ", linha " + (i+1) + ": inc malformado no RECEBIDO.");
                                }
                            }
                        }
                    } else if (evento.equals("SUSPEITA")) {
                        long susTime;
                        try {
                            susTime = Long.parseLong(partes[0]);
                        } catch (NumberFormatException e) {
                            System.out.println("ERRO: " + pMonitor.getFileName() + ", linha " + (i+1) + ": instante da SUSPEITA inválido.");
                            continue;
                        }

                        long incSuspeita = currentIncPorParticao.getOrDefault(k, -1L);

                        if (incSuspeita == -1L) {
                            System.out.printf("SUSPEITA [DADOS INSUFICIENTES] %s: p%d | inc desconhecido%n", pMonitor.getFileName(), k);
                            continue;
                        }

                        boolean falsa = false;
                        for (int j = i + 1; j < lines.size(); j++) {
                            String next = lines.get(j);
                            if (next.contains("RETOMA;p" + k + ";inc=" + incSuspeita)) { falsa = true; break; }
                            if (next.contains("REINICIO;p" + k)) break;
                        }

                        List<Long> times = hbTimes.getOrDefault("p" + k + "-inc" + incSuspeita, Collections.emptyList());
                        long lastHb = -1;
                        for (long t : times) { if (t <= susTime) lastHb = t; else break; }

                        String silencio = lastHb == -1 ? "N/A" : (susTime - lastHb) + " ms";
                        System.out.printf("SUSPEITA [%s] %s: p%d inc=%d | Silêncio: %s%n", (falsa ? "FALSA" : "REAL"), pMonitor.getFileName(), k, incSuspeita, silencio);

                        if (!falsa) {
                            suspeitasReais.computeIfAbsent(k, x -> new ArrayList<>()).add(susTime);
                        }
                    }
                }

                // Mostrar contagem de eventos deste participante
                System.out.println("  -> Resumo de eventos " + pMonitor.getFileName() + ":");
                for (int k = 0; k < n; k++) {
                    int hbp = hbPerdidosPorParticao.getOrDefault(k, 0);
                    int rec = recuperacoesPorParticao.getOrDefault(k, 0);
                    if (hbp > 0 || rec > 0) {
                        System.out.println("     p" + k + ": HB_PERDIDO=" + hbp + " | RECUPERACAO=" + rec);
                    }
                }

                // 3. Emparelhamento e Tempo de Deteção
                if (!falhasPorParticao.isEmpty()) {
                    for (Map.Entry<Integer, List<Long>> entry : suspeitasReais.entrySet()) {
                        int k = entry.getKey();
                        List<Long> suspeitas = entry.getValue();
                        Collections.sort(suspeitas);
                        List<Long> falhas = falhasPorParticao.getOrDefault(k, Collections.emptyList());

                        int susIdx = 0;
                        for (long falhaTime : falhas) {
                            while (susIdx < suspeitas.size() && suspeitas.get(susIdx) <= falhaTime) {
                                susIdx++;
                            }

                            if (susIdx < suspeitas.size()) {
                                long detecaoTime = suspeitas.get(susIdx) - falhaTime;
                                System.out.printf("  -> DETEÇÃO %s: p%d Falha@%d -> Suspeita@%d (Tempo: %d ms)%n",
                                        pMonitor.getFileName(), k, falhaTime, suspeitas.get(susIdx), detecaoTime);
                                susIdx++;
                            } else {
                                System.out.printf("  -> DETEÇÃO %s: p%d Falha@%d não detetada.%n",
                                        pMonitor.getFileName(), k, falhaTime);
                            }
                        }
                    }
                }
            }
        }
    }

    private static void calcularTamanhoREG(String pasta, int n) throws Exception {
        long totalBytesLinhas = 0;
        long totalRegistos = 0;

        for (int k = 0; k < n; k++) {
            Path pLider = Paths.get(pasta, "lider-p" + k + ".txt");
            if (Files.exists(pLider)) {
                for (String linha : Files.readAllLines(pLider)) {
                    if (linha.isBlank()) continue;

                    // Reconstrói a mensagem REG exatamente como era na COMINDIR03
                    String regStr = "REG;127.0.0.1;" + (Wire.PORTO_BASE + k) + ";" + linha;

                    // Conta os bytes em UTF-8
                    totalBytesLinhas += regStr.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                    totalRegistos++;
                }
            }
        }
        double mediaREG = totalRegistos > 0 ? (double) totalBytesLinhas / totalRegistos : 0;
        System.out.printf("%n=== 5. Comparação de Eficiência (Bytes/Registo) ===%n");
        System.out.printf("Tamanho médio de um REG equivalente (COMINDIR03): %.2f bytes%n", mediaREG);
    }
}