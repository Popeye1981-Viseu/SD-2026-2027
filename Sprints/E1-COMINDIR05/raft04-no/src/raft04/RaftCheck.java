package raft04;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * RaftCheck = verificador da ELEIÇÃO. Corre DEPOIS do ensaio, a partir dos ficheiros.
 * Execução:  RaftCheck <N> <pasta> [falhas.txt]
 *
 * Lê da pasta: no-p<k>-n<i>.txt, estado-*, hb-*, eventos-*, monitor-*.txt, produtor.txt
 * (e truncados-*, que o PartitionNode escreve para se saber QUE registos foram descartados).
 * falhas.txt (opcional) tem uma linha  p<k>;n<i>;<instante ms>  por cada falha provocada.
 *
 * Escreve o relatório na consola e em raft-relatorio.txt (na pasta) com as secções:
 *   1) eleições por partição (termos, candidatos, votos, líder, voto dividido, duração);
 *   2) segurança (termos com mais de um líder; o termo de cada nó nunca diminui);
 *   3) tempo até ao primeiro líder;
 *   4) indisponibilidade de cada falha e leituras enviadas nesse intervalo;
 *   5) leituras perdidas;
 *   6) registos truncados;
 *   7) coincidência final dos ficheiros dos nós;
 *   8) o que viram os detetores do processador e do produtor.
 * No fim há um resumo com os valores que se copiam para as tabelas da ficha.
 */
public class RaftCheck {

    // ------------------------------------------------------------------ tipos auxiliares

    // Uma linha de eventos-p<k>-n<i>.txt:  instante;EVENTO;p<k>;detalhe...
    static class Ev {
        long t;            // instante (ms)
        String nome;       // CANDIDATO, VOTO, LIDER, ...
        int k, no;         // partição e nó que escreveu o evento
        String detalhe;    // tudo o que vem depois de "p<k>;"
    }

    // Uma linha de hb-p<k>-n<i>.txt:  termo;hb;instante;ultimoSeq;ultimoTermo;n;bytes
    static class Hb {
        int no;
        long termo, hb, t, ultimoSeq, ultimoTermo, n, bytes;
    }

    // Uma linha de produtor.txt:  instante;p<k>;sensor;temperatura
    static class Leitura {
        long t;
        int k;
        String sensor, temp;
        String chave() { return sensor + "|" + temp; }
    }

    // Uma falha: p<k>;n<i>;instante
    static class Falha {
        int k, no;
        long t;
        long fim = -1;     // primeiro LOTE de um líder de termo superior (-1 = nunca houve)
    }

    // ------------------------------------------------------------------ dados lidos

    static int n;
    static String pasta;
    static final StringBuilder rel = new StringBuilder();   // o relatório (para a consola e para o ficheiro)

    static List<Ev>[] eventos;                  // por partição
    static List<Hb>[] hbs;                      // por partição
    static List<SensorRecord>[][] logs;         // [k][i] registos finais de cada nó (i-1)
    static long[][] estadoTermo;                // [k][i-1] termo no ficheiro estado (-1 se não existe)
    static List<Leitura>[] leituras;            // por partição
    static List<Falha> falhas = new ArrayList<>();

    // escreve uma linha no relatório e na consola
    static void p(String s) {
        System.out.println(s);
        rel.append(s).append("\n");
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: RaftCheck <N> <pasta> [falhas.txt]");
            return;
        }
        n = Integer.parseInt(args[0]);
        pasta = args[1];
        String falhasPath = args.length > 2 ? args[2] : null;
        int g = Wire.GRUPO;

        eventos = new List[n];
        hbs = new List[n];
        leituras = new List[n];
        logs = new List[n][g];
        estadoTermo = new long[n][g];

        // 1. lê todos os ficheiros
        for (int k = 0; k < n; k++) {
            eventos[k] = new ArrayList<>();
            hbs[k] = new ArrayList<>();
            leituras[k] = new ArrayList<>();
            for (int i = 1; i <= g; i++) {
                lerEventos(k, i);
                lerHbs(k, i);
                logs[k][i - 1] = new RecordFile(pasta + File.separator + "no-p" + k + "-n" + i + ".txt", n).readAll();
                estadoTermo[k][i - 1] = lerEstado(k, i);
            }
            // os eventos de nós diferentes ficam por ordem de tempo
            eventos[k].sort((a, b) -> Long.compare(a.t, b.t));
            hbs[k].sort((a, b) -> Long.compare(a.t, b.t));
        }
        lerProdutor();
        if (falhasPath != null) lerFalhas(falhasPath);

        // 2. relatório
        p("=== Relatório RaftCheck ===");
        p("Pasta: " + pasta + " | N=" + n + " | G=" + g);

        long[] primeiroLider = new long[n];         // tempo até ao primeiro líder (ms), -1 se nunca
        int[] termosAteLider = new int[n];          // termo do primeiro líder
        int[] votosDivididos = new int[n];          // termos com candidatos mas sem líder
        seccaoEleicoes(primeiroLider, termosAteLider, votosDivididos);
        int termosComDoisLideres = seccaoSeguranca();
        seccaoPrimeiroLider(primeiroLider);
        List<Long> indisponibilidades = seccaoFalhas();
        long[] perdidas = seccaoPerdidas();                // [0] total perdidas, [1] durante a eleição
        long[] truncagem = seccaoTruncados();              // [0] eventos, [1] descartados, [2] existem com outro seq
        long divergentes = seccaoCoincidencia();
        seccaoDetetores();

        // 3. resumo para as tabelas da ficha
        p("");
        p("=== Resumo (valores para as tabelas) ===");
        for (int k = 0; k < n; k++) {
            p("p" + k + ": primeiro líder em " + (primeiroLider[k] < 0 ? "n/d" : primeiroLider[k] + " ms")
                    + " | termos até ao primeiro líder: " + (termosAteLider[k] == 0 ? "n/d" : termosAteLider[k])
                    + " | votos divididos: " + votosDivididos[k]);
        }
        p("Indisponibilidades (ms): " + (indisponibilidades.isEmpty() ? "n/d" : indisponibilidades.toString()));
        p("Leituras perdidas: total=" + perdidas[0] + " | durante a eleição=" + perdidas[1]
                + " | fora das falhas (truncagem ou outras)=" + (perdidas[0] - perdidas[1]));
        p("Registos truncados: eventos=" + truncagem[0] + " | descartados=" + truncagem[1]
                + " | existem com outro seq no líder=" + truncagem[2]);
        p("Termos com mais de um líder: " + termosComDoisLideres);
        p("seq divergentes na coincidência final: " + divergentes);

        Files.writeString(Paths.get(pasta, "raft-relatorio.txt"), rel.toString(), StandardCharsets.UTF_8);
        p("Relatório escrito em " + Paths.get(pasta, "raft-relatorio.txt"));
    }

    // ------------------------------------------------------------------ leitura dos ficheiros

    static void lerEventos(int k, int i) throws IOException {
        Path f = Paths.get(pasta, "eventos-p" + k + "-n" + i + ".txt");
        if (!Files.exists(f)) return;
        for (String linha : Files.readAllLines(f, StandardCharsets.UTF_8)) {
            String[] c = linha.split(";", 4);          // instante ; EVENTO ; p<k> ; detalhe
            if (c.length < 3) continue;
            try {
                Ev e = new Ev();
                e.t = Long.parseLong(c[0]);
                e.nome = c[1];
                e.k = k;
                e.no = i;
                e.detalhe = c.length > 3 ? c[3] : "";
                eventos[k].add(e);
            } catch (NumberFormatException ex) {
                p("AVISO: linha de eventos malformada em " + f.getFileName() + ": " + linha);
            }
        }
    }

    static void lerHbs(int k, int i) throws IOException {
        Path f = Paths.get(pasta, "hb-p" + k + "-n" + i + ".txt");
        if (!Files.exists(f)) return;
        for (String linha : Files.readAllLines(f, StandardCharsets.UTF_8)) {
            String[] c = linha.split(";");
            if (c.length != 7) continue;
            try {
                Hb h = new Hb();
                h.no = i;
                h.termo = Long.parseLong(c[0]);
                h.hb = Long.parseLong(c[1]);
                h.t = Long.parseLong(c[2]);
                h.ultimoSeq = Long.parseLong(c[3]);
                h.ultimoTermo = Long.parseLong(c[4]);
                h.n = Long.parseLong(c[5]);
                h.bytes = Long.parseLong(c[6]);
                hbs[k].add(h);
            } catch (NumberFormatException ex) {
                p("AVISO: linha de hb malformada em " + f.getFileName() + ": " + linha);
            }
        }
    }

    // termo guardado em estado-p<k>-n<i>.txt (-1 se o ficheiro não existe)
    static long lerEstado(int k, int i) throws IOException {
        Path f = Paths.get(pasta, "estado-p" + k + "-n" + i + ".txt");
        if (!Files.exists(f)) return -1;
        return Long.parseLong(Files.readString(f, StandardCharsets.UTF_8).trim().split(";")[0]);
    }

    static void lerProdutor() throws IOException {
        Path f = Paths.get(pasta, "produtor.txt");
        if (!Files.exists(f)) {
            p("AVISO: produtor.txt não existe; não se podem calcular as leituras perdidas.");
            return;
        }
        for (String linha : Files.readAllLines(f, StandardCharsets.UTF_8)) {
            String[] c = linha.split(";");
            if (c.length != 4) continue;
            try {
                Leitura l = new Leitura();
                l.t = Long.parseLong(c[0]);
                l.k = Integer.parseInt(c[1].substring(1));
                l.sensor = c[2];
                l.temp = c[3];
                if (l.k >= 0 && l.k < n) leituras[l.k].add(l);
            } catch (RuntimeException ex) {
                p("AVISO: linha de produtor.txt malformada: " + linha);
            }
        }
    }

    static void lerFalhas(String caminho) throws IOException {
        for (String linha : Files.readAllLines(Paths.get(caminho), StandardCharsets.UTF_8)) {
            if (linha.isBlank()) continue;
            String[] c = linha.trim().split(";");
            if (c.length != 3) {
                p("AVISO: linha de falhas malformada (esperado p<k>;n<i>;instante): " + linha);
                continue;
            }
            Falha f = new Falha();
            f.k = Integer.parseInt(c[0].substring(1));
            f.no = Integer.parseInt(c[1].substring(1));
            f.t = Long.parseLong(c[2]);
            falhas.add(f);
        }
    }

    // ------------------------------------------------------------------ pequenas funções de texto

    // valor de "chave=valor" dentro de um detalhe separado por ';' ou ','
    static String campo(String detalhe, String chave) {
        for (String parte : detalhe.split("[;,]")) {
            parte = parte.trim();
            if (parte.startsWith(chave + "=")) return parte.substring(chave.length() + 1);
        }
        return null;
    }

    // "n2" -> 2
    static int idNo(String s) {
        return Integer.parseInt(s.replace("n", "").trim());
    }

    // ------------------------------------------------------------------ 1) eleições

    static void seccaoEleicoes(long[] primeiroLider, int[] termosAteLider, int[] votosDivididos) {
        p("");
        p("--- 1) Eleições por partição ---");
        for (int k = 0; k < n; k++) {
            // por termo: candidatos (nó -> instante), líderes (nó -> instante), votos concedidos por candidato
            TreeMap<Long, Map<Integer, Long>> candidatos = new TreeMap<>();
            TreeMap<Long, Map<Integer, Long>> lideres = new TreeMap<>();
            Map<Long, Map<Integer, TreeSet<Integer>>> votos = new HashMap<>();

            for (Ev e : eventos[k]) {
                if (e.nome.equals("CANDIDATO")) {
                    long t = Long.parseLong(campo(e.detalhe, "termo"));
                    candidatos.computeIfAbsent(t, x -> new LinkedHashMap<>()).putIfAbsent(e.no, e.t);
                } else if (e.nome.equals("LIDER")) {
                    long t = Long.parseLong(campo(e.detalhe, "termo"));
                    lideres.computeIfAbsent(t, x -> new LinkedHashMap<>()).putIfAbsent(e.no, e.t);
                } else if (e.nome.equals("VOTO") && e.detalhe.endsWith("concedido")) {
                    long t = Long.parseLong(campo(e.detalhe, "termo"));
                    int cand = idNo(campo(e.detalhe, "candidato"));
                    votos.computeIfAbsent(t, x -> new HashMap<>())
                            .computeIfAbsent(cand, x -> new TreeSet<>()).add(e.no);   // e.no = eleitor
                }
            }

            TreeSet<Long> termos = new TreeSet<>(candidatos.keySet());
            termos.addAll(lideres.keySet());
            p("p" + k + ":");
            if (termos.isEmpty()) p("  (sem eleições registadas)");
            votosDivididos[k] = 0;
            primeiroLider[k] = -1;
            termosAteLider[k] = 0;

            for (long t : termos) {
                StringBuilder sb = new StringBuilder("  termo " + t + ": candidatos ");
                Map<Integer, Long> cs = candidatos.getOrDefault(t, new LinkedHashMap<>());
                List<String> lista = new ArrayList<>();
                for (int c : cs.keySet()) {
                    int obtidos = 1 + votos.getOrDefault(t, new HashMap<>()).getOrDefault(c, new TreeSet<>()).size();
                    lista.add("n" + c + "(votos=" + obtidos + ")");
                }
                sb.append(lista.isEmpty() ? "-" : String.join(", ", lista));
                Map<Integer, Long> ls = lideres.get(t);
                if (ls == null) {
                    sb.append(" | voto dividido (sem líder)");
                    votosDivididos[k]++;
                } else {
                    for (Map.Entry<Integer, Long> l : ls.entrySet()) {
                        sb.append(" | líder n").append(l.getKey());
                        if (!cs.isEmpty()) {
                            long inicio = cs.values().stream().mapToLong(Long::longValue).min().getAsLong();
                            sb.append(" | eleição ").append(l.getValue() - inicio).append(" ms");
                        }
                    }
                    if (termosAteLider[k] == 0) termosAteLider[k] = (int) t;   // 1.º termo com líder
                }
                p(sb.toString());
            }
        }
    }

    // ------------------------------------------------------------------ 2) segurança

    // devolve o nº de termos com mais de um LIDER (deve ser 0)
    static int seccaoSeguranca() {
        p("");
        p("--- 2) Segurança da eleição ---");
        int total = 0;
        for (int k = 0; k < n; k++) {
            // termos com mais de um líder
            Map<Long, TreeSet<Integer>> lideresPorTermo = new TreeMap<>();
            for (Ev e : eventos[k]) {
                if (e.nome.equals("LIDER")) {
                    lideresPorTermo.computeIfAbsent(Long.parseLong(campo(e.detalhe, "termo")), x -> new TreeSet<>()).add(e.no);
                }
            }
            for (Map.Entry<Long, TreeSet<Integer>> en : lideresPorTermo.entrySet()) {
                if (en.getValue().size() > 1) {
                    total++;
                    p("VIOLAÇÃO: p" + k + " termo " + en.getKey() + " teve líderes " + en.getValue());
                }
            }
            // o termo de cada nó nunca diminui (os eventos de um nó estão pela ordem em que ocorreram)
            for (int i = 1; i <= Wire.GRUPO; i++) {
                long anterior = 0;
                boolean ok = true;
                for (Ev e : eventos[k]) {
                    if (e.no != i) continue;
                    Long t = null;
                    String d = e.detalhe;
                    if (e.nome.equals("TERMO")) {
                        String[] c = d.split("->");         // "a->b"
                        long de = Long.parseLong(c[0].trim());
                        t = Long.parseLong(c[1].trim());
                        if (de < anterior) { ok = false; p("VIOLAÇÃO: n" + i + " p" + k + " voltou atrás: " + anterior + " -> " + de); }
                    } else if (e.nome.equals("CANDIDATO") || e.nome.equals("LIDER") || e.nome.equals("VOTO")
                            || e.nome.equals("DEMISSAO") || e.nome.equals("NOVO_LIDER") || e.nome.equals("ARRANQUE")) {
                        String ts = campo(d, "termo");
                        if (ts != null) t = Long.parseLong(ts);
                    }
                    if (t != null) {
                        if (t < anterior) { ok = false; p("VIOLAÇÃO: n" + i + " p" + k + " termo " + t + " < " + anterior + " (" + e.nome + ")"); }
                        anterior = Math.max(anterior, t);
                    }
                }
                long est = estadoTermo[k][i - 1];
                if (est >= 0 && est < anterior) {
                    ok = false;
                    p("VIOLAÇÃO: estado-p" + k + "-n" + i + " tem termo " + est + " < " + anterior);
                }
            }
        }
        p("Termos com mais de um líder: " + total + (total == 0 ? "  (correto)" : "  (ERRO)"));
        p("Verificação de que o termo de cada nó nunca diminui: concluída (ver VIOLAÇÃO acima, se houver)");
        return total;
    }

    // ------------------------------------------------------------------ 3) tempo até ao primeiro líder

    static void seccaoPrimeiroLider(long[] primeiroLider) {
        p("");
        p("--- 3) Tempo até ao primeiro líder (desde o arranque do primeiro nó) ---");
        for (int k = 0; k < n; k++) {
            long arranque = Long.MAX_VALUE;
            long lider = Long.MAX_VALUE;
            for (Ev e : eventos[k]) {
                if (e.nome.equals("ARRANQUE")) arranque = Math.min(arranque, e.t);
                if (e.nome.equals("LIDER")) lider = Math.min(lider, e.t);
            }
            if (arranque == Long.MAX_VALUE || lider == Long.MAX_VALUE) {
                primeiroLider[k] = -1;
                p("p" + k + ": n/d (sem ARRANQUE ou sem LIDER)");
            } else {
                primeiroLider[k] = lider - arranque;
                p("p" + k + ": " + primeiroLider[k] + " ms");
            }
        }
    }

    // ------------------------------------------------------------------ 4) falhas

    // devolve as indisponibilidades (ms) medidas
    static List<Long> seccaoFalhas() {
        List<Long> indisp = new ArrayList<>();
        p("");
        p("--- 4) Indisponibilidade de cada falha ---");
        if (falhas.isEmpty()) {
            p("(sem falhas.txt)");
            return indisp;
        }
        for (Falha f : falhas) {
            // líder do termo mais alto que já existia no instante da falha
            long t0 = 0;
            int lider0 = 0;
            for (Ev e : eventos[f.k]) {
                if (e.nome.equals("LIDER") && e.t <= f.t) {
                    long t = Long.parseLong(campo(e.detalhe, "termo"));
                    if (t >= t0) { t0 = t; lider0 = e.no; }
                }
            }
            String cab = "falha p" + f.k + " n" + f.no + " em " + f.t + ": ";
            if (lider0 == 0) { p(cab + "ainda não havia líder; sem indisponibilidade a medir"); continue; }
            if (lider0 != f.no) { p(cab + "n" + f.no + " não era o líder (era n" + lider0 + ", termo " + t0 + "); sem indisponibilidade"); continue; }

            // primeiro LOTE de um líder de termo superior
            for (Hb h : hbs[f.k]) {
                if (h.t >= f.t && h.termo > t0) { f.fim = h.t; break; }
            }
            if (f.fim < 0) {
                p(cab + "líder do termo " + t0 + "; NÃO houve novo líder até ao fim dos registos");
                continue;
            }
            long durac = f.fim - f.t;
            indisp.add(durac);
            int enviadas = 0;
            for (Leitura l : leituras[f.k]) if (l.t >= f.t && l.t < f.fim) enviadas++;
            p(cab + "líder do termo " + t0 + "; indisponibilidade " + durac + " ms (intervalo previsto ]"
                    + (Wire.ELEICAO_MIN_MS - Wire.PERIODO_HB_MS) + "; " + (Wire.ELEICAO_MAX_MS + Wire.VERIFICACAO_MS)
                    + "] ms); leituras enviadas a p" + f.k + " nesse intervalo: " + enviadas);
        }
        return indisp;
    }

    // ------------------------------------------------------------------ 5) leituras perdidas

    // devolve {total perdidas, perdidas durante a eleição}
    static long[] seccaoPerdidas() {
        p("");
        p("--- 5) Leituras perdidas (em produtor.txt mas no registo final de nenhum nó) ---");
        long total = 0, durante = 0;
        for (int k = 0; k < n; k++) {
            // quantas vezes cada (sensor, temperatura) aparece no melhor dos nós (máximo entre os nós)
            Map<String, Integer> disponiveis = new HashMap<>();
            for (int i = 0; i < Wire.GRUPO; i++) {
                Map<String, Integer> contagem = new HashMap<>();
                for (SensorRecord r : logs[k][i]) contagem.merge(r.sensor + "|" + r.temperatura, 1, Integer::sum);
                for (Map.Entry<String, Integer> e : contagem.entrySet()) disponiveis.merge(e.getKey(), e.getValue(), Math::max);
            }
            int perdidasK = 0, duranteK = 0;
            List<String> detalhes = new ArrayList<>();
            for (Leitura l : leituras[k]) {                    // por ordem de envio
                int disp = disponiveis.getOrDefault(l.chave(), 0);
                if (disp > 0) {
                    disponiveis.put(l.chave(), disp - 1);      // esta leitura consta de um registo
                } else {
                    perdidasK++;
                    boolean naEleicao = false;
                    for (Falha f : falhas) {
                        if (f.k == k && f.fim >= 0 && l.t >= f.t && l.t < f.fim) naEleicao = true;
                    }
                    if (naEleicao) duranteK++;
                    detalhes.add("    " + l.t + " " + l.sensor + " " + l.temp + (naEleicao ? " (durante a eleição)" : ""));
                }
            }
            p("p" + k + ": enviadas=" + leituras[k].size() + " | perdidas=" + perdidasK
                    + " | durante a eleição=" + duranteK + " | fora (truncagem ou outras)=" + (perdidasK - duranteK));
            for (int j = 0; j < detalhes.size() && j < 30; j++) p(detalhes.get(j));
            if (detalhes.size() > 30) p("    ... (+" + (detalhes.size() - 30) + ")");
            total += perdidasK;
            durante += duranteK;
        }
        return new long[]{total, durante};
    }

    // ------------------------------------------------------------------ 6) truncagens

    // registo "final" do líder da partição: o nó que foi líder do termo mais alto
    // (se nunca houve líder, o nó cujo registo é mais atual)
    static List<SensorRecord> logFinalDoLider(int k) {
        long t0 = -1;
        int lider = 0;
        for (Ev e : eventos[k]) {
            if (e.nome.equals("LIDER")) {
                long t = Long.parseLong(campo(e.detalhe, "termo"));
                if (t >= t0) { t0 = t; lider = e.no; }
            }
        }
        if (lider != 0) return logs[k][lider - 1];
        List<SensorRecord> melhor = logs[k][0];
        for (int i = 1; i < Wire.GRUPO; i++) if (logs[k][i].size() > melhor.size()) melhor = logs[k][i];
        return melhor;
    }

    // devolve {nº de eventos TRUNCADO, registos descartados, descartados que existem com outro seq}
    static long[] seccaoTruncados() throws IOException {
        p("");
        p("--- 6) Registos truncados ---");
        long eventosT = 0, descartados = 0, existem = 0;
        for (int k = 0; k < n; k++) {
            List<SensorRecord> finalLider = logFinalDoLider(k);
            for (int i = 1; i <= Wire.GRUPO; i++) {
                // blocos de descartados de n<i>, pela ordem das truncagens
                List<List<SensorRecord>> blocos = lerTruncados(k, i);
                int idxBloco = 0;
                for (Ev e : eventos[k]) {
                    if (!e.nome.equals("TRUNCADO") || e.no != i) continue;
                    eventosT++;
                    int desc = Integer.parseInt(campo(e.detalhe, "descartados"));
                    descartados += desc;
                    int achados = 0;
                    if (idxBloco < blocos.size()) {
                        // multiconjunto: cada registo do líder só "serve" uma vez
                        List<SensorRecord> livres = new ArrayList<>(finalLider);
                        for (SensorRecord d : blocos.get(idxBloco)) {
                            for (int j = 0; j < livres.size(); j++) {
                                SensorRecord l = livres.get(j);
                                boolean mesmoRegisto = l.seq == d.seq && l.termo == d.termo;
                                if (!mesmoRegisto && l.sensor.equals(d.sensor) && l.temperatura == d.temperatura) {
                                    achados++;
                                    livres.remove(j);
                                    break;
                                }
                            }
                        }
                    }
                    idxBloco++;
                    existem += achados;
                    p("p" + k + " n" + i + " em " + e.t + ": TRUNCADO " + e.detalhe + " | destes, " + achados
                            + " existem com outro seq no registo final do líder; " + (desc - achados) + " perderam-se");
                }
            }
        }
        if (eventosT == 0) p("(nenhum TRUNCADO)");
        return new long[]{eventosT, descartados, existem};
    }

    // lê truncados-p<k>-n<i>.txt: blocos "#de=<seq>" seguidos dos registos descartados
    static List<List<SensorRecord>> lerTruncados(int k, int i) throws IOException {
        List<List<SensorRecord>> blocos = new ArrayList<>();
        Path f = Paths.get(pasta, "truncados-p" + k + "-n" + i + ".txt");
        if (!Files.exists(f)) return blocos;
        List<SensorRecord> atual = null;
        for (String linha : Files.readAllLines(f, StandardCharsets.UTF_8)) {
            if (linha.isBlank()) continue;
            if (linha.startsWith("#")) {
                atual = new ArrayList<>();
                blocos.add(atual);
            } else if (atual != null) {
                atual.add(SensorRecord.fromLine(linha, n));
            }
        }
        return blocos;
    }

    // ------------------------------------------------------------------ 7) coincidência final

    // devolve o nº total de seq divergentes
    static long seccaoCoincidencia() {
        p("");
        p("--- 7) Coincidência final dos ficheiros dos nós ---");
        long total = 0;
        for (int k = 0; k < n; k++) {
            long menor = Long.MAX_VALUE;
            StringBuilder tamanhos = new StringBuilder();
            for (int i = 0; i < Wire.GRUPO; i++) {
                long us = logs[k][i].size();
                menor = Math.min(menor, us);
                tamanhos.append(" n").append(i + 1).append("=").append(us);
            }
            List<Long> difs = new ArrayList<>();
            for (long s = 1; s <= menor; s++) {
                String ref = logs[k][0].get((int) s - 1).toLine();
                for (int i = 1; i < Wire.GRUPO; i++) {
                    if (!logs[k][i].get((int) s - 1).toLine().equals(ref)) { difs.add(s); break; }
                }
            }
            total += difs.size();
            p("p" + k + ": ultimoSeq por nó:" + tamanhos + " | comparados 1.." + menor
                    + " | seq divergentes: " + (difs.isEmpty() ? "nenhum" : difs.toString()));
        }
        return total;
    }

    // ------------------------------------------------------------------ 8) detetores

    // resume monitor-*.txt: contagem de eventos e lista dos NOVO_LIDER/SUSPEITA/RETOMA/AVISO
    static void seccaoDetetores() throws IOException {
        p("");
        p("--- 8) O que viram os detetores (monitor-*.txt) ---");
        File[] fich = new File(pasta).listFiles((d, nome) -> nome.startsWith("monitor-") && nome.endsWith(".txt"));
        if (fich == null || fich.length == 0) {
            p("(sem ficheiros monitor-*.txt)");
            return;
        }
        java.util.Arrays.sort(fich);
        for (File f : fich) {
            Map<String, Integer> contagem = new TreeMap<>();
            List<String> destaques = new ArrayList<>();
            for (String linha : Files.readAllLines(f.toPath(), StandardCharsets.UTF_8)) {
                String[] c = linha.split(";", 3);
                if (c.length < 2) continue;
                String nome = c[1].startsWith("AVISO") ? "AVISO" : c[1];
                contagem.merge(nome, 1, Integer::sum);
                if (!nome.equals("HB_PERDIDO")) destaques.add("    " + linha);
            }
            p(f.getName() + ": " + contagem);
            for (int j = 0; j < destaques.size() && j < 40; j++) p(destaques.get(j));
            if (destaques.size() > 40) p("    ... (+" + (destaques.size() - 40) + ")");
        }
    }
}
