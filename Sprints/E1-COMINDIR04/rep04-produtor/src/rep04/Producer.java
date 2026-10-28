package rep04;

import java.util.Random;
import java.util.Scanner;

// Produtor: lê comandos da consola, calcula a partição de cada leitura e publica-a
// no tópico de entrada dessa partição. Não guarda nada, não atribui seq e não sabe
// quem são os líderes (desacoplamento no espaço).
// Execução: Producer <N> [api]
public class Producer {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }
        // número de partições: tem de ser o mesmo N com que os líderes foram arrancados
        Partitioner part = new Partitioner(Integer.parseInt(args[0]));
        // endereço da API do daemon IPFS; http://127.0.0.1:5001/api/v0 por omissão
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        IpfsPubSub ps = new IpfsPubSub(api);
        Random rnd = new Random();
        Scanner sc = new Scanner(System.in);

        System.out.println("N=" + part.count() + " | Comandos: <sensor> <temperatura> | "
                + "rajada <n> <s> | rajada-quente <n> <s> | sair");

        // ciclo principal: um comando por linha
        while (sc.hasNextLine()) {
            String[] p = sc.nextLine().trim().split("\\s+");
            if (p.length == 0 || p[0].isEmpty()) continue;   // linha vazia: ignora

            try {
                if (p[0].equals("sair")) {
                    break;

                } else if (p[0].equals("rajada") && p.length == 3) {
                    // rajada <n> <s>: sensores escolhidos ao acaso entre S01 e S<s>
                    rajada(ps, part, rnd, Integer.parseInt(p[1]), Integer.parseInt(p[2]), false);

                } else if (p[0].equals("rajada-quente") && p.length == 3) {
                    // rajada-quente <n> <s>: 80 % de S01, o resto entre S02 e S<s>
                    int s = Integer.parseInt(p[2]);
                    if (s < 2) {
                        System.out.println("rajada-quente precisa de s >= 2");
                    } else {
                        rajada(ps, part, rnd, Integer.parseInt(p[1]), s, true);
                    }

                } else if (p.length == 2) {
                    // <sensor> <temperatura>: uma leitura isolada
                    publicar(ps, part, p[0], Double.parseDouble(p[1]));

                } else {
                    System.out.println("Comando inválido");
                }
            } catch (NumberFormatException e) {
                // n, s ou temperatura que não são números: o produtor continua
                System.out.println("Número inválido: " + e.getMessage());
            } catch (Exception e) {
                // falha ao falar com o daemon IPFS (por exemplo, daemon parado): o produtor continua
                System.out.println("Erro ao publicar: " + e.getMessage());
            }
        }
    }

    // Publica uma leitura no tópico de entrada da sua partição e devolve a partição usada
    private static int publicar(IpfsPubSub ps, Partitioner part, String sensor, double temp)
            throws Exception {
        int k = part.of(sensor);                         // partição calculada a partir do sensor

        // NOVO: vetor a zeros (dimensão N) e L=0
        VectorClock vetorZero = new VectorClock(part.count());
        ps.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0L, vetorZero));
        
        System.out.println(sensor + " " + temp + " → p" + k);   // partição de destino
        return k;
    }

    // Publica n leituras o mais depressa possível.
    // quente = false: sensor ao acaso entre S01 e S<s>
    // quente = true : 80 % S01, 20 % ao acaso entre S02 e S<s>
    private static void rajada(IpfsPubSub ps, Partitioner part, Random rnd,
                               int n, int s, boolean quente) throws Exception {
        int[] porParticao = new int[part.count()];       // leituras enviadas para cada partição
        long primeira = 0, ultima = 0;                   // instantes da primeira e da última publicação

        for (int i = 0; i < n; i++) {
            String sensor;
            if (!quente) {
                sensor = String.format("S%02d", 1 + rnd.nextInt(s));       // S01..S<s>
            } else if (rnd.nextDouble() < 0.8) {
                sensor = "S01";                                            // chave quente (80 %)
            } else {
                sensor = String.format("S%02d", 2 + rnd.nextInt(s - 1));   // S02..S<s>
            }
            double temp = 15 + rnd.nextDouble() * 15;    // temperatura aleatória entre 15 e 30 °C

            int k = publicar(ps, part, sensor, temp);
            porParticao[k]++;                            // conta para o resumo da rajada

            long agora = System.currentTimeMillis();
            if (i == 0) primeira = agora;                // instante da primeira publicação
            ultima = agora;                              // no fim do ciclo fica o da última

            // NOVO: Pausa de 5 ms para o IPFS não bloquear por excesso de tráfego
            Thread.sleep(5);
        }

        // instantes da primeira e da última publicação (usados nas medições do ponto 16)
        System.out.println("Rajada: " + n + " leituras | primeira=" + primeira
                + " ultima=" + ultima + " (" + (ultima - primeira) + " ms)");
        // quantas leituras foram para cada partição
        for (int k = 0; k < porParticao.length; k++) {
            System.out.println("  p" + k + ": " + porParticao[k] + " leituras");
        }
    }
}