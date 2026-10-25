package rep03;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PartitionLeader {

    // ESTADO PARTILHADO entre as duas atividades
    private static long ultimoSeq;
    private static long lamport;
    private static VectorClock vector;

    // trinco que protege o estado e garante que tudo acontece de forma atómica
    private static final Object LOCK = new Object();

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            return;
        }
        int k = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);
        Partitioner part = new Partitioner(n);
        String endereco = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        IpfsPubSub ps = new IpfsPubSub(api);
        RecordFile file = new RecordFile("lider-p" + k + ".txt", n);
        int porto = Wire.PORTO_BASE + k;

        // Inicialização do estado a partir do ficheiro (recuperação de reinício)
        SensorRecord ultimoRegisto = file.last();
        if (ultimoRegisto != null) {
            ultimoSeq = ultimoRegisto.seq;
            lamport = ultimoRegisto.lamport;
            vector = ultimoRegisto.vector.copy();
        } else {
            ultimoSeq = 0;
            lamport = 0;
            vector = new VectorClock(n);
        }

        System.out.println("Líder p" + k + " (N=" + part.count() + ") | último seq=" + ultimoSeq
                + " | L=" + lamport + " | V=" + vector + " | recuperação em " + endereco + ":" + porto);

        Thread entrada = new Thread(() -> {
            try {
                ps.subscribe(Wire.topicIn(k),
                        msg -> tratarEntrada(msg, k, part, ps, file, endereco, porto));
            } catch (IOException | InterruptedException e) {
                System.out.println("Subscrição de entrada terminou: " + e.getMessage());
            }
        });
        entrada.start();

        try (ServerSocket server = new ServerSocket(porto)) {
            while (true) {
                try (Socket s = server.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {
                    s.setSoTimeout(Wire.T_TCP_MS);
                    String remoto = s.getRemoteSocketAddress().toString();
                    String pedido = in.readLine();
                    atenderPedido(pedido, file, out, remoto);
                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }

    private static void tratarEntrada(String msg, int k, Partitioner part, IpfsPubSub ps,
                                      RecordFile file, String endereco, int porto) {
        SensorRecord r = null;
        long L_msg;
        VectorClock V_msg;
        String sensor;
        double temp;

        try {
            if (!Wire.type(msg).equals("IN")) {
                throw new IllegalArgumentException("tipo não é IN");
            }
            // Formato: IN;<sensor>;<temperatura>;<L>;<V>
            String[] f = Wire.body(msg).split(";");
            if (f.length != 4 || f[0].isBlank()) {
                throw new IllegalArgumentException("esperado IN;<sensor>;<temperatura>;<L>;<V>");
            }
            sensor = f[0].trim();
            temp = Double.parseDouble(f[1].trim());
            L_msg = Long.parseLong(f[2].trim());
            V_msg = VectorClock.parse(f[3].trim(), part.count());

            int j = part.of(sensor);
            if (j != k) {
                System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                return;
            }

            // SECÇÃO CRÍTICA ÚNICA
            synchronized (LOCK) {
                // 1. Calcular o estado temporário (sem alterar o estado real ainda)
                long novoL = Math.max(lamport, L_msg) + 1;

                VectorClock novoV = vector.copy();
                novoV.merge(V_msg);
                novoV.tick(k);

                long novoSeq = ultimoSeq + 1;

                // 2. Verificar invariante com o estado temporário
                if (novoV.get(k) != novoSeq) {
                    throw new IllegalStateException("Invariante quebrado: V[k]=" + novoV.get(k) + " != seq=" + novoSeq);
                }

                // 3. Criar o registo com os valores temporários
                r = SensorRecord.now(novoSeq, sensor, temp, novoL, novoV);

                // 4. Tentar persistir em disco. Se falhar (IOException),
                // a exceção sobe e o estado do líder (lamport, vector, ultimoSeq) fica intacto.
                file.append(r);

                // 5. SÓ AGORA confirmamos o novo estado no líder
                lamport = novoL;
                vector = novoV;
                ultimoSeq = novoSeq;

                // 6. Assinalar evento
                System.out.println("EVENTO: seq=" + novoSeq + " L=" + novoL + " V=" + novoV);
            }
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + msg.trim() + " | " + e.getMessage());
            return;
        } catch (IOException e) {
            System.out.println("Erro a escrever no ficheiro: " + e.getMessage());
            return;
        }

        try {
            ps.publish(Wire.topicData(k), Wire.reg(endereco, porto, r));
        } catch (IOException | InterruptedException e) {
            System.out.println("Registado mas não enviado (" + r.getSeq() + "): " + e.getMessage());
        }
    }

    private static void atenderPedido(String pedido, RecordFile file, PrintWriter out, String remoto)
            throws IOException {
        // ... (mantém exatamente o teu código original, está perfeito) ...
        if (pedido == null) {
            System.out.println("Ligação de " + remoto + " fechada sem pedido");
            return;
        }
        long de, ate;
        String[] p = pedido.trim().split("\\s+");
        try {
            if (p.length != 3 || !p[0].equals("PEDIDO")) throw new IllegalArgumentException();
            de = Long.parseLong(p[1]);
            ate = Long.parseLong(p[2]);
        } catch (IllegalArgumentException e) {
            out.println("ERRO esperado PEDIDO <de> <até>");
            System.out.println("Pedido mal formado de " + remoto + ": " + pedido);
            return;
        }
        if (de > ate) {
            out.println("ERRO intervalo inválido " + de + ".." + ate);
            System.out.println("Intervalo inválido de " + remoto + ": " + pedido);
            return;
        }
        long ultimo;
        synchronized (LOCK) {
            ultimo = ultimoSeq;
        }
        long ateEfetivo = Math.min(ate, ultimo);
        List<SensorRecord> regs = file.range(de, ateEfetivo);
        for (SensorRecord r : regs) {
            out.println(r.toLine());
        }
        out.println("FIM");
        if (out.checkError()) {
            throw new IOException("ligação interrompida durante a resposta a " + remoto);
        }
        System.out.println("RECUPERAÇÃO: " + de + ".." + ateEfetivo + " para " + remoto + " (" + regs.size() + " registos)");
    }
}