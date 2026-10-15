package rep02;

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

    private final int k;
    private final int N;
    private final String enderecoAnunciado;
    private final int portoRecuperacao;
    private final RecordFile file;
    private final IpfsPubSub ps;

    // Estado partilhado pelas duas atividades (entrada e recuperação).
    // A atribuição do seq e o append acontecem na MESMA secção sincronizada.
    private final Object lock = new Object();
    private long ultimoSeq;

    public PartitionLeader(int k, int N, String endereco, String api) throws IOException {
        this.k = k;
        this.N = N;
        this.enderecoAnunciado = endereco;
        this.portoRecuperacao = Wire.PORTO_BASE + k;
        this.file = new RecordFile("lider-p" + k + ".txt");
        this.ps = new IpfsPubSub(api);
        this.ultimoSeq = file.lastSeq();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            System.exit(1);
        }
        int k = Integer.parseInt(args[0]);
        int N = Integer.parseInt(args[1]);
        String endereco = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";
        new PartitionLeader(k, N, endereco, api).run();
    }

    public void run() throws Exception {
        System.out.println("Líder p" + k + ": lastSeq() = " + ultimoSeq
                + ", anuncia " + enderecoAnunciado + ":" + portoRecuperacao
                + ", subscreve " + Wire.topicIn(k) + ", publica em " + Wire.topicData(k));

        // ---------- atividade 1: entrada (thread dedicada) ----------
        Thread entrada = new Thread(this::loopEntrada, "entrada-p" + k);
        entrada.setDaemon(true);
        entrada.start();

        // ---------- atividade 2: servidor de recuperação (thread principal) ----------
        try (ServerSocket server = new ServerSocket(portoRecuperacao)) {
            System.out.println("Servidor de recuperação à escuta no porto " + portoRecuperacao);
            while (true) {
                try (Socket s = server.accept();
                     BufferedReader in = new BufferedReader(
                             new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(
                             new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {
                    s.setSoTimeout(Wire.T_TCP_MS);
                    atender(s, in, out);
                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }

    // ---------- atividade de entrada ----------
    private void loopEntrada() {
        try {
            ps.subscribe(Wire.topicIn(k), this::onIn);
        } catch (IOException | InterruptedException e) {
            System.err.println("Subscrição da entrada terminou: " + e.getMessage());
        }
    }

    private void onIn(String msg) {
        try {
            if (!"IN".equals(Wire.type(msg))) {
                System.out.println("REJEITADO: " + msg);
                return;
            }
            String[] f = Wire.body(msg).split(";");
            if (f.length != 2) {
                System.out.println("REJEITADO: " + msg);
                return;
            }
            String sensor = f[0];
            double temp;
            try {
                temp = Double.parseDouble(f[1]);
            } catch (NumberFormatException e) {
                System.out.println("REJEITADO: " + msg);
                return;
            }

            // (Parte B) verificação de partição — descomentar quando existir Partitioner:
            // if (new Partitioner(N).of(sensor) != k) {
            //     System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + ...);
            //     return;
            // }

            // secção sincronizada: atribuir seq E escrever localmente ANTES de propagar
            SensorRecord r;
            synchronized (lock) {
                long seq = ++ultimoSeq;
                r = SensorRecord.now(seq, sensor, temp);
                file.append(r);
            }

            ps.publish(Wire.topicData(k),
                    Wire.reg(enderecoAnunciado, portoRecuperacao, r));
        } catch (IOException | InterruptedException e) {
            System.err.println("Falha a tratar IN: " + e.getMessage());
        } catch (RuntimeException e) {
            System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
        }
    }

    // ---------- atividade de recuperação ----------
    private void atender(Socket s, BufferedReader in, PrintWriter out) throws IOException {
        String pedido = in.readLine();
        if (pedido == null) {
            out.println("ERRO pedido vazio");
            return;
        }
        String[] partes = pedido.trim().split("\\s+");
        if (partes.length != 3 || !"PEDIDO".equals(partes[0])) {
            out.println("ERRO pedido mal formado");
            return;
        }
        long de, ate;
        try {
            de = Long.parseLong(partes[1]);
            ate = Long.parseLong(partes[2]);
        } catch (NumberFormatException e) {
            out.println("ERRO limites inválidos");
            return;
        }
        if (de > ate) {
            out.println("ERRO de > até");
            return;
        }

        long topo;
        synchronized (lock) {
            topo = Math.min(ate, ultimoSeq);
        }

        String remoto = String.valueOf(s.getRemoteSocketAddress());

        if (de > topo) {
            out.println("FIM");
            System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " para " + remoto + " (0 registos)");
            return;
        }

        List<SensorRecord> recs = file.range(de, topo);
        for (SensorRecord r : recs) {
            out.println(r.toLine());
        }
        out.println("FIM");
        System.out.println("RECUPERAÇÃO: " + de + ".." + topo + " para " + remoto
                + " (" + recs.size() + " registos)");
    }
}
