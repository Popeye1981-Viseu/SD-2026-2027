package rep04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionLeader {

    // ESTADO PARTILHADO entre as várias threads
    private static long ultimoSeq;
    private static long lamport;
    private static VectorClock vector;

    // NOVO (COMINDIR04): Incarnação, Heartbeat e Lote Pendente
    private static long inc;
    private static long hb;
    private static List<SensorRecord> pendentes;

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

        // NOVO (COMINDIR04): Inicializar Incarnação, Heartbeat e Lista Pendentes
        inc = System.currentTimeMillis();
        hb = 0;
        pendentes = new ArrayList<>();

        System.out.println("Líder p" + k + " (N=" + part.count() + ") | inc=" + inc + " | último seq=" + ultimoSeq
                + " | L=" + lamport + " | V=" + vector + " | recuperação em " + endereco + ":" + porto);

        // NOVO (COMINDIR04): Agendador para enviar o lote/heartbeat
        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ses.scheduleAtFixedRate(() -> enviarLote(k, ps, endereco, porto), 0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);

        Thread entrada = new Thread(() -> {
            try {
                ps.subscribe(Wire.topicIn(k),
                        msg -> tratarEntrada(msg, k, part, file));
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

    private static void tratarEntrada(String msg, int k, Partitioner part, RecordFile file) {
        SensorRecord r;
        long L_msg;
        VectorClock V_msg;
        String sensor;
        double temp;

        try {
            if (!Wire.type(msg).equals("IN")) {
                throw new IllegalArgumentException("tipo não é IN");
            }
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

            synchronized (LOCK) {
                long novoL = Math.max(lamport, L_msg) + 1;
                VectorClock novoV = vector.copy();
                novoV.merge(V_msg);
                novoV.tick(k);
                long novoSeq = ultimoSeq + 1;

                if (novoV.get(k) != novoSeq) {
                    throw new IllegalStateException("Invariante quebrado: V[k]=" + novoV.get(k) + " != seq=" + novoSeq);
                }

                r = SensorRecord.now(novoSeq, sensor, temp, novoL, novoV);
                file.append(r);

                lamport = novoL;
                vector = novoV;
                ultimoSeq = novoSeq;

                // NOVO (COMINDIR04): Adicionar à lista de pendentes (em vez de publicar já)
                pendentes.add(r);

                System.out.println("EVENTO: seq=" + novoSeq + " L=" + novoL + " V=" + novoV + " (em espera para lote)");
            }
        } catch (Exception e) {
            System.out.println("REJEITADO: " + msg.trim() + " | " + e.getMessage());
        }
    }

    // NOVO (COMINDIR04): Método para fechar e publicar o lote periodicamente
    private static void enviarLote(int k, IpfsPubSub ps, String endereco, int porto) {
        List<SensorRecord> loteAtual;
        long seqAtual;
        long hbAtual;

        // 1. Secção Sincronizada: Snapshot atómico
        synchronized (LOCK) {
            loteAtual = pendentes;
            pendentes = new ArrayList<>(); // Nova lista para os próximos IN
            seqAtual = ultimoSeq;
            hbAtual = ++hb;
        }

        // 2. Fora da secção: Codificar e publicar
        Batch batch = new Batch(endereco, porto, inc, hbAtual, seqAtual, loteAtual);
        String msg = batch.encode();
        long bytes = msg.getBytes(StandardCharsets.UTF_8).length;

        try {
            ps.publish(Wire.topicData(k), msg);
        } catch (IOException | InterruptedException e) {
            System.out.println("Erro ao publicar lote: " + e.getMessage());
        }

        // 3. Registar no ficheiro hb-p<k>.txt (bloco try/catch separado)
        try {
            String linha = inc + ";" + hbAtual + ";" + System.currentTimeMillis() + ";" + seqAtual + ";" + loteAtual.size() + ";" + bytes + "\n";
            Files.writeString(Paths.get("hb-p" + k + ".txt"), linha, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.out.println("Erro ao escrever no ficheiro de heartbeat: " + e.getMessage());
        }

        System.out.println("HEARTBEAT: hb=" + hbAtual + " n=" + loteAtual.size() + " ultimoSeq=" + seqAtual);
    }

    private static void atenderPedido(String pedido, RecordFile file, PrintWriter out, String remoto)
            throws IOException {
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