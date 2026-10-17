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

// Líder de uma partição: atribui os números de sequência, escreve no seu ficheiro,
// publica os registos para as réplicas e atende pedidos de recuperação por TCP.
// Execução: PartitionLeader <k> <N> [endereço] [api]
public class PartitionLeader {

    // ESTADO PARTILHADO entre as duas atividades: o último seq atribuído (e já escrito).
    // A thread de entrada lê-o e escreve-o; a thread de recuperação só o lê.
    private static long ultimoSeq;

    // trinco que protege ultimoSeq e garante que seq + append acontecem juntos
    private static final Object LOCK = new Object();

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            return;
        }
        int k = Integer.parseInt(args[0]);                              // partição deste líder
        Partitioner part = new Partitioner(Integer.parseInt(args[1]));  // função de partição com N partições
        // endereço que o líder ANUNCIA às réplicas (127.0.0.1 por omissão)
        String endereco = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        IpfsPubSub ps = new IpfsPubSub(api);
        RecordFile file = new RecordFile("lider-p" + k + ".txt");
        int porto = Wire.PORTO_BASE + k;          // porto de recuperação: 7000 + k

        // seq inicial obtido com lastSeq(), como na COMINDIR01 (permite reiniciar o líder)
        ultimoSeq = file.lastSeq();
        System.out.println("Líder p" + k + " (N=" + part.count() + ") | último seq=" + ultimoSeq
                + " | recuperação em " + endereco + ":" + porto);

        // ATIVIDADE DE ENTRADA: thread própria, bloqueia dentro de subscribe
        Thread entrada = new Thread(() -> {
            try {
                // cada mensagem recebida em rep02-in-p<k> é tratada por tratarEntrada
                ps.subscribe(Wire.topicIn(k),
                        msg -> tratarEntrada(msg, k, part, ps, file, endereco, porto));
            } catch (IOException | InterruptedException e) {
                System.out.println("Subscrição de entrada terminou: " + e.getMessage());
            }
        });
        entrada.start();

        // ATIVIDADE DE RECUPERAÇÃO: thread principal, atende as ligações uma de cada vez
        try (ServerSocket server = new ServerSocket(porto)) {
            while (true) {
                try (Socket s = server.accept();   // bloqueia até uma réplica se ligar
                     BufferedReader in = new BufferedReader(
                             new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(
                             new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {
                    s.setSoTimeout(Wire.T_TCP_MS);  // não espera indefinidamente pelo pedido
                    String remoto = s.getRemoteSocketAddress().toString();
                    String pedido = in.readLine();   // bloqueia até chegar o pedido
                    atenderPedido(pedido, file, out, remoto);
                } catch (IOException e) {
                    // uma falha numa ligação termina só essa ligação
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }

    // Trata uma mensagem do tópico de entrada: IN;<sensor>;<temperatura>
    private static void tratarEntrada(String msg, int k, Partitioner part, IpfsPubSub ps,
                                      RecordFile file, String endereco, int porto) {
        SensorRecord r;
        try {
            // validar o formato; qualquer problema lança IllegalArgumentException
            if (!Wire.type(msg).equals("IN")) {
                throw new IllegalArgumentException("tipo não é IN");
            }
            String[] f = Wire.body(msg).split(";");
            if (f.length != 2 || f[0].isBlank()) {
                throw new IllegalArgumentException("esperado IN;<sensor>;<temperatura>");
            }
            String sensor = f[0].trim();
            double temp = Double.parseDouble(f[1].trim());   // NumberFormatException é IllegalArgumentException

            // PASSO 9: o sensor tem de pertencer a esta partição; se não, não é registado
            int j = part.of(sensor);
            if (j != k) {
                System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                return;
            }

            // SECÇÃO CRÍTICA: atribuição do seq e append na mesma secção sincronizada
            synchronized (LOCK) {
                long seq = ultimoSeq + 1;
                r = SensorRecord.now(seq, sensor, temp);
                file.append(r);     // se a escrita falhar, ultimoSeq não avança
                ultimoSeq = seq;    // só agora o registo fica visível para a recuperação
            }
        } catch (IllegalArgumentException e) {
            // mensagem inválida: rejeitada, o líder continua
            System.out.println("REJEITADO: " + msg.trim());
            return;
        } catch (IOException e) {
            System.out.println("Erro a escrever no ficheiro: " + e.getMessage());
            return;
        }

        // ESCREVER LOCALMENTE, DEPOIS PROPAGAR: o registo já está no ficheiro,
        // por isso qualquer réplica que dê pela falta dele pode pedi-lo por TCP
        try {
            ps.publish(Wire.topicData(k), Wire.reg(endereco, porto, r));
            System.out.println("Registado e enviado: " + r.toLine());
        } catch (IOException | InterruptedException e) {
            System.out.println("Registado mas não enviado (" + r.getSeq() + "): " + e.getMessage());
        }
    }

    // Atende um pedido de recuperação: PEDIDO <de> <até>
    private static void atenderPedido(String pedido, RecordFile file, PrintWriter out, String remoto)
            throws IOException {
        if (pedido == null) {
            // a réplica fechou a ligação sem enviar o pedido
            System.out.println("Ligação de " + remoto + " fechada sem pedido");
            return;
        }

        // validar o formato "PEDIDO <de> <até>"
        long de, ate;
        String[] p = pedido.trim().split("\\s+");
        try {
            if (p.length != 3 || !p[0].equals("PEDIDO")) {
                throw new IllegalArgumentException();
            }
            de = Long.parseLong(p[1]);
            ate = Long.parseLong(p[2]);
        } catch (IllegalArgumentException e) {
            out.println("ERRO esperado PEDIDO <de> <até>");
            System.out.println("Pedido mal formado de " + remoto + ": " + pedido);
            return;
        }
        // a ficha pede ERRO para pedido mal formado ou de > até
        if (de > ate) {
            out.println("ERRO intervalo inválido " + de + ".." + ate);
            System.out.println("Intervalo inválido de " + remoto + ": " + pedido);
            return;
        }

        // limitar 'até' ao último seq escrito (leitura do estado partilhado, protegida)
        long ultimo;
        synchronized (LOCK) {
            ultimo = ultimoSeq;
        }
        long ateEfetivo = Math.min(ate, ultimo);

        // o ficheiro do líder é a FONTE da recuperação; range é synchronized no RecordFile,
        // por isso nunca lê o ficheiro a meio de um append
        List<SensorRecord> regs = file.range(de, ateEfetivo);
        for (SensorRecord r : regs) {
            out.println(r.toLine());     // uma linha por registo: seq;sensor;temperatura;instante
        }
        out.println("FIM");              // fim da resposta; a ligação fecha ao sair do try

        // o PrintWriter não lança exceções: verifica se a escrita falhou
        if (out.checkError()) {
            throw new IOException("ligação interrompida durante a resposta a " + remoto);
        }
        System.out.println("RECUPERAÇÃO: " + de + ".." + ateEfetivo + " para " + remoto
                + " (" + regs.size() + " registos)");
    }
}