package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Diário de eventos da réplica: {@code eventos-<id>.txt}, no formato
 * {@code instante;evento;detalhe} (o mesmo do líder).
 *
 * A escrita é sincronizada porque o serviço recebe mensagens na thread do
 * WebSocket e publica heartbeats na thread do agendador.
 */
public final class EventosReplica implements AutoCloseable {

    private final Path ficheiro;
    private final FileChannel canal;

    public EventosReplica(Path ficheiro) throws IOException {
        if (ficheiro == null) throw new IllegalArgumentException("ficheiro nulo");
        this.ficheiro = ficheiro;
        Path pasta = ficheiro.toAbsolutePath().getParent();
        if (pasta != null) Files.createDirectories(pasta);
        this.canal = FileChannel.open(ficheiro, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    public Path ficheiro() { return ficheiro; }

    /** Acrescenta uma linha e força-a para o disco. */
    public synchronized void registar(String evento, String detalhe) {
        String linha = Instant.now() + ";" + evento + ";"
                + (detalhe == null ? "" : detalhe.replace('\n', ' ').replace('\r', ' ')) + "\n";
        try {
            ByteBuffer buf = ByteBuffer.wrap(linha.getBytes(StandardCharsets.UTF_8));
            while (buf.hasRemaining()) canal.write(buf);
            canal.force(true);
        } catch (IOException e) {
            System.err.println("AVISO: não foi possível escrever o evento " + evento + ": " + e.getMessage());
        }
    }

    /** Regista e imprime na consola (para o operador ver o que acontece). */
    public synchronized void registarEImprimir(String evento, String detalhe) {
        registar(evento, detalhe);
        System.out.println(evento + " " + (detalhe == null ? "" : detalhe));
    }

    @Override
    public void close() throws IOException {
        canal.close();
    }
}
