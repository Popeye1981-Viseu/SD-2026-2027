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
 * Diário de eventos do líder: `eventos-lider.txt`, uma linha por evento
 * no formato `instante;evento;detalhe` (E2-02, objetivo 2).
 *
 * `detalhe` pode conter ';' — quem lê deve usar split(";", 3).
 * A escrita é sincronizada porque é chamada de threads diferentes
 * (pool de comandos do JDA e agendador de selagem).
 */
public final class Eventos implements AutoCloseable {

    private final FileChannel canal;

    public Eventos(Path pasta) throws IOException {
        Files.createDirectories(pasta);
        // APPEND: cada registar() acrescenta no fim, sem reposicionar o canal.
        this.canal = FileChannel.open(pasta.resolve("eventos-lider.txt"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    /**
     * Acrescenta uma linha de evento e força-a para o disco.
     * Uma falha aqui NÃO aborta a operação principal: perder uma linha de
     * evidência é menos grave do que falhar uma escrita de registo já feita.
     */
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

    @Override
    public void close() throws IOException {
        canal.close();
    }
}