package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Armazenamento cego da réplica: quota, verificação do hash, escrita atómica
 * e ACK apenas depois dos ficheiros, bem como a soma dos usados no arranque.
 */
class ArmazemTest {

    private static byte[] dados(int n, long semente) {
        byte[] b = new byte[n];
        new Random(semente).nextBytes(b);
        return b;
    }

    private static String hdr() {
        return "1;c2FsdA==;bm9uY2U=\n";
    }

    @Test
    void gravaBinEHdrEDepoisContaOsUsados(@TempDir Path pasta) throws Exception {
        EventosReplica eventos = new EventosReplica(pasta.resolve("eventos-t.txt"));
        Armazem armazem = new Armazem(pasta.resolve("dados"), 10_000, eventos);

        byte[] bin = dados(400, 1);
        String chunkId = Armazem.sha256Hex(bin);
        Armazem.Rececao r = armazem.receber(chunkId, hdr().getBytes(StandardCharsets.UTF_8), bin);

        assertEquals(Armazem.Resultado.OK, r.resultado());
        assertEquals(chunkId, r.sha256());
        assertEquals(400, armazem.usados());
        assertEquals(1, armazem.escritas());

        Path ficheiroBin = pasta.resolve("dados").resolve(chunkId + Armazem.EXT_BIN);
        Path ficheiroHdr = pasta.resolve("dados").resolve(chunkId + Armazem.EXT_HDR);
        assertArrayEquals(bin, Files.readAllBytes(ficheiroBin));
        assertEquals(hdr(), Files.readString(ficheiroHdr, StandardCharsets.UTF_8));
        assertEquals(chunkId, Armazem.sha256Hex(Files.readAllBytes(ficheiroBin)),
                "o nome do .bin é o SHA-256 do seu conteúdo");

        assertEquals(java.util.List.of(chunkId), armazem.inventario());
        String diario = Files.readString(pasta.resolve("eventos-t.txt"), StandardCharsets.UTF_8);
        assertTrue(diario.contains(";STORE;"), diario);
        assertTrue(diario.contains(chunkId), diario);
    }

    @Test
    void storeDuplicadoRespondeOkSemReescrever(@TempDir Path pasta) throws Exception {
        EventosReplica eventos = new EventosReplica(pasta.resolve("eventos-t.txt"));
        Armazem armazem = new Armazem(pasta.resolve("dados"), 10_000, eventos);
        byte[] bin = dados(100, 2);
        String chunkId = Armazem.sha256Hex(bin);

        assertEquals(Armazem.Resultado.OK,
                armazem.receber(chunkId, hdr().getBytes(StandardCharsets.UTF_8), bin).resultado());
        assertEquals(1, armazem.escritas());

        Armazem.Rececao segundo = armazem.receber(chunkId,
                hdr().getBytes(StandardCharsets.UTF_8), bin);
        assertEquals(Armazem.Resultado.OK, segundo.resultado());
        assertEquals(1, armazem.escritas(), "não pode haver uma segunda escrita");
        assertEquals(100, armazem.usados());
    }

    @Test
    void storeDuplicadoComQuotaCheiaRespondeOkSemReescreverNemRegistarQuota(@TempDir Path pasta)
            throws Exception {
        EventosReplica eventos = new EventosReplica(pasta.resolve("eventos-t.txt"));
        // Quota exatamente igual ao chunk: depois do primeiro STORE fica cheia.
        Path dados = pasta.resolve("dados");
        Armazem armazem = new Armazem(dados, 100, eventos);
        byte[] bin = dados(100, 40);
        String chunkId = Armazem.sha256Hex(bin);

        assertEquals(Armazem.Resultado.OK,
                armazem.receber(chunkId, hdr().getBytes(StandardCharsets.UTF_8), bin).resultado());
        assertEquals(100, armazem.usados());
        assertEquals(1, armazem.escritas());
        Path ficheiroBin = dados.resolve(chunkId + Armazem.EXT_BIN);
        assertTrue(Files.exists(ficheiroBin));

        // Duplicado com a quota cheia: é um ACK idempotente, não um QUOTA.
        Armazem.Rececao duplicado = armazem.receber(chunkId,
                hdr().getBytes(StandardCharsets.UTF_8), bin);
        assertEquals(Armazem.Resultado.OK, duplicado.resultado());
        assertEquals("já existia", duplicado.detalhe());
        assertEquals(100, armazem.usados(), "o duplicado não consome espaço novo");
        assertEquals(1, armazem.escritas(), "o .bin não pode ser reescrito");
        assertTrue(Files.exists(ficheiroBin));

        String diarioAposDuplicado = Files.readString(pasta.resolve("eventos-t.txt"),
                StandardCharsets.UTF_8);
        assertFalse(diarioAposDuplicado.contains(";QUOTA;"),
                "um STORE duplicado nunca pode ser registado como QUOTA");
        assertTrue(diarioAposDuplicado.contains("(já existia)"), diarioAposDuplicado);

        // Um chunk NOVO continua a ser recusado quando não cabe.
        byte[] novo = dados(1, 41);
        String novoId = Armazem.sha256Hex(novo);
        assertEquals(Armazem.Resultado.QUOTA,
                armazem.receber(novoId, hdr().getBytes(StandardCharsets.UTF_8), novo).resultado());
        assertEquals(100, armazem.usados());
        assertEquals(1, armazem.escritas());
        assertFalse(Files.exists(dados.resolve(novoId + Armazem.EXT_BIN)));
        String diarioFinal = Files.readString(pasta.resolve("eventos-t.txt"),
                StandardCharsets.UTF_8);
        assertEquals(1, diarioFinal.lines().filter(l -> l.contains(";QUOTA;")).count(), diarioFinal);
    }

    @Test
    void recusaPorQuotaSemGravarNemPublicarAck(@TempDir Path pasta) throws Exception {
        EventosReplica eventos = new EventosReplica(pasta.resolve("eventos-t.txt"));
        Armazem armazem = new Armazem(pasta.resolve("dados"), 500, eventos);

        byte[] grande = dados(501, 3);
        String chunkId = Armazem.sha256Hex(grande);
        Armazem.Rececao r = armazem.receber(chunkId, hdr().getBytes(StandardCharsets.UTF_8), grande);

        assertEquals(Armazem.Resultado.QUOTA, r.resultado());
        assertEquals(0, armazem.usados());
        assertFalse(Files.exists(pasta.resolve("dados").resolve(chunkId + Armazem.EXT_BIN)));
        String diario = Files.readString(pasta.resolve("eventos-t.txt"), StandardCharsets.UTF_8);
        assertTrue(diario.contains(";QUOTA;"), diario);
    }

    @Test
    void hashErradoRecusaSemGravar(@TempDir Path pasta) throws Exception {
        EventosReplica eventos = new EventosReplica(pasta.resolve("eventos-t.txt"));
        Armazem armazem = new Armazem(pasta.resolve("dados"), 10_000, eventos);

        byte[] bin = dados(100, 4);
        String errado = Armazem.sha256Hex(dados(100, 999));
        Armazem.Rececao r = armazem.receber(errado, hdr().getBytes(StandardCharsets.UTF_8), bin);

        assertEquals(Armazem.Resultado.HASH, r.resultado());
        assertEquals(0, armazem.usados());
        assertFalse(Files.exists(pasta.resolve("dados").resolve(errado + Armazem.EXT_BIN)));
        String diario = Files.readString(pasta.resolve("eventos-t.txt"), StandardCharsets.UTF_8);
        assertTrue(diario.contains(";ERRO;"), diario);
        assertTrue(diario.contains("INTEGRIDADE"), diario);
    }

    @Test
    void usadosSaoRecalculadosSomandoOsBinNoArranque(@TempDir Path pasta) throws Exception {
        EventosReplica eventos = new EventosReplica(pasta.resolve("eventos-t.txt"));
        Path dados = pasta.resolve("dados");
        Armazem primeiro = new Armazem(dados, 10_000, eventos);
        for (int i = 0; i < 3; i++) {
            byte[] bin = dados(100 + i, 10 + i);
            primeiro.receber(Armazem.sha256Hex(bin), hdr().getBytes(StandardCharsets.UTF_8), bin);
        }
        assertEquals(303, primeiro.usados());

        Armazem segundo = new Armazem(dados, 10_000, eventos);
        assertEquals(303, segundo.usados(), "o arranque soma os .bin da pasta");
        assertEquals(3, segundo.inventario().size());
    }

    @Test
    void completarHdrEmFaltaMantemOBin(@TempDir Path pasta) throws Exception {
        EventosReplica eventos = new EventosReplica(pasta.resolve("eventos-t.txt"));
        Path dados = pasta.resolve("dados");
        Armazem armazem = new Armazem(dados, 10_000, eventos);
        byte[] bin = dados(100, 20);
        String chunkId = Armazem.sha256Hex(bin);

        assertEquals(Armazem.Resultado.OK,
                armazem.receber(chunkId, hdr().getBytes(StandardCharsets.UTF_8), bin).resultado());
        Files.delete(dados.resolve(chunkId + Armazem.EXT_HDR));

        // O bin válido fica; só o cabeçalho é reescrito.
        assertEquals(Armazem.Resultado.OK,
                armazem.receber(chunkId, hdr().getBytes(StandardCharsets.UTF_8), bin).resultado());
        assertTrue(Files.exists(dados.resolve(chunkId + Armazem.EXT_BIN)));
        assertTrue(Files.exists(dados.resolve(chunkId + Armazem.EXT_HDR)));
        assertEquals(1, armazem.escritas(), "o bin não volta a ser escrito");
    }

    @Test
    void binCorrompidoNoDiscoEhDetetado(@TempDir Path pasta) throws Exception {
        EventosReplica eventos = new EventosReplica(pasta.resolve("eventos-t.txt"));
        Path dados = pasta.resolve("dados");
        Armazem armazem = new Armazem(dados, 10_000, eventos);
        byte[] bin = dados(100, 30);
        String chunkId = Armazem.sha256Hex(bin);
        armazem.receber(chunkId, hdr().getBytes(StandardCharsets.UTF_8), bin);

        // Altera um byte do .bin: o nome deixa de corresponder ao conteúdo.
        Path ficheiro = dados.resolve(chunkId + Armazem.EXT_BIN);
        byte[] alterado = Files.readAllBytes(ficheiro);
        alterado[0] ^= 0x01;
        Files.write(ficheiro, alterado);

        assertEquals(Armazem.Resultado.HASH,
                armazem.receber(chunkId, hdr().getBytes(StandardCharsets.UTF_8), bin).resultado());
    }
}
