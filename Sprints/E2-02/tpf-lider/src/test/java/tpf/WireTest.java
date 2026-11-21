package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class WireTest {

    @Test
    void masterKeyCom16BytesLancaExcecaoEPreservaBytes(@TempDir Path tempDir) throws Exception {
        // 1. Prepara uma master.key inválida de 16 bytes numa pasta temporária
        Path caminho = tempDir.resolve("master.key");
        byte[] chaveInvalida = new byte[16];
        for (int i = 0; i < 16; i++) chaveInvalida[i] = (byte) i;
        Files.write(caminho, chaveInvalida);

        // 2. Invoca o método público e confirma que a exceção correta é lançada
        Wire.MasterKeyException ex = assertThrows(
                Wire.MasterKeyException.class,
                () -> Wire.carregarOuCriarMasterKey(caminho),
                "Uma master.key de 16 bytes devia originar MasterKeyException."
        );
        assertTrue(ex.getMessage().contains("32 bytes"),
                "A mensagem devia referir o tamanho esperado, mas foi: " + ex.getMessage());

        // 3. Confirma que o ficheiro original NÃO foi apagado nem substituído
        assertTrue(Files.exists(caminho),
                "O ficheiro original não devia ser apagado.");
        byte[] apos = Files.readAllBytes(caminho);
        assertArrayEquals(chaveInvalida, apos,
                "Os 16 bytes originais deviam ser preservados byte a byte (nada de substituição silenciosa).");
    }
}
