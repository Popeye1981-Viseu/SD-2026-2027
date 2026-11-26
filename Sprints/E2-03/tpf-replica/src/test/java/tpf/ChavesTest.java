package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyPair;

import static org.junit.jupiter.api.Assertions.*;

/** Persistência e proteção do par Ed25519 da réplica. */
class ChavesTest {

    @Test
    void criaCarregaEAssinaComOMesmoPar(@TempDir Path pasta) throws Exception {
        Chaves.Par primeiro = Chaves.carregarOuCriar(pasta);
        assertTrue(primeiro.criado());
        assertTrue(Files.exists(pasta.resolve(Chaves.FICHEIRO_PRIVADA)));
        assertTrue(Files.exists(pasta.resolve(Chaves.FICHEIRO_PUBLICA)));

        Chaves.Par segundo = Chaves.carregarOuCriar(pasta);
        assertFalse(segundo.criado());
        assertArrayEquals(primeiro.chaves().getPublic().getEncoded(),
                segundo.chaves().getPublic().getEncoded());
        assertArrayEquals(primeiro.chaves().getPrivate().getEncoded(),
                segundo.chaves().getPrivate().getEncoded());

        // A chave carregada assina e a pública verifica (é o que o hub faz).
        String assinatura = Chaves.assinar(segundo.chaves().getPrivate(), "A", "bm9uY2U=");
        java.security.Signature v = java.security.Signature.getInstance("Ed25519");
        v.initVerify(segundo.chaves().getPublic());
        v.update(Wire.conteudoAssinatura("A", "bm9uY2U=").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertTrue(v.verify(java.util.Base64.getDecoder().decode(assinatura)));
    }

    @Test
    void permissoesDaChavePrivadaSaoSoDoDono(@TempDir Path pasta) throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                FileSystems.getDefault().supportedFileAttributeViews().contains("posix"),
                "teste de permissões POSIX");
        Chaves.carregarOuCriar(pasta);
        assertEquals(PosixFilePermissions.fromString("rw-------"),
                Files.getPosixFilePermissions(pasta.resolve(Chaves.FICHEIRO_PRIVADA)));
        assertEquals(PosixFilePermissions.fromString("rw-------"),
                Files.getPosixFilePermissions(pasta.resolve(Chaves.FICHEIRO_PUBLICA)));
    }

    @Test
    void parIncompletoNaoEhRegeneradoEmSilencio(@TempDir Path pasta) throws Exception {
        Chaves.carregarOuCriar(pasta);
        Files.delete(pasta.resolve(Chaves.FICHEIRO_PUBLICA));

        IOException e = assertThrows(IOException.class, () -> Chaves.carregarOuCriar(pasta));
        assertTrue(e.getMessage().contains("incompleto"), e.getMessage());
        assertTrue(Files.exists(pasta.resolve(Chaves.FICHEIRO_PRIVADA)),
                "a privada existente não pode ser apagada");
        assertFalse(Files.exists(pasta.resolve(Chaves.FICHEIRO_PUBLICA)));
    }

    @Test
    void parQueNaoCorrespondeEhDetetado(@TempDir Path pasta) throws Exception {
        Chaves.carregarOuCriar(pasta);
        KeyPair outro = java.security.KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        Files.write(pasta.resolve(Chaves.FICHEIRO_PUBLICA), outro.getPublic().getEncoded());

        assertThrows(IOException.class, () -> Chaves.carregarOuCriar(pasta));
    }

    @Test
    void ficheiroCorrompidoAborta(@TempDir Path pasta) throws Exception {
        Chaves.carregarOuCriar(pasta);
        Files.write(pasta.resolve(Chaves.FICHEIRO_PRIVADA), new byte[]{1, 2, 3});
        assertThrows(GeneralSecurityException.class, () -> Chaves.carregarOuCriar(pasta));
    }
}
