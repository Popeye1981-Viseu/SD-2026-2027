package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regra de arranque: nunca criar uma master.key nova por cima de dados existentes.
 *
 * `Wire.carregarOuCriarMasterKey` cria a chave quando ela falta — correto no primeiro
 * arranque, perigoso quando já há `registos.log`, `eventos-lider.txt` ou `selados/`:
 * a chave nova torna os chunks antigos indecifráveis. Estes testes fixam essa recusa.
 */
class LeaderBotArranqueTest {

    private static Path chave(Path pasta)   { return pasta.resolve("master.key"); }
    private static Path log(Path pasta)     { return pasta.resolve(LogRegistos.FICHEIRO); }
    private static Path eventos(Path pasta) { return pasta.resolve(LogRegistos.FICHEIRO_EVENTOS); }

    private static void escreverDados(Path pasta) throws Exception {
        Files.write(log(pasta), "1;2025-01-01T00:00:00Z;ana;arquivo;QQ==\n".getBytes(StandardCharsets.UTF_8));
        Files.write(eventos(pasta), "2025-01-01T00:00:00Z;ARRANQUE;ok\n".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void primeiraExecucaoSemDadosPodeCriarAChave(@TempDir Path pasta) {
        assertNull(LeaderBot.recusaPorFaltaDeChave(chave(pasta), pasta, false),
                "sem dados, a chave pode (e deve) ser criada");
    }

    @Test
    void dadosSemChaveRecusamOArranque(@TempDir Path pasta) throws Exception {
        escreverDados(pasta);

        String recusa = LeaderBot.recusaPorFaltaDeChave(chave(pasta), pasta, false);

        assertNotNull(recusa, "com dados e sem chave o arranque tem de ser recusado");
        assertTrue(recusa.contains("master.key"), "a mensagem deve nomear a chave: " + recusa);
        assertTrue(recusa.contains("-Dtpf.novaChave=true"),
                "a mensagem deve indicar a saída explícita: " + recusa);
    }

    @Test
    void soComRegistosLogJaRecusa(@TempDir Path pasta) throws Exception {
        Files.write(log(pasta), "1;2025-01-01T00:00:00Z;ana;arquivo;QQ==\n".getBytes(StandardCharsets.UTF_8));
        assertNotNull(LeaderBot.recusaPorFaltaDeChave(chave(pasta), pasta, false),
                "um registos.log sozinho já é dados que uma chave nova tornaria órfãos");
    }

    @Test
    void soComEventosJaRecusa(@TempDir Path pasta) throws Exception {
        Files.write(eventos(pasta), "2025-01-01T00:00:00Z;ARRANQUE;ok\n".getBytes(StandardCharsets.UTF_8));
        assertNotNull(LeaderBot.recusaPorFaltaDeChave(chave(pasta), pasta, false));
    }

    @Test
    void comChaveExistenteArranca(@TempDir Path pasta) throws Exception {
        escreverDados(pasta);
        Files.write(chave(pasta), new byte[32]);
        assertNull(LeaderBot.recusaPorFaltaDeChave(chave(pasta), pasta, false));
    }

    @Test
    void novaChaveExplicitaPermiteContinuar(@TempDir Path pasta) throws Exception {
        escreverDados(pasta);
        assertNull(LeaderBot.recusaPorFaltaDeChave(chave(pasta), pasta, true),
                "-Dtpf.novaChave=true é a aceitação consciente de perder os chunks antigos");
    }

    @Test
    void pastaDeDadosInexistenteNaoRecusa(@TempDir Path pasta) {
        assertNull(LeaderBot.recusaPorFaltaDeChave(chave(pasta), pasta.resolve("nao-existe"), false),
                "sem pasta de dados é o primeiro arranque");
    }
}
