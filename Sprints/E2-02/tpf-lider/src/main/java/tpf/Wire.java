package tpf;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.Set;

/**
 * Classe utilitária que centraliza a configuração do sistema e a gestão da master.key.
 */
public final class Wire {

    // Parâmetros configuráveis via propriedades do sistema (ex: -Dtpf.chunkMax=1024)
    public static final long CHUNK_MAX_BYTES = Long.parseLong(System.getProperty("tpf.chunkMax", "65536"));
    public static final long PAD_BYTES = Long.parseLong(System.getProperty("tpf.pad", "4096"));
    public static final long SELAGEM_MS = Long.parseLong(System.getProperty("tpf.selagem", "5000"));

    // Construtor privado para impedir a instanciação (classe puramente estática)
    private Wire() {}

    /**
     * Exceção personalizada para isolar falhas relacionadas com a master.key.
     * Permite que os testes unitários capturem o erro sem que a JVM seja terminada.
     */
    public static class MasterKeyException extends Exception {
        public MasterKeyException(String message) { super(message); }
        public MasterKeyException(String message, Throwable cause) { super(message, cause); }
    }

    /**
     * Carrega ou cria a master.key no caminho indicado.
     * @param caminho O caminho para o ficheiro (permite testes com pastas temporárias).
     * @return Um array de 32 bytes com a chave.
     * @throws MasterKeyException Se houver falha de I/O, se o ficheiro for inválido ou se a proteção falhar.
     */
    public static byte[] carregarOuCriarMasterKey(Path caminho) throws MasterKeyException {
        boolean criadaNesteArranque = false;

        // 1. Se o ficheiro já existe, apenas o lemos e validamos (NUNCA o apagamos).
        if (Files.exists(caminho)) {
            return lerChaveValida(caminho);
        }

        // 2. Se não existe, tentamos criá-lo atomicamente com permissões restritas.
        try {
            // Tenta usar POSIX (Linux/macOS) para garantir permissões "rw-------" (só o dono).
            Set<java.nio.file.attribute.PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");
            Files.createFile(caminho, PosixFilePermissions.asFileAttribute(perms));
            criadaNesteArranque = true;
        } catch (FileAlreadyExistsException e) {
            // CONDIÇÃO DE CORRIDA: Outro processo criou o ficheiro entre o Files.exists e o createFile.
            // Não abortamos. Assumimos que a chave é válida e tentamos lê-la.
            return lerChaveValida(caminho);
        } catch (UnsupportedOperationException e) {
            // FALLBACK WINDOWS: O sistema não suporta POSIX.
            try {
                Files.createFile(caminho);
                criadaNesteArranque = true;
                // Aplica permissões manuais para Windows e verifica se foram aceites.
                aplicarPermissoesWindows(caminho.toFile());
            } catch (FileAlreadyExistsException ex) {
                // Mesma condição de corrida, mas no Windows.
                return lerChaveValida(caminho);
            } catch (IOException ex) {
                // Se falhou a proteção, e o ficheiro foi criado por nós, limpamos o rasto.
                if (criadaNesteArranque) apagarFicheiroSeguro(caminho);
                throw new MasterKeyException("Falha ao criar ou proteger master.key no Windows.", ex);
            }
        } catch (IOException e) {
            throw new MasterKeyException("Falha ao criar a master.key.", e);
        }

        // 3. O ficheiro é nosso e está protegido. Geramos os 32 bytes e escrevemos.
        try {
            byte[] novaChave = new byte[32];
            new SecureRandom().nextBytes(novaChave); // Gerador criptograficamente seguro
            Files.write(caminho, novaChave);
            return novaChave;
        } catch (IOException e) {
            // Se a escrita falhar, o ficheiro fica a 0 bytes. Limpamos se fomos nós a criá-lo.
            if (criadaNesteArranque) apagarFicheiroSeguro(caminho);
            throw new MasterKeyException("Falha ao escrever na master.key.", e);
        }
    }

    /**
     * Lê o ficheiro e valida se tem exatamente 32 bytes.
     * Usa um ciclo de retry para lidar com processos que criaram o ficheiro mas ainda não escreveram os dados.
     */
    private static byte[] lerChaveValida(Path caminho) throws MasterKeyException {
        for (int i = 0; i < 5; i++) {
            try {
                byte[] chave = Files.readAllBytes(caminho);
                if (chave.length == 32) {
                    return chave;
                }
                // Se a leitura funcionou mas o tamanho é errado, pode ser uma escrita em curso.
                // Dormimos 50ms e tentamos novamente.
                Thread.sleep(50);
            } catch (IOException e) {
                // Falhas de I/O (ex: ficheiro apagado, sem permissões) abortam imediatamente. Não há retry.
                throw new MasterKeyException("Falha de I/O ao ler a master.key.", e);
            } catch (InterruptedException e) {
                // Restaura a flag de interrupção e aborta. O sleep foi interrompido.
                Thread.currentThread().interrupt();
                throw new MasterKeyException("Leitura da master.key interrompida.", e);
            }
        }
        // Se após 5 tentativas o ficheiro continuar a ter o tamanho errado, é inválido.
        throw new MasterKeyException("A master.key existente não tem 32 bytes após 5 tentativas.");
    }

    /**
     * Fallback de segurança para Windows.
     * O java.io.File não garante a aplicação das permissões, por isso verificamos os booleanos.
     */
    private static void aplicarPermissoesWindows(File ficheiro) throws IOException {
        // Remove acesso a todos, depois concede apenas ao dono.
        boolean outrosNaoLeem = ficheiro.setReadable(false, false);
        boolean outrosNaoEscrevem = ficheiro.setWritable(false, false);
        boolean donoLe = ficheiro.setReadable(true, true);
        boolean donoEscreve = ficheiro.setWritable(true, true);

        // Se o SO recusou alguma das operações, a segurança está comprometida.
        if (!outrosNaoLeem || !outrosNaoEscrevem || !donoLe || !donoEscreve) {
            throw new IOException("O sistema operativo recusou a aplicação de permissões restritas.");
        }
    }

    /**
     * Apaga o ficheiro de forma segura, alertando o administrador se não for possível.
     */
    private static void apagarFicheiroSeguro(Path caminho) {
        try {
            Files.deleteIfExists(caminho);
        } catch (IOException e) {
            // Imprime um bloco de erro altamente visível para não passar despercebido.
            System.err.println("==================================================");
            System.err.println("ERRO CRÍTICO: NÃO FOI POSSÍVEL APAGAR A MASTER.KEY COMPROMETIDA!");
            System.err.println("Caminho: " + caminho.toAbsolutePath());
            System.err.println("Causa: " + e.getMessage());
            System.err.println("AÇÃO OBRIGATÓRIA: Apague este ficheiro manualmente antes de reiniciar.");
            System.err.println("==================================================");
        }
    }
}