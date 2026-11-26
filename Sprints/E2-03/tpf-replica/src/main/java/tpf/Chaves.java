package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Set;
import java.util.UUID;

/**
 * Par de chaves Ed25519 persistido em {@code dados-<id>/} (E2-03).
 *
 *   chave-privada.der   PKCS#8
 *   chave-publica.der   X.509
 *
 * As permissões são {@code rw-------} (só o dono), porque a chave privada é a
 * identidade da réplica: quem a tiver pode autenticar-se como ela. Um par
 * incompleto ou ilegível NÃO é substituído em silêncio — a réplica recusa
 * arrancar, para não criar uma identidade nova por cima da antiga.
 */
public final class Chaves {

    public static final String FICHEIRO_PRIVADA = "chave-privada.der";
    public static final String FICHEIRO_PUBLICA = "chave-publica.der";

    private static final SecureRandom RNG = new SecureRandom();

    /** Resultado de carregar/criar: o par e se foi criado neste arranque. */
    public record Par(KeyPair chaves, boolean criado) {}

    private Chaves() {}

    public static Par carregarOuCriar(Path pasta) throws IOException, GeneralSecurityException {
        Files.createDirectories(pasta);
        Path privada = pasta.resolve(FICHEIRO_PRIVADA);
        Path publica = pasta.resolve(FICHEIRO_PUBLICA);

        boolean temPrivada = Files.exists(privada);
        boolean temPublica = Files.exists(publica);

        if (temPrivada != temPublica) {
            throw new IOException("par de chaves incompleto em " + pasta.toAbsolutePath()
                    + ": existe " + (temPrivada ? FICHEIRO_PRIVADA : FICHEIRO_PUBLICA)
                    + " mas falta o outro ficheiro. Não é gerada uma identidade nova em silêncio;"
                    + " reponha o par a partir de uma cópia de segurança ou remova os dois"
                    + " ficheiros para aceitar um registo novo.");
        }

        if (temPrivada) {
            PrivateKey chavePrivada = KeyFactory.getInstance("Ed25519")
                    .generatePrivate(new PKCS8EncodedKeySpec(Files.readAllBytes(privada)));
            PublicKey chavePublica = KeyFactory.getInstance("Ed25519")
                    .generatePublic(new X509EncodedKeySpec(Files.readAllBytes(publica)));
            verificarPar(chavePrivada, chavePublica, pasta);
            return new Par(new KeyPair(chavePublica, chavePrivada), false);
        }

        KeyPairGenerator gerador = KeyPairGenerator.getInstance("Ed25519");
        KeyPair par = gerador.generateKeyPair();
        escreverSeguro(privada, par.getPrivate().getEncoded());
        escreverSeguro(publica, par.getPublic().getEncoded());
        return new Par(par, true);
    }

    /** Assina o conteúdo de registo com a chave privada (Ed25519). */
    public static String assinar(PrivateKey chave, String id, String nonceB64)
            throws GeneralSecurityException {
        Signature s = Signature.getInstance("Ed25519");
        s.initSign(chave);
        s.update(Wire.conteudoAssinatura(id, nonceB64).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return java.util.Base64.getEncoder().encodeToString(s.sign());
    }

    /**
     * Confirma que os dois ficheiros formam um par: assina um nonce fixo com a
     * privada e verifica com a pública. Sem isto, uma cópia trocada só falharia
     * no primeiro PROVA, já em produção.
     */
    private static void verificarPar(PrivateKey privada, PublicKey publica, Path pasta)
            throws GeneralSecurityException, IOException {
        byte[] nonce = new byte[32];
        RNG.nextBytes(nonce);
        String nonceB64 = java.util.Base64.getEncoder().encodeToString(nonce);
        byte[] dados = Wire.conteudoAssinatura("verificacao-local", nonceB64)
                .getBytes(java.nio.charset.StandardCharsets.UTF_8);

        Signature s = Signature.getInstance("Ed25519");
        s.initSign(privada);
        s.update(dados);
        byte[] assinatura = s.sign();

        Signature v = Signature.getInstance("Ed25519");
        v.initVerify(publica);
        v.update(dados);
        if (!v.verify(assinatura)) {
            throw new IOException("as chaves de " + pasta.toAbsolutePath()
                    + " não formam um par Ed25519 válido (privada e pública não correspondem)");
        }
    }

    private static void escreverSeguro(Path destino, byte[] dados) throws IOException {
        Path pasta = destino.toAbsolutePath().getParent();
        Path temporario = pasta.resolve("." + destino.getFileName() + "." + UUID.randomUUID() + ".tmp");
        Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");

        try {
            try {
                Files.createFile(temporario, PosixFilePermissions.asFileAttribute(perms));
            } catch (UnsupportedOperationException e) {
                Files.createFile(temporario);
            }

            try (FileChannel ch = FileChannel.open(temporario, StandardOpenOption.WRITE)) {
                ByteBuffer buf = ByteBuffer.wrap(dados);
                while (buf.hasRemaining()) ch.write(buf);
                ch.force(true);
            }

            try {
                Files.move(temporario, destino, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporario, destino, StandardCopyOption.REPLACE_EXISTING);
            }

            // Reforça as permissões depois do move (alguns sistemas não as preservam).
            try {
                Files.setPosixFilePermissions(destino, perms);
            } catch (UnsupportedOperationException ignorada) {
                // Windows: a proteção possível é a do diretório do utilizador.
            }
            sincronizarPasta(pasta);
        } finally {
            Files.deleteIfExists(temporario);
        }
    }

    /** fsync ao diretório, best-effort (ver README do líder, §5.2.6). */
    private static void sincronizarPasta(Path pasta) {
        try (FileChannel ch = FileChannel.open(pasta, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignorada) {
            // Sem suporte no SO: a operação continua, mas fica documentada.
        }
    }
}
