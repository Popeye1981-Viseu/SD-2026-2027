package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regras do hub (E2-03, objetivo 3): registo com token, desafio-resposta
 * Ed25519, subscrições, publicações e encaminhamento — sem sockets reais,
 * através de um canal de teste injetado.
 */
class PubSubHubTest {

    /** Canal de uma ligação de teste: guarda o que o hub enviou e se foi fechada. */
    static final class CanalFalso implements PubSubHub.CanalLigacao {
        final List<String> enviados = new ArrayList<>();
        boolean fechada;
        int codigo;
        String motivo;

        @Override public void enviar(String mensagem) { enviados.add(mensagem); }
        @Override public void fechar(int codigo, String motivo) {
            this.fechada = true;
            this.codigo = codigo;
            this.motivo = motivo;
        }

        String ultimoDesafio() {
            for (int i = enviados.size() - 1; i >= 0; i--) {
                if (enviados.get(i).startsWith("DESAFIO;")) return enviados.get(i);
            }
            return null;
        }
    }

    private record Contexto(PubSubHub hub, ReplicaRegistry registry, Path pasta,
                            List<String[]> recebidas, AtomicLong relogio) {}

    private static Contexto contexto(Path pasta) throws Exception {
        Eventos eventos = new Eventos(pasta);
        ReplicaRegistry registry = new ReplicaRegistry(eventos,
                new Placement(1.0, 0), () -> 1_000);
        AtomicLong relogio = new AtomicLong(1_000);
        List<String[]> recebidas = new ArrayList<>();
        PubSubHub hub = new PubSubHub(0, eventos, registry, pasta,
                (id, msg) -> recebidas.add(new String[]{id, msg}), relogio::get);
        return new Contexto(hub, registry, pasta, recebidas, relogio);
    }

    private static KeyPair par() throws Exception {
        return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    }

    private static String publicaB64(KeyPair par) {
        return Base64.getEncoder().encodeToString(par.getPublic().getEncoded());
    }

    private static String assinar(KeyPair par, String id, String nonceB64) throws Exception {
        Signature s = Signature.getInstance("Ed25519");
        s.initSign(par.getPrivate());
        s.update((PubSubHub.PREFIXO_ASSINATURA + id + "|" + nonceB64)
                .getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(s.sign());
    }

    /** Autentica uma ligação nova e devolve-a já em AUTENTICADA. */
    private static PubSubHub.Ligacao autenticar(Contexto c, String id, KeyPair par,
                                                String token, CanalFalso canal) throws Exception {
        PubSubHub.Ligacao l = c.hub().novaLigacao(canal);
        c.hub().processar(l, new Wire.Ola(id, publicaB64(par), token).encode());
        String desafio = canal.ultimoDesafio();
        assertNotNull(desafio, "o hub devia enviar DESAFIO");
        String nonce = desafio.substring("DESAFIO;".length());
        c.hub().processar(l, new Wire.Prova(id, assinar(par, id, nonce)).encode());
        assertFalse(canal.fechada, "a prova válida não devia fechar a ligação: " + canal.motivo);
        assertEquals(PubSubHub.Fase.AUTENTICADA, l.fase());
        return l;
    }

    @Test
    void primeiroRegistoValidoGravaReplicasTxt(@TempDir Path pasta) throws Exception {
        Contexto c = contexto(pasta);
        KeyPair par = par();
        String token = c.hub().gerarToken();
        CanalFalso canal = new CanalFalso();

        PubSubHub.Ligacao l = autenticar(c, "r1", par, token, canal);

        assertTrue(c.hub().registada("r1"));
        assertArrayEquals(par.getPublic().getEncoded(), c.hub().chavePublica("r1"));
        assertEquals(Estado.VIVA, c.registry().estado("r1"));

        List<String> linhas = Files.readAllLines(pasta.resolve(PubSubHub.FICHEIRO_REPLICAS),
                StandardCharsets.UTF_8);
        assertEquals(1, linhas.size());
        assertEquals("r1;" + publicaB64(par), linhas.get(0));

        String diario = Files.readString(pasta.resolve(LogRegistos.FICHEIRO_EVENTOS),
                StandardCharsets.UTF_8);
        assertTrue(diario.contains("REGISTO-REPLICA"), diario);
    }

    @Test
    void provaInvalidaNaoConsomeOTokenNemRegistaAReplica(@TempDir Path pasta) throws Exception {
        Contexto c = contexto(pasta);
        KeyPair par = par();
        KeyPair intruso = par();
        String token = c.hub().gerarToken();

        CanalFalso canal = new CanalFalso();
        PubSubHub.Ligacao l = c.hub().novaLigacao(canal);
        c.hub().processar(l, new Wire.Ola("r1", publicaB64(par), token).encode());
        String nonce = canal.ultimoDesafio().substring("DESAFIO;".length());

        // Assinatura feita com outra chave: recusada.
        c.hub().processar(l, new Wire.Prova("r1", assinar(intruso, "r1", nonce)).encode());
        assertTrue(canal.fechada);
        assertFalse(c.hub().registada("r1"));
        assertFalse(Files.exists(pasta.resolve(PubSubHub.FICHEIRO_REPLICAS)));
        assertEquals(1, c.hub().tokensAtivos(), "a prova inválida não pode consumir o token");

        // O mesmo token continua a funcionar numa ligação nova, com a chave certa.
        CanalFalso canal2 = new CanalFalso();
        autenticar(c, "r1", par, token, canal2);
        assertTrue(c.hub().registada("r1"));
        assertEquals(0, c.hub().tokensAtivos(), "o token é consumido na prova válida");
    }

    @Test
    void tokenReutilizadoEExpiradoSaoRecusados(@TempDir Path pasta) throws Exception {
        Contexto c = contexto(pasta);
        KeyPair par = par();
        String token = c.hub().gerarToken();
        autenticar(c, "r1", par, token, new CanalFalso());

        CanalFalso segundo = new CanalFalso();
        PubSubHub.Ligacao l = c.hub().novaLigacao(segundo);
        c.hub().processar(l, new Wire.Ola("r2", publicaB64(par), token).encode());
        assertTrue(segundo.fechada, "um token já consumido não pode registar outra réplica");
        assertFalse(c.hub().registada("r2"));

        String token2 = c.hub().gerarToken();
        c.relogio().addAndGet(Wire.TOKEN_VALIDADE_MS + 1);
        CanalFalso terceiro = new CanalFalso();
        PubSubHub.Ligacao l3 = c.hub().novaLigacao(terceiro);
        c.hub().processar(l3, new Wire.Ola("r3", publicaB64(par), token2).encode());
        assertTrue(terceiro.fechada, "um token com mais de 10 min não pode registar");
        assertFalse(c.hub().registada("r3"));
    }

    @Test
    void replicaRegistadaVoltaComOlaSimplesEProvaNova(@TempDir Path pasta) throws Exception {
        Contexto c = contexto(pasta);
        KeyPair par = par();
        autenticar(c, "r1", par, c.hub().gerarToken(), new CanalFalso());

        // Nova ligação: OLA;r1 (sem chave nem token) + desafio-resposta.
        CanalFalso canal = new CanalFalso();
        PubSubHub.Ligacao l = c.hub().novaLigacao(canal);
        c.hub().processar(l, new Wire.Ola("r1", null, null).encode());
        String nonce = canal.ultimoDesafio().substring("DESAFIO;".length());
        c.hub().processar(l, new Wire.Prova("r1", assinar(par, "r1", nonce)).encode());
        assertEquals(PubSubHub.Fase.AUTENTICADA, l.fase());
        assertFalse(canal.fechada);

        // Repetir a PROVA da ligação anterior (nonce antigo) falha na nova ligação.
        CanalFalso replayCanal = new CanalFalso();
        PubSubHub.Ligacao replay = c.hub().novaLigacao(replayCanal);
        c.hub().processar(replay, new Wire.Ola("r1", null, null).encode());
        c.hub().processar(replay, new Wire.Prova("r1", assinar(par, "r1", nonce)).encode());
        assertTrue(replayCanal.fechada, "o nonce é novo em cada ligação");

        // Assinar com outra chave (ou com outro id) também é recusado.
        CanalFalso outraChave = new CanalFalso();
        PubSubHub.Ligacao l2 = c.hub().novaLigacao(outraChave);
        c.hub().processar(l2, new Wire.Ola("r1", null, null).encode());
        String n2 = outraChave.ultimoDesafio().substring("DESAFIO;".length());
        c.hub().processar(l2, new Wire.Prova("r1", assinar(par(), "r1", n2)).encode());
        assertTrue(outraChave.fechada);

        CanalFalso outroId = new CanalFalso();
        PubSubHub.Ligacao l3 = c.hub().novaLigacao(outroId);
        c.hub().processar(l3, new Wire.Ola("r1", null, null).encode());
        String n3 = outroId.ultimoDesafio().substring("DESAFIO;".length());
        c.hub().processar(l3, new Wire.Prova("r2", assinar(par, "r2", n3)).encode());
        assertTrue(outroId.fechada, "o id da PROVA tem de ser o id anunciado no OLA");
    }

    @Test
    void olaComIdRegistadoEChaveDiferenteEhRecusado(@TempDir Path pasta) throws Exception {
        Contexto c = contexto(pasta);
        KeyPair par = par();
        autenticar(c, "r1", par, c.hub().gerarToken(), new CanalFalso());

        CanalFalso canal = new CanalFalso();
        PubSubHub.Ligacao l = c.hub().novaLigacao(canal);
        c.hub().processar(l, new Wire.Ola("r1", publicaB64(par()), c.hub().gerarToken()).encode());
        assertTrue(canal.fechada);
        assertEquals(PubSubHub.Fase.NOVA, l.fase());
    }

    @Test
    void mensagensForaDeOrdemDesligamERegistamErro(@TempDir Path pasta) throws Exception {
        Contexto c = contexto(pasta);
        KeyPair par = par();

        // SUB/PUB antes da PROVA.
        CanalFalso antes = new CanalFalso();
        PubSubHub.Ligacao l = c.hub().novaLigacao(antes);
        c.hub().processar(l, "SUB;tpf-ctl");
        assertTrue(antes.fechada);

        CanalFalso antes2 = new CanalFalso();
        PubSubHub.Ligacao l2 = c.hub().novaLigacao(antes2);
        c.hub().processar(l2, "PUB;tpf-lider;HB;r1;0;1");
        assertTrue(antes2.fechada);

        // PROVA sem desafio.
        CanalFalso semDesafio = new CanalFalso();
        PubSubHub.Ligacao l3 = c.hub().novaLigacao(semDesafio);
        c.hub().processar(l3, new Wire.Prova("r1", "abc").encode());
        assertTrue(semDesafio.fechada);

        // Segundo OLA.
        CanalFalso c2 = new CanalFalso();
        PubSubHub.Ligacao l4 = c.hub().novaLigacao(c2);
        c.hub().processar(l4, new Wire.Ola("r1", publicaB64(par), c.hub().gerarToken()).encode());
        c.hub().processar(l4, new Wire.Ola("r1", null, null).encode());
        assertTrue(c2.fechada);

        String diario = Files.readString(pasta.resolve(LogRegistos.FICHEIRO_EVENTOS),
                StandardCharsets.UTF_8);
        assertTrue(diario.contains(";ERRO;"), diario);
    }

    @Test
    void subscricaoEPublicacaoSoNosTopicosPermitidos(@TempDir Path pasta) throws Exception {
        Contexto c = contexto(pasta);
        KeyPair par = par();
        PubSubHub.Ligacao l = autenticar(c, "r1", par, c.hub().gerarToken(), new CanalFalso());

        // Subscrever o tópico de outra réplica desliga.
        CanalFalso intruso = new CanalFalso();
        PubSubHub.Ligacao l2 = autenticar(c, "r2", par, c.hub().gerarToken(), intruso);
        c.hub().processar(l2, "SUB;tpf-r-r1");
        assertTrue(intruso.fechada);

        // Os tópicos permitidos são aceites.
        CanalFalso ok = new CanalFalso();
        PubSubHub.Ligacao l3 = autenticar(c, "r3", par, c.hub().gerarToken(), ok);
        c.hub().processar(l3, new Wire.Sub("tpf-r-r3").encode());
        c.hub().processar(l3, new Wire.Sub("tpf-ctl").encode());
        assertFalse(ok.fechada);
        assertEquals(1, c.hub().subscritores("tpf-r-r3").size());
        assertEquals(1, c.hub().subscritores("tpf-ctl").size());

        // Publicar no tópico de outra réplica desliga.
        CanalFalso mauPub = new CanalFalso();
        PubSubHub.Ligacao l4 = autenticar(c, "r4", par, c.hub().gerarToken(), mauPub);
        c.hub().processar(l4, "PUB;tpf-r-r1;X");
        assertTrue(mauPub.fechada);

        // Publicar em tpf-lider é entregue ao líder com o id autenticado.
        c.recebidas().clear();
        c.hub().processar(l, "PUB;tpf-lider;HB;r1;10;1000");
        assertEquals(1, c.recebidas().size());
        assertEquals("r1", c.recebidas().get(0)[0]);
        assertEquals("HB;r1;10;1000", c.recebidas().get(0)[1]);
    }

    @Test
    void publishSoAlcancaQuemSubscreveuOTopico(@TempDir Path pasta) throws Exception {
        Contexto c = contexto(pasta);
        KeyPair par = par();

        CanalFalso c1 = new CanalFalso();
        CanalFalso c2 = new CanalFalso();
        PubSubHub.Ligacao l1 = autenticar(c, "r1", par, c.hub().gerarToken(), c1);
        PubSubHub.Ligacao l2 = autenticar(c, "r2", par, c.hub().gerarToken(), c2);
        c.hub().processar(l1, "SUB;tpf-r-r1");
        c.hub().processar(l2, "SUB;tpf-r-r2");
        c.hub().processar(l2, "SUB;tpf-ctl");
        c1.enviados.clear();
        c2.enviados.clear();

        assertEquals(1, c.hub().publish("tpf-r-r1", "STORE;abc;hdr;bin"));
        assertEquals(List.of("PUB;tpf-r-r1;STORE;abc;hdr;bin"), c1.enviados);
        assertTrue(c2.enviados.isEmpty(), "o tópico de r1 não chega a r2");

        assertEquals(1, c.hub().publish("tpf-ctl", "OLA"));
        assertEquals(List.of("PUB;tpf-ctl;OLA"), c2.enviados);

        assertEquals(0, c.hub().publish("tpf-r-inexistente", "X"));
    }

    @Test
    void verificarProvaAceitaSoAChaveEOConteudoCertos(@TempDir Path pasta) throws Exception {
        Contexto c = contexto(pasta);
        KeyPair par = par();
        String nonce = "bm9uY2U=";
        String assinatura = assinar(par, "r1", nonce);

        assertTrue(c.hub().verificarProva("r1", nonce, assinatura, publicaB64(par)));
        assertFalse(c.hub().verificarProva("r2", nonce, assinatura, publicaB64(par)));
        assertFalse(c.hub().verificarProva("r1", "b3V0cm8=", assinatura, publicaB64(par)));
        assertFalse(c.hub().verificarProva("r1", nonce, assinatura, publicaB64(par())));
        assertFalse(c.hub().verificarProva("r1", nonce, "nao-e-base64", publicaB64(par)));
    }

    @Test
    void removerReplicaRevogaAChaveEDesligaALigacao(@TempDir Path pasta) throws Exception {
        Contexto c = contexto(pasta);
        KeyPair par = par();
        CanalFalso canal = new CanalFalso();
        autenticar(c, "r1", par, c.hub().gerarToken(), canal);

        assertTrue(c.hub().removerReplica("r1"));
        assertFalse(c.hub().registada("r1"));
        assertTrue(canal.fechada);
        assertNull(c.registry().estado("r1"));
        assertEquals("", Files.readString(pasta.resolve(PubSubHub.FICHEIRO_REPLICAS),
                StandardCharsets.UTF_8));

        assertFalse(c.hub().removerReplica("r1"), "já não existe");
    }

    @Test
    void replicasTxtEhRecarregadoNoArranque(@TempDir Path pasta) throws Exception {
        Contexto c = contexto(pasta);
        KeyPair par = par();
        autenticar(c, "r1", par, c.hub().gerarToken(), new CanalFalso());

        Contexto novo = contexto(pasta);
        assertTrue(novo.hub().registada("r1"));
        assertEquals(Estado.MORTA, novo.registry().estado("r1"),
                "uma réplica registada mas ainda sem heartbeat fica MORTA");
    }
}
