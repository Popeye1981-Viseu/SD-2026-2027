package tpf;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regras de texto do líder, testáveis sem Discord.
 *
 * A decisão que estes testes fixam: `isBlank()` VALIDA e `trim()` MUTA. O texto de
 * /registar é guardado exatamente como submetido; só os valores que são tokens
 * (ex.: o id de /replica-remover) são aparados antes de serem validados.
 */
class LeaderBotTest {

    @Test
    void registoPreservaEspacosIniciaisEFinais() throws Exception {
        assertEquals("  olá  ", LeaderBot.validarTexto("  olá  ", "texto", false));
        assertEquals("\n  y: 1\n", LeaderBot.validarTexto("\n  y: 1\n", "texto", false),
                "mudanças de linha no início/fim fazem parte do conteúdo");
        assertEquals("a\tb", LeaderBot.validarTexto("a\tb", "texto", false));
        assertEquals("fim   ", LeaderBot.validarTexto("fim   ", "texto", false));
    }

    @Test
    void identificadorEhAparado() throws Exception {
        assertEquals("abc", LeaderBot.validarTexto("  abc  ", "id", true));
        assertEquals("a-b_c", LeaderBot.validarTexto("\ta-b_c\n", "id", true));
    }

    @Test
    void textoSemConteudoEhRejeitadoNosDoisModos() {
        assertThrows(LeaderBot.ArgumentoInvalido.class,
                () -> LeaderBot.validarTexto("   ", "texto", false));
        assertThrows(LeaderBot.ArgumentoInvalido.class,
                () -> LeaderBot.validarTexto("   ", "id", true));
        assertThrows(LeaderBot.ArgumentoInvalido.class,
                () -> LeaderBot.validarTexto("", "texto", false));
        assertThrows(LeaderBot.ArgumentoInvalido.class,
                () -> LeaderBot.validarTexto(null, "texto", false));
    }

    @Test
    void erroNomeiaOArgumentoEmCausa() {
        LeaderBot.ArgumentoInvalido e = assertThrows(LeaderBot.ArgumentoInvalido.class,
                () -> LeaderBot.validarTexto("   ", "texto", false));
        assertTrue(e.getMessage().contains("texto"), e.getMessage());
        assertTrue(e.getMessage().contains("vazio"), e.getMessage());
    }
}