package raft04;

import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Wire = "o fio": tudo o que é partilhado por TODOS os processos e que define
 * o protocolo de comunicação (constantes, nomes dos tópicos, portos e formato do IN).
 *
 * Não tem estado nem instâncias: é só uma "caixa de ferramentas" estática.
 * Como todos os processos (nó, processador, produtor) usam estas mesmas constantes,
 * qualquer alteração aqui tem de ser feita nos TRÊS projetos.
 *
 * COMINDIR (eleição de líder): em relação à ficha anterior desaparece tudo o que
 * se referia à incarnação (inc) e aparecem os parâmetros da eleição, o tópico da
 * eleição, o porto de cada nó e a simulação de partição de rede (isolated).
 */
public final class Wire {

    // Tempo máximo (ms) de ligação TCP e de espera por cada linha na recuperação.
    public static final int T_TCP_MS = 5000;

    // Os portos TCP de recuperação partem daqui (ver nodePort).
    public static final int PORTO_BASE = 7000;

    // Temperatura a partir da qual o processador de alarmes gera um alarme.
    public static final double LIMIAR_ALARME = 28.0;

    // PERIODO_HB_MS = Δ, o período do lote/heartbeat (por omissão 2000 ms).
    // Pode ser mudado SEM recompilar com  -Draft04.periodo=1000.
    public static final long PERIODO_HB_MS = Long.getLong("raft04.periodo", 2000L);

    // FATOR_TIMEOUT: o detetor do produtor/processador suspeita ao fim de PERIODO_HB_MS * FATOR_TIMEOUT.
    // Mudável com -Draft04.fator=...  (por omissão 3).
    public static final int FATOR_TIMEOUT = Integer.getInteger("raft04.fator", 3);

    // De quanto em quanto tempo (ms) cada processo verifica prazos/timeouts.
    public static final long VERIFICACAO_MS = 200L;

    // Prazo de eleição: sorteado uniformemente em [ELEICAO_MIN_MS, ELEICAO_MAX_MS].
    // Por omissão 3Δ e 6Δ (6000 ms e 12000 ms). Mudáveis com
    // -Draft04.eleicaoMin=... e -Draft04.eleicaoMax=... (o ensaio 9(b) usa os dois a 6000).
    public static final long ELEICAO_MIN_MS = Long.getLong("raft04.eleicaoMin", 3 * PERIODO_HB_MS);
    public static final long ELEICAO_MAX_MS = Long.getLong("raft04.eleicaoMax", 6 * PERIODO_HB_MS);

    // G = número de nós de cada grupo (um grupo por partição de dados).
    public static final int GRUPO = 3;

    // Construtor privado: ninguém deve fazer "new Wire()".
    private Wire() { }

    // Tópico onde o produtor/processador PUBLICAM as leituras (IN) para o líder da partição k.
    public static String topicIn(int k)       { return "raft04-in-p" + k; }

    // Tópico onde o líder da partição k PUBLICA os lotes (LOTE).
    public static String topicData(int k)     { return "raft04-p" + k; }

    // Tópico onde os nós da partição k trocam PEDIDO_VOTO e VOTO.
    public static String topicElection(int k) { return "raft04-eleicao-p" + k; }

    // Porto TCP de recuperação do nó i (1..GRUPO) da partição k.
    // Ex.: p0 -> 7001, 7002, 7003;  p1 -> 7011, 7012, 7013;  p2 -> 7021, 7022, 7023.
    public static int nodePort(int k, int i) { return PORTO_BASE + 10 * k + i; }

    // SIMULAÇÃO DE PARTIÇÃO DE REDE: o nó i da partição k está "isolado" enquanto
    // existir na pasta de trabalho um ficheiro chamado isolar-p<k>-n<i>.
    // Um nó isolado não publica nem processa mensagens dos tópicos de lotes e de eleição
    // e as suas ligações TCP de recuperação falham. O tópico dos IN NÃO é afetado.
    public static boolean isolated(int k, int i) {
        return Files.exists(Paths.get("isolar-p" + k + "-n" + i));
    }

    // Constrói a mensagem de entrada:  IN;<sensor>;<temperatura>;<L>;<V>
    // L e V são os carimbos lógicos (Lamport e vetorial) de quem envia.
    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector.toString();
    }

    // Devolve o TIPO da mensagem = texto até ao primeiro ';' (ex.: "IN", "LOTE", "VOTO").
    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) {
            // sem ';' (ou começa por ';') não há tipo: mensagem inválida
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }
        return m.substring(0, i);
    }

    // Devolve o CORPO da mensagem = tudo o que vem depois do primeiro ';'.
    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }
}
