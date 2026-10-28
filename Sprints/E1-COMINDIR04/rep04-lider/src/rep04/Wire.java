package rep04;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;   // acima deste número de retidos, a réplica recupera
    public static final int T_TCP_MS = 5000;       // tempo máximo de ligação e de espera por cada linha
    public static final int PORTO_BASE = 7000;     // porto de recuperação do líder da partição k: 7000 + k

    // NOVO: Limiar para o processador de alarmes
    public static final double LIMIAR_ALARME = 28.0;

    private Wire() { }

    // ALTERADO: prefixo rep04
    public static String topicIn(int k)   { return "rep04-in-p" + k; }

    // ALTERADO: prefixo rep04
    public static String topicData(int k) { return "rep04-p" + k; }

    // ALTERADO: inclui L e V na mensagem IN
    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector.toString();
    }

    // ALTERADO: o r.toLine() já vai incluir os campos L e V no final,
    // resultando em: REG;<endereço>;<porto>;<seq>;<sensor>;<temperatura>;<instante>;<L>;<V>
    public static String reg(String host, int port, SensorRecord r) {
        return "REG;" + host + ";" + port + ";" + r.toLine();
    }

    // tipo da mensagem (IN, REG); lança IllegalArgumentException se não houver tipo
    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }
        return m.substring(0, i);
    }

    // conteúdo da mensagem sem o tipo
    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }

    // divide uma mensagem REG em { endereço, porto, linha do registo }
    public static String[] regParts(String msg) {
        String[] f = msg.trim().split(";", 4);
        if (f.length != 4 || !f[0].equals("REG")) {
            throw new IllegalArgumentException("REG mal formado: " + msg);
        }
        return new String[] { f[1], f[2], f[3] };
    }
}