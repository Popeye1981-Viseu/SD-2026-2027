package tpf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Chunk do arquivo (E2-02, objetivo 3): acumula registos, serializa com padding
 * e reconstrói-se por leitura direta ou inversa.
 *
 * Formato do TEXTO EM CLARO (antes de cifrar), tudo em big-endian:
 *
 *   offset 0   "TPFCH1"        6 bytes   magic + versão do formato
 *   offset 6   int32 lenConteudo         bytes ocupados pelos registos
 *   offset 10  int32 numeroRegistos      quantos registos o chunk contém
 *   offset 14  registos, por ordem de LSN, cada um:
 *                  corpo || int32 len(corpo)
 *   offset 14+lenConteudo ... padding de zeros até múltiplo de Wire.PAD_BYTES
 *
 * corpo de um registo:
 *   int64  lsn
 *   int64  instante (epoch millis, UTC)
 *   int32  len(autor)  + bytes UTF-8
 *   int32  len(canal)  + bytes UTF-8
 *   int32  len(texto)  + bytes UTF-8
 *
 * O int32 no FIM de cada corpo é o que torna a leitura inversa possível: a partir
 * do fim do conteúdo lê-se esse tamanho e recua-se exatamente esse número de
 * bytes. O padding NÃO é distinguível por conteúdo (são zeros, e um campo de
 * tamanho pode legitimamente terminar em 0x00), por isso o cabeçalho declara
 * onde acaba o conteúdo e o padding é validado como integralmente nulo.
 *
 * O instante é guardado com precisão de milissegundos.
 */
public final class Chunk {

    /** Assinatura do formato: 5 letras + versão. */
    public static final byte[] MAGIC = {'T', 'P', 'F', 'C', 'H', '1'};

    /** 6 (magic) + 4 (lenConteudo) + 4 (numeroRegistos). */
    static final int CABECALHO = MAGIC.length + 8;

    /** lsn(8) + instante(8) + 3 * (len(4)) = 28 bytes, o mínimo de um corpo. */
    static final int MIN_CORPO = 28;

    /** O texto em claro (ou o padding, ou o cabeçalho) não obedece ao formato. */
    public static final class CorrupcaoException extends Exception {
        public CorrupcaoException(String mensagem) { super(mensagem); }
    }

    private final List<Registo> registos = new ArrayList<>();
    private long bytesCorpo;          // soma de (corpo + 4) dos registos
    private Instant primeiroInstante;

    private Chunk() {}

    /** Abre um chunk novo com o primeiro registo. */
    public static Chunk novo(Registo r) {
        Chunk c = new Chunk();
        c.adicionar(r);
        return c;
    }

    /** Acrescenta um registo ao chunk aberto (a ordem é a dos LSN). */
    public void adicionar(Registo r) {
        registos.add(r);
        bytesCorpo += corpoDe(r).length + 4;
        if (primeiroInstante == null) primeiroInstante = r.instante();
    }

    public boolean vazio() { return registos.isEmpty(); }

    public int tamanho() { return registos.size(); }

    /** Tamanho em claro, sem padding (é este valor que se compara com CHUNK_MAX_BYTES). */
    public long bytes() { return CABECALHO + bytesCorpo; }

    /** Tamanho em claro depois do padding (múltiplo de PAD_BYTES). */
    public long bytesComPadding() { return arredondarPadding(bytes()); }

    public Instant primeiroInstante() { return primeiroInstante; }

    public long lsnMinimo() { return registos.get(0).lsn(); }

    public long lsnMaximo() { return registos.get(registos.size() - 1).lsn(); }

    public List<Registo> registos() { return List.copyOf(registos); }

    /** Arredonda para cima até um múltiplo de Wire.PAD_BYTES. */
    static long arredondarPadding(long n) {
        long p = Wire.PAD_BYTES;
        return ((n + p - 1) / p) * p;
    }

    // ------------------------------------------------------------------
    // Serialização
    // ------------------------------------------------------------------

    /**
     * Serializa o chunk com padding até um múltiplo de Wire.PAD_BYTES.
     * O padding são zeros no fim do array (a alocação já vem a zeros).
     */
    public byte[] serializarComPadding() {
        int total = Math.toIntExact(bytesComPadding());
        ByteBuffer buf = ByteBuffer.allocate(total);

        buf.put(MAGIC);
        buf.putInt(Math.toIntExact(bytesCorpo));
        buf.putInt(registos.size());

        for (Registo r : registos) {
            byte[] corpo = corpoDe(r);
            buf.put(corpo);
            buf.putInt(corpo.length);        // tamanho no FIM: habilita a leitura inversa
        }
        // O resto do buffer fica a zeros: é o padding.
        return buf.array();
    }

    /** Corpo binário de um registo (sem o int32 de tamanho). */
    static byte[] corpoDe(Registo r) {
        byte[] autor = r.autor().getBytes(StandardCharsets.UTF_8);
        byte[] canal = r.canal().getBytes(StandardCharsets.UTF_8);
        byte[] texto = r.texto().getBytes(StandardCharsets.UTF_8);

        ByteBuffer b = ByteBuffer.allocate(16 + 12 + autor.length + canal.length + texto.length);
        b.putLong(r.lsn());
        b.putLong(r.instante().toEpochMilli());
        b.putInt(autor.length); b.put(autor);
        b.putInt(canal.length); b.put(canal);
        b.putInt(texto.length); b.put(texto);
        return b.array();
    }

    // ------------------------------------------------------------------
    // Leitura direta (do início para o fim)
    // ------------------------------------------------------------------

    /** Reconstrói os registos lendo o conteúdo de frente para trás. */
    public static List<Registo> ler(byte[] claro) throws CorrupcaoException {
        int[] envelope = validarEnvelope(claro);
        int lenConteudo = envelope[0];
        int numero = envelope[1];

        ByteBuffer todo = ByteBuffer.wrap(claro);
        todo.position(CABECALHO);
        todo.limit(CABECALHO + lenConteudo);
        ByteBuffer conteudo = todo.slice();

        List<Registo> res = new ArrayList<>(numero);
        for (int i = 0; i < numero; i++) {
            int inicio = conteudo.position();
            Registo r = lerCorpo(conteudo, i);
            int escrito = conteudo.position() - inicio;

            if (conteudo.remaining() < 4) {
                throw new CorrupcaoException("registo " + i + ": falta o int32 de tamanho no fim do corpo");
            }
            int declarado = conteudo.getInt();
            if (declarado != escrito) {
                throw new CorrupcaoException("registo " + i + ": tamanho declarado " + declarado
                        + " não corresponde aos " + escrito + " bytes lidos");
            }
            res.add(r);
        }
        if (conteudo.hasRemaining()) {
            throw new CorrupcaoException("sobram " + conteudo.remaining()
                    + " bytes no conteúdo depois de " + numero + " registos");
        }
        return res;
    }

    // ------------------------------------------------------------------
    // Leitura inversa (do fim para o início), ignorando o padding
    // ------------------------------------------------------------------

    /**
     * Reconstrói os registos lendo o conteúdo DE TRÁS PARA A FRENTE.
     *
     * O padding não é percorrido: o cabeçalho diz onde termina o conteúdo, e o
     * intervalo [fimConteudo, claro.length) só é aceite se for todo nulo.
     * A partir do fim, cada int32 de tamanho indica quanto recuar para encontrar
     * o corpo anterior; no fim, a lista é invertida para ficar por ordem de LSN.
     */
    public static List<Registo> lerInverso(byte[] claro) throws CorrupcaoException {
        int[] envelope = validarEnvelope(claro);
        int lenConteudo = envelope[0];
        int numero = envelope[1];

        int inicioConteudo = CABECALHO;
        int fimConteudo = CABECALHO + lenConteudo;

        List<Registo> res = new ArrayList<>(numero);
        int pos = fimConteudo;

        for (int i = 0; i < numero; i++) {
            if (pos - 4 < inicioConteudo) {
                throw new CorrupcaoException("leitura inversa: sem espaço para o tamanho do registo "
                        + (numero - 1 - i) + " (posição " + pos + ")");
            }
            int declarado = ByteBuffer.wrap(claro, pos - 4, 4).getInt();
            if (declarado < MIN_CORPO) {
                throw new CorrupcaoException("leitura inversa: tamanho de corpo inválido (" + declarado
                        + ") na posição " + (pos - 4));
            }
            int inicioCorpo = pos - 4 - declarado;
            if (inicioCorpo < inicioConteudo) {
                throw new CorrupcaoException("leitura inversa: o registo na posição " + (pos - 4)
                        + " começa antes do conteúdo (" + inicioCorpo + " < " + inicioConteudo + ")");
            }

            ByteBuffer corpo = ByteBuffer.wrap(claro, inicioCorpo, declarado);
            res.add(lerCorpo(corpo, numero - 1 - i));
            pos = inicioCorpo;
        }

        if (pos != inicioConteudo) {
            throw new CorrupcaoException("leitura inversa: sobraram " + (pos - inicioConteudo)
                    + " bytes por consumir antes do primeiro registo");
        }

        // Lida de trás para a frente, a lista está ao contrário: põe-se na ordem dos LSN.
        Collections.reverse(res);
        return res;
    }

    // ------------------------------------------------------------------
    // Validação comum
    // ------------------------------------------------------------------

    /** Valida magic, alinhamento, cabeçalho e padding. Devolve {lenConteudo, numeroRegistos}. */
    private static int[] validarEnvelope(byte[] claro) throws CorrupcaoException {
        if (claro == null) throw new CorrupcaoException("texto em claro nulo");

        int alinhamento = (int) Wire.PAD_BYTES;
        if (claro.length < CABECALHO) {
            throw new CorrupcaoException("chunk com " + claro.length + " bytes: menor que o cabeçalho ("
                    + CABECALHO + ")");
        }
        if (claro.length % alinhamento != 0) {
            throw new CorrupcaoException("chunk com " + claro.length + " bytes: não é múltiplo de PAD_BYTES ("
                    + alinhamento + ")");
        }
        for (int i = 0; i < MAGIC.length; i++) {
            if (claro[i] != MAGIC[i]) {
                throw new CorrupcaoException("assinatura inválida no offset " + i
                        + ": não é um chunk no formato TPFCH1");
            }
        }

        ByteBuffer cab = ByteBuffer.wrap(claro, MAGIC.length, 8);
        int lenConteudo = cab.getInt();
        int numero = cab.getInt();

        if (lenConteudo < 0 || numero < 0) {
            throw new CorrupcaoException("cabeçalho inválido: lenConteudo=" + lenConteudo
                    + ", numeroRegistos=" + numero);
        }
        if ((long) CABECALHO + lenConteudo > claro.length) {
            throw new CorrupcaoException("cabeçalho inválido: conteúdo de " + lenConteudo
                    + " bytes não cabe em " + claro.length);
        }
        if (numero > lenConteudo / (MIN_CORPO + 4)) {
            throw new CorrupcaoException("cabeçalho inválido: " + numero + " registos não cabem em "
                    + lenConteudo + " bytes");
        }

        // O padding tem de ser todo nulo: um byte diferente de zero é conteúdo a mais.
        for (int i = CABECALHO + lenConteudo; i < claro.length; i++) {
            if (claro[i] != 0) {
                throw new CorrupcaoException("padding inválido no offset " + i
                        + ": esperava zero, encontrei 0x" + Integer.toHexString(claro[i] & 0xFF));
            }
        }
        return new int[]{lenConteudo, numero};
    }

    /** Lê um corpo de registo a partir da posição atual do buffer. */
    private static Registo lerCorpo(ByteBuffer b, int idx) throws CorrupcaoException {
        try {
            long lsn = b.getLong();
            long millis = b.getLong();
            String autor = lerTexto(b, idx, "autor");
            String canal = lerTexto(b, idx, "canal");
            String texto = lerTexto(b, idx, "texto");
            if (lsn < 1) throw new CorrupcaoException("registo " + idx + ": LSN inválido (" + lsn + ")");
            return new Registo(lsn, Instant.ofEpochMilli(millis), autor, canal, texto);

        } catch (java.nio.BufferUnderflowException e) {
            throw new CorrupcaoException("registo " + idx + ": corpo truncado (faltam bytes)");
        } catch (IllegalArgumentException e) {
            throw new CorrupcaoException("registo " + idx + ": " + e.getMessage());
        }
    }

    /** Lê um campo de texto com prefixo de tamanho. */
    private static String lerTexto(ByteBuffer b, int idx, String campo) throws CorrupcaoException {
        if (b.remaining() < 4) {
            throw new CorrupcaoException("registo " + idx + ": falta o tamanho do campo " + campo);
        }
        int len = b.getInt();
        if (len < 0 || len > b.remaining()) {
            throw new CorrupcaoException("registo " + idx + ": campo " + campo + " com tamanho inválido ("
                    + len + ", restam " + b.remaining() + ")");
        }
        byte[] bytes = new byte[len];
        b.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
