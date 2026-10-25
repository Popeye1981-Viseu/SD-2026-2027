package rep03;

// Função de partição: decide em que partição fica cada sensor.
// A chave de partição é o identificador do sensor.
public final class Partitioner {
    private final int n;   // número de partições (N)

    public Partitioner(int n) {
        // tem de haver pelo menos uma partição
        if (n < 1) {
            throw new IllegalArgumentException("Número de partições inválido: " + n);
        }
        this.n = n;
    }

    // partição de um sensor: determinística e igual em qualquer JVM.
    // String.hashCode() tem o resultado fixado pela especificação do Java,
    // por isso o produtor e o líder, em processos diferentes, chegam ao mesmo valor.
    // floorMod (e não %) garante um resultado entre 0 e n-1 mesmo com hashCode negativo.
    public int of(String sensorId) {
        return Math.floorMod(sensorId.hashCode(), n);
    }

    // número de partições
    public int count() {
        return n;
    }
}
