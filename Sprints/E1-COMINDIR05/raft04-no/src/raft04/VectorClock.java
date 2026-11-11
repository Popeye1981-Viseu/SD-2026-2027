package raft04;

import java.util.Arrays;

/**
 * VectorClock = relógio vetorial com N componentes, um por partição.
 *
 * A componente k conta quantos eventos o líder da partição k já produziu "e que eu conheço".
 * Serve para saber a ORDEM CAUSAL entre registos de partições diferentes:
 *   BEFORE     - um aconteceu antes do outro (há uma cadeia causal entre eles);
 *   AFTER      - ao contrário;
 *   EQUAL      - vetores idênticos;
 *   CONCURRENT - nenhum dos dois influenciou o outro (não há ordem causal).
 */
public final class VectorClock {
    public enum Order { BEFORE, AFTER, EQUAL, CONCURRENT }

    private final long[] v;   // as N componentes

    // vetor novo, tudo a zeros
    public VectorClock(int n) {
        this.v = new long[n];
    }

    // construtor interno, usado por copy() e parse()
    private VectorClock(long[] v) {
        this.v = v;
    }

    // máximo componente a componente com outro vetor
    // (regra de receção: "fico a saber tudo o que o outro sabia")
    public void merge(VectorClock other) {
        for (int i = 0; i < v.length; i++) {
            v[i] = Math.max(v[i], other.v[i]);
        }
    }

    // evento local no processo k: incrementa só a componente k
    public void tick(int k) {
        v[k]++;
    }

    // valor da componente k
    public long get(int k) {
        return v[k];
    }

    // cópia independente (útil para não alterar o original por engano)
    public VectorClock copy() {
        return new VectorClock(v.clone());
    }

    // relação de this com other
    // - se tem alguma componente MENOR e nenhuma maior -> BEFORE
    // - se tem alguma MAIOR e nenhuma menor            -> AFTER
    // - se tem as duas                                  -> CONCURRENT
    // - se não tem nenhuma                              -> EQUAL
    public Order compare(VectorClock other) {
        boolean menor = false, maior = false;
        for (int i = 0; i < v.length; i++) {
            if (v[i] < other.v[i]) menor = true;
            if (v[i] > other.v[i]) maior = true;
        }
        if (menor && maior) return Order.CONCURRENT;
        if (menor) return Order.BEFORE;
        if (maior) return Order.AFTER;
        return Order.EQUAL;
    }

    // representação "v0,v1,...,vN-1"  (é assim que vai nas mensagens e nos ficheiros)
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < v.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(v[i]);
        }
        return sb.toString();
    }

    // inverso de toString(): "3,0,1" -> vetor [3,0,1]. Valida que tem exatamente n componentes.
    public static VectorClock parse(String s, int n) {
        String[] f = s.trim().split(",");
        if (f.length != n) {
            throw new IllegalArgumentException("Vetor com dimensão " + f.length + ", esperado " + n + ": " + s);
        }
        long[] v = new long[n];
        for (int i = 0; i < n; i++) {
            v[i] = Long.parseLong(f[i]);
        }
        return new VectorClock(v);
    }

    // igualdade por conteúdo (comparar os arrays, não as referências)
    @Override
    public boolean equals(Object o) {
        return o instanceof VectorClock && Arrays.equals(v, ((VectorClock) o).v);
    }

    // coerente com equals()
    @Override
    public int hashCode() {
        return Arrays.hashCode(v);
    }
}
