package tpf;

/**
 * Estado de uma réplica visto pelo líder (E2-03, objetivo 5).
 *
 * A distinção entre SUSPEITA e MORTA é deliberada: uma réplica que reinicia
 * ou perde a rede durante alguns segundos não deve desencadear a cópia de
 * todos os seus chunks. Só as réplicas MORTA são excluídas da colocação.
 */
public enum Estado {
    VIVA,
    SUSPEITA,
    MORTA
}
