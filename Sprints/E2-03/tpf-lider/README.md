# tpf-lider · Sprint E2-03 (hub, réplicas, colocação e catálogo)

Bot Discord (JDA) que implementa o **líder do arquivo distribuído**: cada `/registar`,
cada mensagem em `#arquivo` e cada registo de `/rajada` recebe um LSN e é gravado em
`registos.log` com `fsync` **antes** de o utilizador receber a resposta; o chunk aberto é
selado, cifrado e escrito em `selados/`, colocado em **R = 3** réplicas por *rendezvous
hashing* ponderado pelo espaço livre e passa a `DURAVEL` quando os `ACK` válidos cumprem
`max(W_MIN, ⌊R/2⌋+1)`.

O líder é também o **hub de publish/subscribe sobre WebSocket**: as réplicas registam-se
com um token de uso único, autenticam-se por desafio-resposta Ed25519, subscrevem o seu
tópico privado e `tpf-ctl`, publicam heartbeats e `ACK` em `tpf-lider`, e o líder publica
`STORE` em `tpf-r-<id>`.


## Estado do sprint

Implementado:

- `Wire` com `CHUNK_MAX_BYTES` (65536), `PAD_BYTES` (4096) e `SELAGEM_MS` (5000),
  configuráveis por `-Dtpf.*`;
- `master.key` com 32 bytes de `SecureRandom` e permissões de leitura só para o dono,
  criada no primeiro arranque; o arranque é recusado se a chave existir com tamanho errado;
- `Registo` com `lsn`, `instante`, `autor`, `canal` e `texto`;
- `/registar <texto>` e as mensagens em `#arquivo` a passarem pelo **mesmo** método
  sincronizado `registar(autor, canal, texto)`;
- atribuição do LSN seguinte e append da linha `lsn;instante;autor;canal;texto(Base64)`
  a `registos.log`, com `FileChannel.force(true)` antes da resposta;
- recuperação de `registos.log` no arranque e continuação da numeração a partir do último LSN;
- **reparação retomável** da cauda parcial, com ficheiro de intenção `reparacao.intent`,
  **validação estrita do prefixo do log** (estrutura, continuidade dos LSN, impressão digital
  SHA-256) e **conferência do tamanho da cauda** (`fimValido + descartados`);
- **evento `REPARACAO` emitido no máximo uma vez**, com deduplicação por campos (não por
  substring);
- **diário de eventos validado antes de ser alterado**: uma linha terminada por `\n` com
  formato inválido aborta o arranque e o ficheiro fica intacto;
- **decisão explícita sobre cauda com Base64 inválido** (truncar vs abortar, §5.1.1), fixada
  por testes;
- **fail-stop**: se uma escrita falhar e a reposição do ficheiro não puder ser tornada
  durável, a instância bloqueia e recusa novas escritas até o líder ser reiniciado;
- **chunk aberto e selagem** por `CHUNK_MAX_BYTES` ou por `SELAGEM_MS` (temporizador
  `ScheduledExecutorService`), com serialização binária, **padding até múltiplo de `PAD_BYTES`**
  e **leitura inversa** que ignora e valida o padding (§5.4.1);
- **cifra por chunk**: `salt` (32 bytes) e `nonce` (12 bytes) aleatórios, `DEK = HKDF-SHA256(MK,
  salt, "tpf-chunk-v1")`, AES-256-GCM com tag de 128 bits e `aad = "tpf-v1" || keyVersion ||
  salt || nonce`; `chunkId` = SHA-256 do texto cifrado (§5.4.2);
- **`selados/<chunkId>.bin` + `selados/<chunkId>.hdr`**, com o nome do `.bin` verificável
  (SHA-256 do seu conteúdo) e sem texto em claro (§5.4.3);
- `eventos-lider.txt` com os eventos `REGISTO`, `SELADO`, `REPARACAO` e os de arranque;
- `/rajada <n> <intervalo_ms>` a injetar `n` registos sintéticos pelo mesmo caminho;
- `/estado` passou a mostrar o último LSN, o número de chunks selados e a ocupação do chunk
  aberto;
- **arranque em duas fases**: valida diário de eventos, intenção e log por inteiro antes de
  alterar o que quer que seja; só depois corta caudas, grava a intenção e emite `REPARACAO`
  (§5.2.4);
- **`/registar` preserva o texto submetido** (sem `trim`); só valores que são *tokens* — o
  `<id>` de `/replica-remover` — são aparados (§7);
- **falha do log ≠ falha do chunk**: log falhado ⇒ o pedido não aconteceu (mensagem di-lo e
  repetir é seguro); chunk falhado ⇒ registo durável, resposta com o LSN, falha apenas em
  `ERRO` (§9).

E2-03 (este sprint):

- `Wire` com `PORTO` (8765), `PERIODO_HB_MS` (2000), `SUSPEITA_MS` (6000), `MORTA_MS` (10000),
  `TOKEN_VALIDADE_MS` (600000), `R` (3), `W_MIN` (2), `ACK_TIMEOUT_MS` (4000), `MAX_RONDAS` (3),
  `MARGEM_BYTES` (65536), `ALFA` (1.0), `BACKOFF_MIN_MS`/`BACKOFF_MAX_MS` (1000/30000), os
  tópicos `tpf-lider`, `tpf-ctl` e `tpf-r-<id>`, e `encode()`/`decode()` para **OLA, DESAFIO,
  PROVA, SUB, PUB, HB, ACK e STORE**. O quórum de durabilidade é calculado
  (`max(W_MIN, ⌊R/2⌋+1)`), nunca uma constante;
- **`PubSubHub`** (Java-WebSocket, porto `tpf.porto`): máquina `NOVA → DESAFIADA →
  AUTENTICADA`, desafio de 32 bytes de `SecureRandom` novo por ligação e descartado após uma
  tentativa, registo com token de uso único válido 10 min (consumido e gravado em
  `replicas.txt` só depois de uma `PROVA` Ed25519 válida), recusa de `id` já registado com
  outra chave, subscrições só em `tpf-r-<o seu id>` e `tpf-ctl`, publicações só em
  `tpf-lider`, e entrega ao líder em processo **com o id autenticado da ligação**;
- **`HeartbeatMonitor`**, **`ReplicaRegistry`** e **`Estado`**: `VIVA`/`SUSPEITA`/`MORTA`,
  quota, `usados`, `reservados`, último HB, `reserveAndChoose` **atómico** (escolha + reserva
  na mesma secção), `confirmarReserva`/`libertar`, eventos `ESTADO <id> <antigo> -> <novo>`;
- **`Placement`**: rendezvous hashing ponderado, `u` dos primeiros 53 bits de
  `SHA-256(chunkId + "|" + replicaId)`, pontuação `livre^ALFA / -ln(u)`, filtro
  `livre >= tamanho + MARGEM_BYTES`, determinística;
- **`Catalog`** (`catalogo.txt`): `chunkId;lsnDe;lsnAte;tamanho;replicas;estado`, estados
  `SELADO`/`DURAVEL`, `adicionar`, `confirmar`, `copias`, `gravarAtomico` (temporário +
  `ATOMIC_MOVE`), carregado no arranque;
- **escrita replicada** no `LeaderBot`: entrada no catálogo como `SELADO`,
  `reserveAndChoose(..., R, {})`, `STORE` em `tpf-r-<id>`, validação de `ACK` (hash, id
  autenticado e STORE prévio), `DURAVEL` quando o quórum é atingido, `ACK_TIMEOUT_MS` liberta
  reservas e dispara nova ronda com as restantes réplicas até `MAX_RONDAS`, continuando mesmo
  depois de `DURAVEL`; eventos `STORE`, `ACK`, `ACK-REJEITADO`, `DURAVEL`, `SUBREPLICADO`,
  `RONDA`;
- **comandos Discord**: `/replica-token` gera um token real numa resposta **efémera** (só
  `admin-arquivo`), `/replica-remover` revoga a chave, desliga a réplica e exclui-a da
  colocação, e `/estado` mostra por réplica estado/ocupação/nº de chunks e no total os chunks
  `SELADO`, `DURAVEL` e subreplicados;
- **projeto `tpf-replica`** (novo): par Ed25519 persistido, cliente `java.net.http.WebSocket`,
  `OLA`/`PROVA`/`SUB`, `HB` e `INVENTARIO`, `STORE` com verificação de quota e hash, escrita
  atómica e `ACK` só depois, backoff exponencial 1 s → 30 s, eventos em `eventos-<id>.txt`.

Ainda **não** implementado (fica para o sprint seguinte): manifesto cifrado, reconciliação
(reparação e remoção de excesso), `DELETE`, desafios de posse (`POSSE?`/`desafios.txt`),
`FETCH`, recuperação a partir das réplicas e `ArquivoCheck`. `/config` e `/recuperar` mantêm a
resposta provisória do sprint anterior.

Requisitos: **Java 17 ou superior** e **Maven**.

## 1. Configurar o Discord (uma vez)

1. Em <https://discord.com/developers/applications>, crie a aplicação (**New Application**) e
   abra o separador **Bot**.
2. Em **Privileged Gateway Intents**, ative **Message Content Intent** e guarde.
3. Carregue em **Reset Token** e copie o token. **Não o ponha no código nem no repositório.**
4. Crie o servidor de teste do grupo, o canal `#arquivo` e o papel `admin-arquivo`.
   Atribua o papel aos membros do grupo.
5. Convide o bot. Substitua `<APPLICATION_ID>` pelo id que aparece em **General Information**:

   ```
   https://discord.com/oauth2/authorize?client_id=<APPLICATION_ID>&scope=bot%20applications.commands&permissions=68608
   ```

   `68608` corresponde a **View Channels**, **Send Messages** e **Read Message History**.
   Esta última é necessária para responder às mensagens de `#arquivo`.
6. Copie o id do servidor: em **Definições do utilizador → Avançado**, ative o
   **Modo de programador**, clique com o botão direito no servidor e escolha
   **Copiar ID do servidor**.

## 2. Configurar o token e a chave mestra

Escolha uma das opções para o token:

- **Variável de ambiente** (recomendado):

  ```bash
  export DISCORD_TOKEN='o-token-do-bot'
  ```

- **Ficheiro** `discord.token` na pasta onde arranca o líder, com o token numa só linha.
  O ficheiro está no `.gitignore`. Para usar outro caminho, passe
  `-Dtpf.tokenFile=<caminho>`.

A `master.key` é criada automaticamente no primeiro arranque (em `master.key`, ou no caminho
indicado por `-Dtpf.masterKey`), com 32 bytes aleatórios e permissões `rw-------`.
**Não a apague:** sem ela não é possível decifrar os chunks. Se existir mas não tiver 32 bytes,
o líder recusa arrancar e **não** altera o ficheiro — a decisão é do administrador.

**A `master.key` nunca é regenerada por cima de dados existentes.** Se `registos.log`,
`eventos-lider.txt` ou `selados/` já existirem e a `master.key` faltar — apagada, caminho de
`-Dtpf.masterKey` errado, mudança de máquina — o líder **recusa arrancar** com um diagnóstico
que explica o risco: uma chave nova cifraria os chunks seguintes com outra chave e tornaria os
anteriores indecifráveis, deixando o arquivo a parecer íntegro e a não o ser. As saídas são
repor a chave a partir de uma cópia de segurança ou assumir a rotura com
`-Dtpf.novaChave=true`. A verificação é feita **antes** de chamar o `Wire`, portanto a chave só
chega a ser criada quando não há nada a perder.

## 3. Arrancar o líder

```bash
cd tpf-lider
mvn -q compile exec:java -Dtpf.guild=<ID_DO_SERVIDOR>
```

Em alternativa, gere um jar executável:

```bash
mvn -q package
java -Dtpf.guild=<ID_DO_SERVIDOR> -jar target/tpf-lider-1.0.jar
```

O id do servidor também pode vir da variável `TPF_GUILD_ID`.

Na consola, e em `dados-lider/eventos-lider.txt`, aparecem:

- `ARRANQUE` — `master.key` validada, `registos.log` recuperado (com o último LSN),
  catálogo carregado e hub à escuta no porto;
- `REPARACAO` — só se havia uma cauda parcial a reparar (ou uma reparação pendente);
- `REGISTO` — um por escrita durável, com o LSN, autor, canal e tamanho da linha;
- `SELADO` — um por chunk selado, com `chunkId`, intervalo de LSN e tamanho em claro;
- `RONDA`, `STORE`, `ACK`, `ACK-REJEITADO`, `DURAVEL`, `SUBREPLICADO`, `ESTADO` — a
  colocação e a durabilidade do E2-03 (§10.6);
- `COMANDOS` — `7 comandos registados em <servidor>`;
- `ARRANQUE` — líder online.

Os comandos são registados no servidor (*guild commands*), por isso aparecem de imediato no
autocomplete.

### 3.1. Propriedades do sistema

| Propriedade | Omissão | Significado |
| --- | --- | --- |
| `tpf.guild` | `TPF_GUILD_ID` | id do servidor Discord |
| `tpf.tokenFile` | `discord.token` | ficheiro com o token (se não houver `DISCORD_TOKEN`) |
| `tpf.masterKey` | `master.key` | caminho da chave mestra |
| `tpf.pasta` | `dados-lider` | pasta de dados do líder (`registos.log`, `eventos-lider.txt`, `reparacao.intent`, `selados/`) |
| `tpf.chunkMax` | `65536` | `CHUNK_MAX_BYTES`: limite em claro que provoca a selagem do chunk |
| `tpf.pad` | `4096` | `PAD_BYTES`: o texto do chunk é completado até um múltiplo deste valor |
| `tpf.selagem` | `5000` | `SELAGEM_MS`: tempo máximo (ms) que um chunk fica aberto |
| `tpf.novaChave` | `false` | aceita criar uma `master.key` nova mesmo existindo dados (perde a capacidade de decifrar os chunks antigos) |
| `tpf.porto` | `8765` | porto do hub WebSocket (`PORTO`) |
| `tpf.periodo` | `2000` | `PERIODO_HB_MS`: intervalo entre heartbeats das réplicas |
| `tpf.suspeita` | `6000` | `SUSPEITA_MS`: silêncio que faz `VIVA → SUSPEITA` (3 heartbeats) |
| `tpf.morta` | `10000` | `MORTA_MS`: silêncio que faz `→ MORTA` (5 heartbeats) |
| `tpf.tokenValidade` | `600000` | `TOKEN_VALIDADE_MS`: validade de um token de registo (10 min) |
| `tpf.r` | `3` | `R`: fator de replicação |
| `tpf.w` | `2` | `W_MIN`: mínimo absoluto de `ACK` para `DURAVEL` |
| `tpf.ackTimeout` | `4000` | `ACK_TIMEOUT_MS`: prazo de uma ronda de `STORE` |
| `tpf.maxRondas` | `3` | `MAX_RONDAS`: tentativas de colocação por chunk (a primeira conta) |
| `tpf.margem` | `65536` | `MARGEM_BYTES`: folga exigida em cada réplica para receber um chunk |
| `tpf.alfa` | `1.0` | `ALFA`: expoente da ponderação pelo espaço livre |
| `tpf.backoffMin` / `tpf.backoffMax` | `1000` / `30000` | `BACKOFF_MIN_MS` / `BACKOFF_MAX_MS` (usadas pela réplica) |

Exemplo com tudo explícito:

```bash
java -Dtpf.guild=123456789012345678 \
     -Dtpf.masterKey=master.key \
     -Dtpf.pasta=. \
     -Dtpf.chunkMax=65536 -Dtpf.pad=4096 -Dtpf.selagem=5000 \
     -Dtpf.porto=8765 -Dtpf.periodo=2000 -Dtpf.suspeita=6000 -Dtpf.morta=10000 \
     -Dtpf.tokenValidade=600000 -Dtpf.r=3 -Dtpf.w=2 -Dtpf.ackTimeout=4000 \
     -Dtpf.maxRondas=3 -Dtpf.margem=65536 -Dtpf.alfa=1.0 \
     -jar target/tpf-lider-1.0.jar
```

## 4. Ficheiros da pasta do líder

```
dados-lider/
├── registos.log        # diário de registos: fonte de verdade (texto, 1 linha por registo)
├── eventos-lider.txt   # diário de eventos do líder
├── reparacao.intent    # só existe DURANTE uma reparação (ver §5.2); se a encontrar antes
│                       # de arrancar, houve um crash a meio — o arranque conclui-a
├── selados/            # chunks cifrados: <chunkId>.bin + <chunkId>.hdr (§5.4)
├── catalogo.txt        # onde está cada chunk: chunkId;lsnDe;lsnAte;tamanho;replicas;estado
└── replicas.txt        # réplicas registadas: id;chavePublicaB64 (uma por linha)
```

O caminho é relativo à pasta onde o líder arranca. Para os ficheiros ficarem na raiz do
projeto, use `-Dtpf.pasta=.`.

## 5. Formato de `registos.log`

Uma linha por registo, em UTF-8, terminada por `\n`:

```
lsn;instante;autor;canal;texto-em-Base64\n
```

| Campo | Exemplo | Notas |
| --- | --- | --- |
| `lsn` | `42` | inteiro ≥ 1, sequencial a partir de 1, sem repetições nem saltos |
| `instante` | `2025-01-31T14:03:11.482Z` | ISO-8601 em UTC (`Instant.toString()`) |
| `autor` | `ana` | nome do utilizador no Discord, sanitizado na escrita |
| `canal` | `arquivo` | canal de origem |
| `texto` | `b2zDoSB0dWRvIGJlbQ==` | texto original em Base64 (suporta `;`, `\n`, emoji) |

Regras do formato:

- O `\n` final é o **marcador de commit** da linha: só conta como registo a linha terminada.
- O **texto** nunca é sanitizado: vai em Base64, por isso `;` e mudanças de linha não quebram
  o formato e o que se lê de volta é exatamente o que foi escrito.
- `autor` e `canal` **são** sanitizados (`;`, CR, LF e caracteres de controlo passam a `_`),
  para que um nome de utilizador com `;` não produza uma linha com 6 campos — ou seja, para
  que o escritor nunca corrompa o próprio log.
- Cada linha é escrita numa única `write()` e seguida de `force(true)` antes da resposta ao
  utilizador, dentro de um método `synchronized`.

### 5.1. Regra de recuperação no arranque

Assunção documentada: o append local tem **semântica de prefixo** — o conteúdo em disco é
sempre um prefixo da sequência de bytes já escrita. Como a linha só é respondida depois do
`force(true)`, um crash só pode deixar lixo **na cauda** do ficheiro.

| Situação | Interpretação | Ação |
| --- | --- | --- |
| cauda **sem** `\n` | escrita parcial interrompida por crash | reparação com intenção: truncar até ao último `\n`, `force(true)`, evento `REPARACAO` |
| linha **com** `\n` mas inválida (campos, LSN, instante, Base64) | corrupção (alteração externa, bit-rot, cópia do ficheiro) | **abortar o arranque** com diagnóstico, sem alterar o ficheiro |
| linha inválida **fora** da cauda | corrupção (uma escrita parcial nunca fica no meio) | **abortar o arranque** |
| LSN fora de sequência (`n` ≠ anterior + 1, ou primeira linha ≠ 1) | registo perdido ou duplicado | **abortar o arranque** |

Uma linha só é aceite se cumprir **três** condições: estrutura (5 campos, `lsn` ≥ 1, instante
parseável, Base64 estrito) **+** terminador `\n` **+** continuidade do LSN.

Justificações:

- **Porque não se trunca uma linha terminada mas inválida.** Se o `\n` chegou ao disco, tudo o
  que está antes também chegou: não é escrita parcial. Essa linha pode corresponder a um LSN
  **já confirmado** ao utilizador. Truncá-la em silêncio criaria um salto (perda de um registo
  confirmado) ou, pior, convidaria à reutilização do LSN. É por isso que o arranque aborta e
  exige decisão do administrador.
- **Porque não se tenta "validar a cauda".** Um prefixo pode analisar bem sem ser o registo
  original: um Base64 truncado (`QUJD` → `QUJ`) descodifica sem erro, e um texto vazio produz
  uma linha com 5 campos válidos. Só o terminador é um critério fiável.
- **Porque a reparação também faz `force(true)`.** Sem isso, um segundo crash voltaria a
  encontrar o mesmo lixo na cauda.
- **Porque nenhum LSN confirmado é perdido.** A limpeza só toca em bytes que nunca foram
  respondidos; a numeração é retomada em `último + 1`.

Diagnóstico em caso de corrupção (exemplo):

```
ERRO FATAL NO ARRANQUE: registos.log inválido: Linha 3: Base64 inválido no 5.º campo
[offset 118, linha 3]. A linha está terminada por '\n', portanto não pode ser uma escrita
parcial: é corrupção. Arranque abortado para não haver repetições nem saltos de LSN.
```

### 5.1.1. Base64 inválido na cauda: porque se trunca, e quando não se trunca

Esta decisão está registada explicitamente porque é contraintuitiva à primeira vista: uma linha
com Base64 inválido pode ser **descartada** (se estiver na cauda, sem `\n`) ou pode **abortar o
arranque** (se estiver terminada). A diferença não é o Base64 — é o terminador.

| Terminador `\n` | Conteúdo | Classificação | Ação | Evento |
| --- | --- | --- | --- | --- |
| **não** | Base64 inválido | escrita parcial interrompida por crash | **truncar** até ao último `\n` | `REPARACAO` |
| **não** | Base64 válido (por coincidência) | idem — o terminador é o único marcador de commit | **truncar** | `REPARACAO` |
| **sim** | Base64 inválido | linha commitada e ilegível → corrupção | **abortar** o arranque | — |
| **sim** | Base64 válido | registo normal | aceitar | — |

**Porque a cauda sem `\n` é truncada, mesmo com Base64 inválido.** Uma escrita interrompida
deixa, por construção, um **prefixo de linha**: `5;2025-;ana;arquivo;b3Jp` — cinco campos, LSN
parsável, Base64 a meio. Isso é exatamente o resultado esperado de um crash no meio do
`write()`, e é o caso **mais frequente** de todos. Abortar aqui tornaria o arranque impossível
precisamente na situação que a recuperação existe para resolver, e obrigaria a intervenção
manual em cada crash — a recuperação deixaria de existir na prática.

**Porque não se "tenta validar" a cauda em vez de a descartar.** Porque o Base64 é *tolerante*:
um prefixo truncado pode descodificar sem erro (`QUJD` → `ABC`; `QUJ` → `AB`) e produzir um
texto diferente do original, com os 5 campos presentes e o LSN certo. Aceitar "a cauda que
analisa bem" **inventaria um registo** com conteúdo errado, e esse registo ficaria durável e
numerado — pior do que descartar. O Base64 sintaticamente válido numa cauda não prova nada
sobre a integridade; só o `\n` prova o commit.

**Porque a cauda truncada é segura de descartar.** A garantia é `force(true)` **antes** da
resposta: se a linha não tem `\n` em disco, o `force` não concluiu, logo o LSN **nunca foi
respondido** ao utilizador. Nada de confirmado se perde, e a numeração é retomada em
`último + 1` — sem repetição nem salto. O evento `REPARACAO` registra quantos bytes foram
descartados, para que a operação não seja silenciosa.

**Porque a linha terminada com Base64 inválido aborta em vez de ser truncada.** Um `\n` em
disco implica, pela semântica de prefixo, que **tudo o que está antes também está** — logo a
linha foi escrita por inteiro e pode ter sido confirmada ao utilizador. Truncá-la produziria um
salto na numeração (perda de um registo confirmado) e, pior, convidaria à reutilização desse
LSN. Trata-se como corrupção (alteração externa, bit-rot, restauro de cópia), que exige decisão
humana.

**Cada linha da matriz está fixada por um teste**, para que a decisão não regresse:

| Cenário | Teste |
| --- | --- |
| cauda sem `\n`, conteúdo inválido → truncar | `caudaSemTerminadorEhTruncadaENaoRepeteLsn` |
| cauda sem `\n` que **analisa bem** → truncar na mesma | `caudaQueAindaAnalisaBemMasSemTerminadorEhDescartada` |
| linha terminada com Base64 inválido → abortar | `caudaComTerminadorMasBase64InvalidoAbortaArranque` |
| linha inválida no meio (terminada) → abortar | `linhaInvalidaNoMeioAbortaArranque` |

Resumo numa frase: **o Base64 não é o critério; o `\n` é.** Sem terminador, o conteúdo é
irrelevante e descarta-se (nunca foi confirmado); com terminador, o conteúdo tem de ser válido
e, se não for, é corrupção.

### 5.2. Reparação com intenção durável (`reparacao.intent`)

Truncar a cauda envolve duas coisas separadas: (a) cortar bytes e (b) deixar evidência em
`eventos-lider.txt`. Um crash entre as duas deixaria o log reparado **sem** o evento, ou o
evento duplicado num arranque seguinte. Para evitar ambos, a reparação é **antecipadamente
registada** num ficheiro de intenção, que descreve não só o comprimento final do log mas também
**qual conteúdo** é suposto existir até lá, e **quantos bytes** de cauda há a descartar.

Formato de `reparacao.intent` (UTF-8, `chave=valor`, uma por linha):

```
tpf-reparacao-v1
intentId=6f1c3a9e-0b52-4f77-9e0e-2b1f3f1d8c44
fimValido=118
descartados=37
ultimoLsn=1
prefixoSha256=9c1f0a3d5b7e28a4c6d0f213a8b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6
```

| Campo | Obrigatório | Significado |
| --- | --- | --- |
| `intentId` | sim | identificador único da reparação; aparece no evento `REPARACAO` como `intent=<id>` |
| `fimValido` | sim | offset do fim do último registo válido = tamanho final de `registos.log` |
| `descartados` | sim | bytes de cauda parcial a descartar |
| `ultimoLsn` | sim | último LSN válido antes da reparação |
| `prefixoSha256` | não | SHA-256 (64 dígitos hex, minúsculas) dos bytes `[0, fimValido)` |

O ficheiro é escrito com `force(true)` **antes** de se tocar no log: a intenção tem de
sobreviver exatamente ao crash que a reparação seguinte vai encontrar.

**Invariante central.** No momento em que a intenção foi gravada, `registos.log` tinha
exatamente `fimValido + descartados` bytes. Como nada é escrito entre gravar a intenção e
truncar (o canal de *append* só abre depois da recuperação), qualquer arranque posterior
encontra o log num de três estados — e a validação distingue-os explicitamente (§5.2.3).

`prefixoSha256` é **opcional** para não invalidar intenções gravadas por versões anteriores do
líder. Quando está presente, é a validação mais forte do prefixo e é sempre escrita pelo
código atual; quando falta, aplicam-se apenas as validações estruturais (P1–P5). Se o campo
existir mas não for 64 dígitos hex, a intenção é ilegível (§5.2.5).

Sequência completa de uma reparação:

1. gravar `reparacao.intent` com `prefixoSha256` do prefixo que se diz válido (`force(true)` no
   ficheiro e `fsync` ao diretório);
2. **validar** `registos.log` contra a intenção — propriedades P1–P7 do quadro seguinte;
3. truncar `registos.log` em `fimValido` (`force(true)`), se ainda for preciso;
4. emitir `REPARACAO … intent=<id>` em `eventos-lider.txt` (`force(true)`);
5. apagar `reparacao.intent` (`fsync` ao diretório).

Nenhum passo destrutivo (3), nenhuma evidência (4) e nenhuma remoção de evidência (5)
acontecem antes de o passo 2 passar por inteiro.

#### 5.2.1. Que propriedades confirmam que o prefixo termina em `fimValido` e é `ultimoLsn`

Validar só `tamanho ≥ fimValido` e a ausência de `\n` depois de `fimValido` responderia à
pergunta errada: diz que **nada foi escrito a mais**, não que **o que está escrito é o que a
intenção descreve**. A validação verifica, do mais fraco para o mais forte:

| # | Propriedade exigida | O que exclui | Falha |
| --- | --- | --- | --- |
| **P1** | `Files.size(registos.log) ≥ fimValido`, e se o ficheiro não existir então `fimValido == 0` **e** `ultimoLsn == 0` | log truncado externamente, volume desmontado, ficheiro movido | aborta |
| **P2** | `fimValido == 0` **ou** o byte em `fimValido-1` é `\n` | `fimValido` a cair **a meio** de um registo — a truncagem inventaria uma fronteira | aborta |
| **P3** | os `fimValido` bytes são **integralmente consumidos** por linhas completas (nenhum byte fora de linha) | bytes órfãos, terminador em falta no interior do prefixo | aborta |
| **P4** | cada linha do prefixo é estruturalmente válida (5 campos, `lsn` ≥ 1, instante parseável, Base64 **estrito**) | lixo terminado por `\n` dentro do prefixo | aborta |
| **P5** | os LSN do prefixo são `1..N` **sem lacunas** e `N == ultimoLsn` | prefixo estruturalmente válido mas de **outro estado** do log (cópia restaurada, registo perdido, intenção de outra execução) | aborta |
| **P6** | `SHA-256(prefixo) == prefixoSha256`, quando a intenção traz o campo | conteúdo **alterado que continua a descodificar** (bit-flip dentro de um campo Base64: P2–P5 passariam) | aborta |
| **P7** | se `tamanho > fimValido`, a cauda tem **exatamente** `descartados` bytes **e** nenhum `\n`; se `tamanho == fimValido`, a truncagem já terminou | cauda com tamanho diferente do registado (bytes perdidos, ficheiro substituído, intenção de outro arranque) e cauda com uma linha completa (registos confirmados para lá de `fimValido`) — emitir `REPARACAO` com `descartados` seria **reportar uma reparação que não aconteceu** | aborta |

Porque é que P6 é necessária e não basta P2–P5: o Base64 é tolerante a muitos bits alternados,
pelo que um byte alterado no meio de um registo pode continuar a produzir texto válido, manter
os 5 campos, manter o LSN e manter a continuidade. O prefixo passaria então P1–P5 e a
reparação emitiria `REPARACAO` e apagaria a intenção, deixando a corrupção para o varrimento
seguinte. P6 é a única propriedade que liga a intenção ao **conteúdo**, e não só à forma e ao
comprimento.

Porque é que P7 é necessária e não bastam P1–P6: sem ela, `descartados` seria apenas um número
para o log de eventos, e não uma **asserção verificável sobre o disco**. Com P7, uma intenção
que diz 37 bytes e um log com 5 bytes de cauda deixam de ser "compatíveis": o estado não é o
descrito, e o arranque para em vez de reportar uma reparação fictícia. P7 é **conjuntiva** — o
tamanho e o terminador são verificados os dois, e a varredura de terminadores corre primeiro,
porque um tamanho coincidente não torna a operação segura, apenas mais enganadora.

Custo: um `SHA-256` do prefixo, **uma vez por arranque com intenção pendente**. No caminho
normal (sem `reparacao.intent`) continua a existir um único varrimento do ficheiro.

Os bytes do prefixo são lidos para memória para poderem ser varridos e resumidos; existe um
limite de `Integer.MAX_VALUE - 8` bytes (≈2 GiB) para essa validação. Um `fimValido` acima do
limite aborta com diagnóstico em vez de arriscar um `OutOfMemoryError`. Num log de tamanho
realista deste sprint (chunks de 64 KiB) o caso não ocorre.

#### 5.2.2. Retoma em qualquer ponto de crash

Como cada passo é idempotente, o arranque retoma a reparação em qualquer ponto de falha:

| Crash acontece… | No arranque seguinte | Eventos `REPARACAO` |
| --- | --- | --- |
| antes de gravar a intenção | o log ainda tem a cauda; o varrimento cria uma intenção nova (com hash) e repara | 1 |
| depois da intenção, antes de validar | a intenção existe e a cauda também → valida (P1–P7) e trunca | 1 |
| depois de validar, antes de truncar | idem | 1 |
| depois de truncar, antes do evento | `tamanho == fimValido` → valida o prefixo e emite o evento em falta | 1 |
| depois do evento, antes de apagar a intenção | a intenção existe e a evidência também → **não** repete, apaga a intenção | 1 |
| depois de apagar | nada pendente, nada a fazer | 1 |

A deduplicação é feita **por campos** (§5.2.4), pelo que o evento `REPARACAO` de uma dada
reparação aparece **no máximo uma vez**, mesmo com reinícios sucessivos.

#### 5.2.3. Os três estados possíveis de `registos.log` perante uma intenção pendente

Como `fimValido + descartados` é o tamanho que o log tinha quando a intenção foi gravada, e
como nada é escrito entre gravar a intenção e truncar, só há **três** leituras possíveis do
tamanho atual:

| `tamanho` atual | Interpretação | Ação |
| --- | --- | --- |
| `== fimValido` | a truncagem **já terminou** num arranque anterior (crash entre truncar e emitir) | concluir: validar o prefixo e **emitir só a evidência** em falta |
| `== fimValido + descartados` | a cauda **ainda está presente**, exatamente como descrita | validar, **truncar** em `fimValido` e emitir a evidência |
| qualquer outro valor `> fimValido` | estado **não descrito** por esta intenção | **abortar** — nem tudo está reparado, nem a cauda é a que a intenção registou |

O terceiro caso é o que P7 fecha. Exemplos concretos que caem no terceiro caso: cauda mais curta
que `descartados` (bytes perdidos por cópia parcial, `dd` interrompido, ficheiro substituído),
cauda mais longa que `descartados` (intenção antiga de outro arranque, ou registos completos
escritos depois), `descartados == 0` com cauda presente (intenção incoerente).

Além do tamanho, há um conjunto de estados que também abortam por incompatibilidade de
conteúdo:

| Estado encontrado com intenção pendente | Regra violada | Porque aborta |
| --- | --- | --- |
| `fimValido > 0` e `registos.log` não existe | P1 | estado impossível (volume desmontado? ficheiro movido?) |
| `fimValido == 0` mas `ultimoLsn > 0` | P1 | intenção incoerente: não há bytes válidos, logo não há LSN |
| `tamanho < fimValido` | P1 | o log ficou mais curto do que a intenção dizia; concluir exigiria inventar conteúdo |
| byte em `fimValido-1` ≠ `\n` | P2 | a truncagem cortaria um registo a meio e inventaria uma fronteira |
| sobram bytes fora de linhas completas | P3 | o prefixo não é uma sequência de linhas |
| linha inválida dentro do prefixo | P4 | corrupção já materializada antes da fronteira |
| LSN do prefixo ≠ `ultimoLsn` (ou com lacunas) | P5 | o prefixo é de **outro** estado do log |
| `SHA-256` do prefixo ≠ `prefixoSha256` | P6 | conteúdo alterado que continua a parecer válido |
| cauda com tamanho ≠ `descartados` | P7 | o estado não corresponde ao descrito pela intenção |
| existe `\n` em `[fimValido, tamanho)` | P7 / regra da cauda | há registos **completos** para lá de `fimValido`: a intenção é antiga e truncar apagaria registos possivelmente já confirmados |

O caso "log cresceu" merece destaque: uma intenção antiga (por exemplo, porque o apagar do
ficheiro falhou) com o líder a ter voltado a escrever registos válidos **não** pode ser
resolvida por truncagem — isso apagaria registos confirmados. O arranque aborta com um
diagnóstico explícito, para que um humano decida. É a mesma política da §5.3: **perante
ambiguidade, parar em vez de adivinhar**.

Diagnósticos (exemplos):

```
ERRO FATAL NO ARRANQUE: registos.log inválido: Reparação pendente (intent=6f1c3a9e-…)
incompatível com …/dados-lider/registos.log: a cauda tem 5 bytes sem terminador, mas a
intenção registou descartados=37 (o log devia ter 155 bytes e tem 123). O estado não
corresponde ao descrito — nem já reparado (tamanho == fimValido), nem por reparar
(tamanho == fimValido + descartados). Nada foi alterado — nem o log, nem a intenção,
nem o diário de eventos. Arranque abortado para inspeção manual.
```

```
ERRO FATAL NO ARRANQUE: registos.log inválido: Reparação pendente (intent=6f1c3a9e-…)
incompatível com …/dados-lider/registos.log: já tem uma linha terminada por '\n' depois
de fimValido=118 (tamanho 240); a intenção é anterior a registos completos, e truncar
apagaria registos possivelmente confirmados.
```

#### 5.2.4. Diário de eventos: validar antes de alterar, e deduplicar por campos

`eventos-lider.txt` é a evidência da reparação, por isso é tratado com as mesmas regras do log.

**Princípio: validar antes de alterar — em todo o arranque.** O arranque tem duas fases
separadas. Na **fase de validação** nada é escrito: validam-se as linhas completas de
`eventos-lider.txt`, lê-se `reparacao.intent`, verifica-se a compatibilidade da intenção com o
log (P1–P7) e varre-se `registos.log`. Só na **fase de escrita** se corta a cauda parcial do
diário, se grava a intenção, se trunca o log e se emite `REPARACAO`. Consequência prática: uma
intenção incompatível ou um log corrompido abortam com os **três** ficheiros byte-a-byte
intactos — incluindo a cauda parcial do diário, que de outro modo seria o primeiro alvo de
escrita do arranque.

**Validação.** Cada linha terminada por `\n` tem de ser `instante;evento;detalhe` (3 campos,
`split(";", 3)`), com instante ISO-8601 parseável e `evento` não vazio. Uma linha terminada
**com formato inválido** — menos de 3 campos, instante vazio ou não parseável, evento vazio —
**aborta o arranque** e o diário é preservado:

```
ERRO FATAL NO ARRANQUE: registos.log inválido: eventos-lider.txt, linha 2: esperava
instante;evento;detalhe (3 campos), encontrei 1. A linha está terminada por '\n', portanto
não é uma escrita parcial: é corrupção. Arranque abortado e diário preservado.
```

A cauda **sem** `\n` continua a ser tratada como escrita parcial (é cortada, com `force(true)`),
tal como no log — é o único caso em que se altera o diário, e só depois de as linhas completas
terem passado a validação.

**Deduplicação.** Uma linha só conta como prova de que a reparação já foi registada se cumprir
**três** condições:

1. a linha é válida (`≥ 3` campos);
2. o campo `evento` é **exatamente** `REPARACAO`;
3. o campo `detalhe` contém um sub-campo (separado por `;`) **exatamente igual** a
   `intent=<intentId>`.

Procurar `intent=<id>` como substring em qualquer linha seria demasiado permissivo em dois
sentidos: uma linha `ARRANQUE` ou `REGISTO` que mencione o id suprimiria a evidência de
reparação, e `intent=abc` casaria com `intent=abc-outro`. Com a comparação por campos, cada
reparação emite a **sua** linha.

#### 5.2.5. Intenção ilegível

Se `reparacao.intent` existir mas estiver malformado — cabeçalho diferente de
`tpf-reparacao-v1`, campo obrigatório em falta, valor não numérico, `prefixoSha256` que não
seja 64 dígitos hex, linha sem `chave=valor`, ou bytes que não são UTF-8 válido — o arranque
**aborta** e o ficheiro **não é apagado**:

```
ERRO FATAL NO ARRANQUE: registos.log inválido: Ficheiro de intenção ilegível:
…/dados-lider/reparacao.intent (cabeçalho ausente ou desconhecido). O arranque foi abortado
e nada foi alterado: inspecione o ficheiro e, se a reparação já não for necessária,
remova-o manualmente.
```

Apagar em silêncio esconderia a evidência de um crash a meio da escrita da própria intenção (ou
de uma alteração externa) e deixaria o líder prosseguir sobre um estado que ninguém validou.

#### 5.2.6. Pormenores que a implementação garante

- **`eventos-lider.txt` também tem cauda parcial.** O arranque valida as linhas completas do
  diário e só depois corta a cauda incompleta (até ao último `\n`, com `force(true)`). É seguro
  porque o canal de eventos é aberto em modo *append*, logo escreve sempre no fim do ficheiro
  atual, mesmo que este tenha sido truncado entretanto.
- **`fsync` ao diretório, não só ao ficheiro.** `force(true)` no canal de `reparacao.intent`
  sincroniza os dados e o *inode*, mas **não** garante que a *entrada de diretório* que torna o
  nome visível esteja durável. Sem essa sincronização, um crash abrupto pode deixar os dados no
  disco e o nome ausente (ou a remoção por aplicar), conforme o sistema de ficheiros e o modo de
  *journaling*. Por isso o código chama uma sincronização do diretório após **criar** e após
  **apagar** a intenção. É *best-effort* e explicada:
  - **Linux/macOS**: `FileChannel.open(dir, READ).force(true)` faz exatamente `fsync` ao
    diretório e devolve sucesso.
  - **Windows**: abrir um diretório como canal falha; a durabilidade da entrada fica dependente
    do sistema de ficheiros (o NTFS regista a criação praticamente de forma síncrona, mas não é
    uma garantia formal).
  - **Em ambos os casos a falha é reportada e não aborta o arranque.** Recusar reparar o log por
    o SO não suportar `fsync` de diretório seria pior do que a janela de risco que resta — e a
    intenção continua a ser o melhor mecanismo disponível.
- **`bytesDescartadosNaRecuperacao()` reporta a reparação concluída *neste* arranque.** Se
  havia uma intenção pendente, o valor são os bytes que ela descreve — mesmo que a truncagem já
  tivesse sido feita antes do crash. Caso contrário, o operador leria "sem reparações" logo a
  seguir a uma reparação ter corrido. Quando há uma intenção pendente **e** uma cauda nova
  descoberta pelo varrimento, os dois valores somam-se.
- **A intenção é sempre escrita com hash** pelo código atual; só intenções antigas (sem o campo)
  são validadas apenas em P1–P5.

### 5.3. Fail-stop: quando o log deixa de aceitar escritas

Durante `acrescentar(...)`, se a escrita ou o `force(true)` falharem, o log tenta repor o
estado anterior com **duas** operações: `truncate` de volta ao offset inicial e `force(true)`
que torna essa reposição durável. A reposição só é considerada boa se **ambas** passarem.

Se qualquer uma delas falhar:

- o `IOException` original é lançado ao chamador, com a falha da reposição em
  `getSuppressed()`;
- a instância fica **bloqueada** (`estaBloqueado() == true`);
- as escritas seguintes falham **antes de tocar no canal**, com uma mensagem que contém
  `Log bloqueado (fail-stop)`;
- `/estado` assinala o estado e `/registar`, `#arquivo` e `/rajada` respondem que é preciso
  reiniciar o líder;
- o ficheiro **não** é reparado "às cegas" — a cauda é tratada no arranque seguinte, com a
  intenção durável e a validação P1–P7 da §5.2.

Três cenários, todos testados:

| Falha | `truncate` | `force` | Resultado |
| --- | --- | --- | --- |
| escrita (linha a meio no ficheiro) | ok | ok | reposição durável: o log **continua** utilizável; a linha seguinte recebe o LSN esperado |
| escrita **ou** `force` inicial | ok | **falha** | **fail-stop**: a truncagem pode ter acontecido, mas sem `force` não se sabe se sobreviveu a um crash |
| escrita **ou** `force` inicial | **falha** | — | **fail-stop**: não se sabe onde acaba o último registo válido |

O estado **não** é `static`: é um campo de instância (`volatile`), o que mantém os testes
independentes e garante que um novo arranque começa sempre desbloqueado.

Diagnóstico em consola (exemplo, impresso de propósito pelos testes de fail-stop):

```
ERRO CRÍTICO: a escrita falhou (falha simulada de escrita) e o rollback também
(falha simulada de truncate).
O log de /…/dados-lider fica BLOQUEADO neste processo: nenhum registo novo será escrito.
Reinicie o líder para reparar a cauda.
```

### 5.4. Formato binário do chunk e do `.hdr`

#### 5.4.1. Texto em claro, antes de cifrar

Tudo em **big-endian**. O chunk é construído em memória, completado com padding e só depois cifrado.

| Offset | Tipo | Campo |
| --- | --- | --- |
| 0 | 6 bytes | `TPFCH1` (assinatura + versão do formato) |
| 6 | int32 | `lenConteudo` — bytes ocupados pelos registos |
| 10 | int32 | `numeroRegistos` |
| 14 | … | registos, por ordem de LSN: `corpo` **`int32 len(corpo)`** |
| 14+lenConteudo | zeros | padding até um múltiplo de `PAD_BYTES` (4096) |

Corpo de um registo:

| Tipo | Campo |
| --- | --- |
| int64 | `lsn` |
| int64 | `instante` em epoch millis (UTC) — precisão de milissegundos |
| int32 + bytes | `autor` em UTF-8 |
| int32 + bytes | `canal` em UTF-8 |
| int32 + bytes | `texto` em UTF-8 |

Notas de desenho:

- **O `int32` de tamanho fica no fim de cada corpo** para tornar a leitura inversa possível:
  a partir do fim do conteúdo, lê-se esse inteiro e recua-se exatamente esse número de bytes.
- **O padding não é distinguível por conteúdo.** São zeros, e um campo de tamanho pode
  legitimamente terminar em `0x00`; por isso o cabeçalho declara `lenConteudo` e o intervalo
  `[14+lenConteudo, total)` é validado como **integralmente nulo**. Um byte diferente de zero é
  corrupção.
- **O tamanho total em claro é sempre múltiplo de `PAD_BYTES`.** `/rajada 500 0` com
  `-Dtpf.chunkMax=4096` produz vários chunks, todos com `bytes` (no evento `SELADO`) múltiplo de
  4096. Este cenário está fixado pelo teste
        `rajadaProduzVariosChunksPorTamanhoTodosMultiplosDePad`, que injeta o limite pequeno na
        instância em vez de depender da propriedade; `-Dtpf.chunkMax=4096` continua a ser a forma de
        o observar à mão, com o líder a correr.
- `texto` pode conter `;`, `\n` e tabs: é *length-prefixed*, não há escapes nem separadores.
- Leitura direta (`Chunk.ler`) e inversa (`Chunk.lerInverso`) são implementadas em separado e
  produzem exatamente a mesma lista; a inversa é a usada em `Sealed.registos(...)`.
- O chunk transporta `autor` e `canal` **já sanitizados**, exatamente como ficaram no log: o
  `Registo` devolvido por `LogRegistos.acrescentarRegisto` é construído a partir dos valores
  escritos, para que log e chunk nunca divirjam. O `texto` não é sanitizado nem aparado em
  nenhum dos dois.

#### 5.4.2. Cifra e identificação

```
DEK     = HKDF-SHA256(MK, salt, info = "tpf-chunk-v1")     extrair + expandir 1 bloco (0x01)
salt    = 32 bytes aleatórios por chunk
nonce   = 12 bytes aleatórios por chunk
aad     = "tpf-v1" || keyVersion(4 bytes) || salt || nonce
cifra   = AES-256-GCM(chave = DEK, iv = nonce, tag = 128 bits) sobre o texto COM padding
chunkId = SHA-256 hexadecimal do texto cifrado, INCLUINDO a tag
```

`abrir` verifica por esta ordem: **(1)** `SHA-256(texto cifrado) == chunkId`; **(2)**
`keyVersion == 1`; **(3)** autenticação GCM. Qualquer falha lança `ChunkCrypto.CifraException`.
A ordem é significativa: a verificação do hash é barata e deteta alterações antes de se usar
qualquer chave; a da versão evita interpretar metadados de um formato desconhecido; a do GCM é a
única que prova autenticidade do conteúdo.

#### 5.4.3. Ficheiros em `selados/`

```
selados/<chunkId>.bin   texto cifrado, incluindo a tag GCM (16 bytes)
selados/<chunkId>.hdr   texto: "keyVersion;saltBase64;nonceBase64\n"   ex.: "1;q1s…;7Xk…"
```

- `<chunkId>` é o SHA-256 do **conteúdo do `.bin`**, pelo que o nome é verificável:
  `sha256sum selados/<id>.bin` devolve `<id>`. O `.hdr` é metadado do mesmo chunk e usa o mesmo
  id (o id identifica o chunk, não cada ficheiro).
- `.bin` tem exatamente `len(texto em claro) + 16` bytes: o padding é aplicado ao texto em claro
  e a tag GCM não é padded.
- Ambos os ficheiros são escritos com `force(true)` e, no fim, faz-se `fsync` ao diretório para
  que as entradas de diretório sejam duráveis.
- Os chunks são **imutáveis**: o nome é o hash do conteúdo, por isso qualquer reescrita muda o
  nome. O registo seguinte abre um chunk novo.
- Um chunk selado abre-se com `Sealed.ler(pasta, chunkId)` (lê o `.hdr`) e
  `Sealed.registos(chaveMestra)` (verifica, decifra e reconstrói por leitura inversa).
- O chunk é **derivado** de `registos.log`: se um chunk se perder (falha ao cifrar ou a gravar),
  os registos continuam duráveis no log e o chunk pode ser reconstruído a partir dele. A fonte
  de verdade é sempre o log, nunca o chunk.

## 6. Formato de `eventos-lider.txt`

Uma linha por evento, em UTF-8, terminada por `\n`:

```
instante;evento;detalhe\n
```

`detalhe` pode conter `;` — quem lê deve usar `split(";", 3)`.

| Evento | Quando | Exemplo de `detalhe` |
| --- | --- | --- |
| `ARRANQUE` | o líder arranca | `registos.log recuperado: último LSN=42, sem reparações` |
| `REPARACAO` | cauda parcial descartada | `registos.log: 37 bytes de uma escrita parcial descartados; ultimoLsn=42; intent=6f1c3a9e-…` |
| `REGISTO` | registo durável | `lsn=43 autor=ana canal=arquivo bytes=58` |
| `SELADO` | chunk selado | `chunkId=ab12… lsn=1-500 bytes=69632 cifrados=69648` |
| `COMANDO` | slash command invocado | `/registar de ana` |
| `RECUSADO` | comando de admin sem o papel | `/config de rui sem o papel admin-arquivo` |
| `INVALIDO` | argumento inválido | `/recuperar: Intervalo inválido: de (9) tem de ser ≤ ate (3).` |
| `ERRO` | falha de I/O, de rede, ou log em fail-stop | `/registar: falha ao gravar em registos.log: Log bloqueado (fail-stop) …` |
| `COMANDOS` | comandos registados | `7 comandos registados em tpf-teste` |

No evento `SELADO`, `bytes` é o **tamanho em claro com padding** (sempre múltiplo de
`PAD_BYTES`) e `cifrados` é o tamanho do `.bin` (`bytes` + 16 da tag GCM); `lsn=a-b` é o
intervalo fechado dos registos contidos no chunk.

Regras de integridade do diário:

- A linha `REPARACAO` é escrita diretamente pelo `LogRegistos`, com `force(true)`, **antes** de
  a intenção ser apagada — é isso que garante "no máximo uma vez" (§5.2.4).
- Uma linha **terminada** com formato inválido aborta o arranque (§5.2.4); uma cauda **sem**
  terminador é escrita parcial e é cortada — mas só depois de as linhas completas terem sido
  validadas.
- Os restantes eventos passam pelo `Eventos`. Uma falha ao escrever um evento de operação
  (`COMANDO`, `REGISTO`, `ERRO`) é reportada na consola mas **não** faz falhar a operação
  principal: perder uma linha de evidência é menos grave do que rejeitar uma escrita de registo
  já concluída.

## 7. Comandos

| Comando | Quem pode usar | Validação | Estado |
| --- | --- | --- | --- |
| `/registar <texto>` | todos | texto não vazio | **implementado**: responde com o LSN atribuído |
| `/estado` | todos | — | **implementado**: último LSN, por réplica estado/ocupação/nº de chunks, e no total chunks `SELADO`, `DURAVEL` e subreplicados |
| `/recuperar <de> <ate>` | todos | inteiros ≥ 1 e `de` ≤ `ate` | provisório |
| `/config r <n>` | `admin-arquivo` | inteiro ≥ 1 | provisório |
| `/replica-token` | `admin-arquivo` | — | **implementado**: token de uso único (10 min), resposta **efémera** |
| `/replica-remover <id>` | `admin-arquivo` | letras, dígitos, `_` e `-`, até 32 caracteres | **implementado**: revoga a chave, desliga e exclui da colocação |
| `/rajada <n> <intervalo_ms>` | `admin-arquivo` | `1 ≤ n ≤ 5000` e `intervalo_ms` ≥ 0 | **implementado**: `n` registos pelo caminho de `/registar` |

Notas:

- Todos os slash commands só funcionam no canal `#arquivo`.
- O Discord só aceita nomes de parâmetros em minúsculas, por isso o `<intervaloMs>` do
  enunciado aparece como `intervalo_ms`.
- As mensagens de pessoas (não bots nem webhooks) em `#arquivo` são registadas pelo **mesmo**
  caminho de `/registar` e respondidas com o LSN. O líder ignora as suas próprias mensagens e
  as de outros bots.
- `/rajada` adia a resposta (`deferReply`) porque `n` registos custam `n` `fsync` e podem
  exceder os 3 segundos que o Discord dá para responder; no fim envia o intervalo de LSN
  atribuído. O limite de 5000 existe para não bloquear o agendador indefinidamente.
- Um comando de administração recusado devolve o **nome real** do comando na mensagem
  (`Não tem permissão para usar /config: é preciso o papel admin-arquivo.`).
- `/registar` guarda o texto **exatamente como foi submetido**, incluindo espaços e mudanças de
  linha no início e no fim (`isBlank()` rejeita o vazio, mas nada é aparado). Só os valores que
  são *tokens* — como o `<id>` de `/replica-remover` — são aparados antes de validados. As
  mensagens de `#arquivo` (`getContentRaw()`) têm o mesmo tratamento, pelo que o mesmo texto
  produz o mesmo registo por qualquer das vias.
- Um `/registar` (ou mensagem em `#arquivo`) só devolve **erro** quando falha a **fase do log**:
  nesse caso nada ficou gravado e a mensagem di-lo explicitamente («o registo **não** foi
  gravado … podes repetir sem duplicar»). Se falhar a **fase do chunk** (artefacto derivado), a
  resposta é o LSN — o registo está durável — e a falha aparece apenas como evento `ERRO` em
  `eventos-lider.txt`.

## 8. Como verificar os critérios de aceitação do E2-02

Os critérios do **E2-03** estão em §10.9. As verificações seguintes são as do caminho
de escrita local (E2-02) e continuam válidas.

Arranque o líder, execute alguns `/registar` e depois, noutra consola:

```bash
# 1. /registar responde com o LSN e a linha já está em registos.log
tail -n 1 dados-lider/registos.log

# 2. a numeração continua sem repetições nem saltos (reiniciar o líder antes)
awk -F';' 'NR>1 && $1 != prev+1 {print "salto na linha " NR} {prev=$1}' dados-lider/registos.log

# 3. o último LSN do ficheiro
cut -d';' -f1 dados-lider/registos.log | tail -1

# 4. o ficheiro termina sempre em '\n'
tail -c 1 dados-lider/registos.log | xxd

# 5. não ficou nenhuma reparação pendente
ls dados-lider/reparacao.intent 2>/dev/null && echo "PENDENTE" || echo "sem intenção pendente"

# 6. duas reparações nunca produzem o mesmo intent
grep -o 'intent=[^; ]*' dados-lider/eventos-lider.txt | sort | uniq -d   # deve estar vazio

# 7. o diário de eventos é válido (3 campos)
awk -F';' 'NF<3 {print "linha inválida: " NR}' dados-lider/eventos-lider.txt
```

Verificar os **chunks selados** (objetivos 3 e 4):

```bash
# 8. cada .bin tem o SHA-256 do seu conteúdo como nome (deve dar ERRADO nenhum)
for f in dados-lider/selados/*.bin; do
  [ "$(sha256sum "$f" | cut -d' ' -f1).bin" = "$(basename "$f")" ] || echo "ERRO: $f"
done

# 9. nenhum texto registado aparece em claro nos chunks
grep -r "o-meu-texto-secreto" dados-lider/selados/ \
  && echo "ERRO: texto em claro!" || echo "ok: sem texto em claro"

# 10. todos os chunks têm tamanho em claro múltiplo de PAD_BYTES (campo bytes= do SELADO)
grep ';SELADO;' dados-lider/eventos-lider.txt | grep -o 'bytes=[0-9]*' \
  | while read b; do n=${b#bytes=}; [ $((n % 4096)) -eq 0 ] || echo "ERRO: $n"; done

# 11. os intervalos de LSN dos chunks cobrem todo o log, sem buracos nem sobreposições
grep ';SELADO;' dados-lider/eventos-lider.txt | grep -o 'lsn=[0-9]*-[0-9]*'

# 12. o .hdr de cada chunk tem keyVersion;salt;nonce
cat dados-lider/selados/*.hdr | awk -F';' 'NF!=3 {print "ERRO: hdr inválido"}'
```

Um registo deve dar um chunk selado **cerca de 5 s** depois de ser escrito (selagem por tempo);
`/rajada 500 0` com `-Dtpf.chunkMax=4096` produz **vários** chunks por tamanho:

```bash
mvn -q compile exec:java -Dtpf.guild=<ID> -Dtpf.chunkMax=4096
#  no Discord: /rajada 500 0
#  depois: ls -l dados-lider/selados/           -> vários .bin
#          grep -c ';SELADO;' dados-lider/eventos-lider.txt
```

Simular uma **escrita parcial** (deve ser reparada, com um único `REPARACAO`):

```bash
cp dados-lider/registos.log /tmp/backup.log
head -c 20 /tmp/backup.log >> dados-lider/registos.log
# reiniciar o líder:
#  - aparece REPARACAO em eventos-lider.txt, com intent=<uuid>
#  - o ficheiro volta ao tamanho anterior (termina em '\n')
#  - o LSN seguinte é o correto, sem repetições
#  - reparacao.intent já não existe
# reiniciar outra vez: grep -c ';REPARACAO;' não aumenta
```

Simular uma **cauda cortada em Base64** (deve ser descartada, §5.1.1):

```bash
# cortar a última linha ao meio: o campo Base64 fica incompleto
head -c $(( $(stat -c%s dados-lider/registos.log) - 12 )) dados-lider/registos.log \
  > /tmp/parcial.log && cp /tmp/parcial.log dados-lider/registos.log
# reiniciar: REPARACAO + LSN retomado; nenhum registo inventado com texto "parecido"
```

Simular um **Base64 inválido numa linha terminada** (deve abortar, §5.1.1):

```bash
printf '2;2025-01-01T00:00:00Z;rui;arquivo;@@@nao-e-base64@@@\n' >> dados-lider/registos.log
# reiniciar: "Linha N: Base64 inválido no 5.º campo ... é corrupção"
#   -> o ficheiro NÃO é alterado
```

Simular um **crash a meio da reparação** (intenção gravada, truncagem ainda não feita):

```bash
SIZE=$(stat -c%s dados-lider/registos.log)
FIM=$(python3 -c 'print(open("dados-lider/registos.log","rb").read().rfind(b"\n")+1)')
HASH=$(head -c "$FIM" dados-lider/registos.log | sha256sum | cut -d' ' -f1)
printf 'tpf-reparacao-v1\nintentId=crashtest\nfimValido=%s\ndescartados=%s\nultimoLsn=1\nprefixoSha256=%s\n' \
  "$FIM" "$((SIZE - FIM))" "$HASH" > dados-lider/reparacao.intent
# arrancar: a truncagem é concluída e o REPARACAO é emitido uma só vez
```

Simular os **três estados** de `registos.log` perante a intenção:

```bash
SIZE=$(stat -c%s dados-lider/registos.log)
FIM=$(python3 -c 'print(open("dados-lider/registos.log","rb").read().rfind(b"\n")+1)')
HASH=$(head -c "$FIM" dados-lider/registos.log | sha256sum | cut -d' ' -f1)

# (a) tamanho == fimValido -> a truncagem já terminou: só falta a evidência
printf 'tpf-reparacao-v1\nintentId=ja-truncado\nfimValido=%s\ndescartados=0\nultimoLsn=1\nprefixoSha256=%s\n' \
  "$FIM" "$HASH" > dados-lider/reparacao.intent
#   -> conclui, emite REPARACAO uma vez, apaga a intenção

# (b) tamanho == fimValido + descartados -> cauda ainda presente: trunca
printf 'tpf-reparacao-v1\nintentId=cauda-presente\nfimValido=%s\ndescartados=%s\nultimoLsn=1\nprefixoSha256=%s\n' \
  "$FIM" "$((SIZE - FIM))" "$HASH" > dados-lider/reparacao.intent
#   -> trunca em fimValido, emite REPARACAO uma vez, apaga a intenção

# (c) tamanho != fimValido e != fimValido + descartados -> estado não descrito: aborta
printf 'tpf-reparacao-v1\nintentId=inconsistente\nfimValido=%s\ndescartados=9999\nultimoLsn=1\nprefixoSha256=%s\n' \
  "$FIM" "$HASH" > dados-lider/reparacao.intent
#   -> ERRO FATAL: "a cauda tem N bytes ... mas a intenção registou descartados=9999"
#   -> log, intenção e diário intactos
```

Simular **prefixo que não corresponde à intenção** (deve abortar, sem tocar em nada):

```bash
# (a) hash errado
FIM=$(python3 -c 'print(open("dados-lider/registos.log","rb").read().rfind(b"\n")+1)')
printf 'tpf-reparacao-v1\nintentId=hash-mau\nfimValido=%s\ndescartados=10\nultimoLsn=1\nprefixoSha256=%s\n' \
  "$FIM" "$(printf '0%.0s' {1..64})" > dados-lider/reparacao.intent
#   -> "o conteúdo do prefixo não corresponde à impressão digital registada"

# (b) byte alterado dentro de um registo (continua a descodificar, mas o hash muda)
cp dados-lider/registos.log /tmp/backup.log
printf 'A' | dd of=dados-lider/registos.log bs=1 seek=30 conv=notrunc
# reiniciar com a intenção de (a) corrigida para o hash antigo
#   -> aborta com o hash calculado no diagnóstico (P6 a apanhar o que P2–P5 não apanham)
```

Simular **intenção inconsistente com o log** (deve abortar, sem tocar em nada):

```bash
# fimValido maior do que o ficheiro -> aborta (P1) e a intenção fica
SIZE=$(stat -c%s dados-lider/registos.log)
printf 'tpf-reparacao-v1\nintentId=inconsistente\nfimValido=%s\ndescartados=10\nultimoLsn=1\n' \
  "$((SIZE + 100))" > dados-lider/reparacao.intent
# arrancar: "Reparação pendente ... esperava N bytes mas o ficheiro só tem M ..."

# fimValido a cair a meio de uma linha -> aborta (P2)
# ultimoLsn que não corresponde ao prefixo -> aborta (P5)
# log que cresceu com registos completos -> aborta (regra da cauda)
```

Simular **intenção ilegível** (deve abortar e preservar o ficheiro):

```bash
printf 'isto-nao-e-uma-intencao\n' > dados-lider/reparacao.intent
# arrancar: "Ficheiro de intenção ilegível ... remova-o manualmente"
#   -> reparacao.intent NÃO é apagado
```

Simular **diário de eventos corrompido** (deve abortar e preservar o diário, incluindo a cauda):

```bash
cp dados-lider/eventos-lider.txt /tmp/eventos.bak
printf 'linha-sem-campos\nresto-de-uma-linha' >> dados-lider/eventos-lider.txt
# reiniciar: "eventos-lider.txt, linha N: esperava instante;evento;detalhe (3 campos)"
#   -> o diário fica IDÊNTICO, incluindo 'resto-de-uma-linha' (validar antes de alterar)
```

Simular **corrupção do log** (deve abortar o arranque):

```bash
cp dados-lider/registos.log /tmp/backup.log
printf 'X' | dd of=dados-lider/registos.log bs=1 seek=30 conv=notrunc
# reiniciar o líder:
#  - "ERRO FATAL NO ARRANQUE: registos.log inválido: ..." e exit code 1
#  - o ficheiro NÃO é alterado e NÃO é criada nenhuma intenção
```

Simular **dados sem `master.key`** (deve abortar sem criar uma chave nova):

```bash
mv master.key /tmp/master.key.bak
./correr-bot.sh
#   -> "ERRO FATAL NO ARRANQUE: existem dados do arquivo em … mas falta a chave mestra …"
ls master.key 2>/dev/null && echo "ERRO: criou uma chave nova!" || echo "ok: nao criou chave"
mv /tmp/master.key.bak master.key     # repor a chave verdadeira
```

Testes automáticos (não precisam de Discord nem de rede):

```bash
mvn -q test
# o stderr com "ERRO CRÍTICO ..." e "AVISO: eventos-lider.txt: N bytes ..." é esperado:
# vem dos testes que simulam falhas de escrita e caudas parciais, não de falhas da suite.
# a suite demora ~5 s a mais por causa do teste da selagem temporal.

# para ver o resumo escondido pelo -q:
grep -h "Tests run" target/surefire-reports/*.txt
```

- `WireTest` — uma `master.key` com 16 bytes origina `MasterKeyException` e os bytes
  originais são preservados (nada de substituição silenciosa).
- `LogRegistosTest` — cobre, por grupos:
  - **regras de validação do log e a decisão sobre Base64 (§5.1.1)**: cauda parcial reparada
    com continuação correta do LSN; **cauda sem `\n` com Base64 inválido é truncada**; **cauda
    sem `\n` que ainda analisa bem é descartada na mesma**; **Base64 inválido numa linha
    terminada por `\n` aborta sem alterar o ficheiro**; linha inválida no meio aborta; LSN fora
    de sequência aborta; round-trip de texto com `;` e mudanças de linha;
  - **fail-stop**: escrita falhada + truncagem falhada bloqueia; escrita falhada + `force` do
    rollback falhado bloqueia; `force` inicial falhado + `force` do rollback falhado bloqueia
    (com `truncate` a correr); em todos, a tentativa seguinte é recusada sem tocar no canal, o
    conteúdo não é agravado e `ultimoLsn` não avança; com rollback completo (truncate + force)
    o log continua utilizável e a linha seguinte recebe o LSN esperado;
  - **retoma da reparação**: intenção pendente com o evento já emitido não duplica
    `REPARACAO`; intenção pendente com a cauda ainda presente trunca e emite uma vez;
    intenção pendente já truncada emite a evidência em falta uma vez; cauda parcial de
    `eventos-lider.txt` é cortada depois da validação;
  - **validação do prefixo (P1–P6)**: hash correto conclui; hash divergente aborta sem truncar
    nem emitir; prefixo com linha terminada inválida aborta **antes** de truncar ou emitir;
    `ultimoLsn` incorreto aborta; `fimValido` a cortar um registo aborta; `fimValido=0` com
    `ultimoLsn≠0` aborta; log ausente aborta; log mais curto aborta; log que cresceu aborta sem
    truncar;
  - **conferência da cauda (P7)**: cauda menor que `descartados` aborta e não reporta 37 bytes
    quando só havia 5; cauda maior que `descartados` aborta; `descartados == 0` com cauda
    presente aborta; cauda exata com hash conclui (P6 e P7 no mesmo cenário); **cauda com o
    tamanho exato mas terminada por `\n` aborta**; **cauda cujo `\n` está a meio (último byte
    não é `\n`) aborta** — os dois últimos comparam os bytes iniciais dos três ficheiros;
  - **diário de eventos**: evento de outro tipo que mencione o `intent` não suprime o
    `REPARACAO`; `intent=abc` não deduplica contra `intent=abc-outro`; linha terminada inválida
    aborta; instante inválido aborta; diário com linha inválida **e** cauda parcial não é
    alterado (prova de "validar antes de alterar"); diário com linha de 2 campos e cauda
    parcial também não é alterado; `intencaoIncompativelNaoAlteraODiarioDeEventos` (cauda
    parcial no log **e** no diário, com intenção de `descartados` errado — os três gatilhos ao
    mesmo tempo) e `logCorrompidoNaoAlteraODiarioDeEventos` provam que, quando o arranque
    aborta, o diário fica byte a byte igual: comparam `Files.readAllBytes` antes e depois da
    chamada a `abrir`;
  - **intenção ilegível**: conteúdo inválido, campos em falta, linha sem `chave=valor` e
    `prefixoSha256` malformado abortam, sempre com o ficheiro intacto.
- `ChunkTest` — o texto em claro é **múltiplo de `PAD_BYTES`** e não acrescenta mais de um bloco
  de padding; leitura **direta** e **inversa** produzem a mesma lista e a mesma ordem de LSN;
  texto com `;`, `\n` e tabs sobrevive ao round-trip; padding com um byte diferente de zero,
  assinatura trocada, tamanho não múltiplo de `PAD_BYTES` e tamanho de corpo inválido (apanhado
  pela leitura inversa) são `Chunk.CorrupcaoException`; o instante mantém precisão de
  milissegundos.
- `ChunkCryptoTest` — round-trip de cifra/decifra com `chunkId == SHA-256(cifrado)` e `.bin` do
  tamanho do claro + 16 (tag); dois chunks do mesmo texto usam `salt` e `nonce` diferentes e dão
  ids diferentes; **byte alterado** falha no hash; **byte alterado com id recalculado** falha na
  autenticação GCM; **`salt` diferente**, **`nonce` diferente**, **`keyVersion` diferente**,
  **chave mestra diferente** e **id de outro chunk** falham todos com `CifraException`; a DEK
  derivada muda com o `salt`.
- `SealedTest` — o nome do `.bin` é o SHA-256 do seu conteúdo e o do `.hdr` usa o mesmo id; o
  `.bin` **não contém o texto em claro** e o `.hdr` só tem `keyVersion;salt;nonce` (3 campos,
  32 e 12 bytes descodificados); `Sealed.ler` + `Sealed.registos` reconstroem exatamente os
  registos; **byte alterado no `.bin`** e **outra chave mestra** fazem `abrir` falhar.
- `LeaderBotTest` — regras de texto, sem Discord: `/registar` preserva `"  olá  "` e
  `"\n  y: 1\n"`; o `<id>` de `/replica-remover` é aparado (`"  abc  "` → `"abc"`); `"   "`,
  `""` e `null` são rejeitados em ambos os modos, via `validarTexto(valor, nome, aparar)`.
- `LeaderBotChunkTest` — integração do líder sem Discord, via `LeaderBot.paraTestes(...)`
  (injeta `Eventos`, `LogRegistos`, a chave mestra, a pasta de dados e, quando é preciso, o
  limite de chunk; não arranca JDA nem regista comandos). Tem quatro testes:
  `falhaDoChunkNaoInvalidaUmRegistoJaDuravel` ocupa `selados/` com um **ficheiro** (falha
  determinística ao gravar o chunk) e regista três registos de ~30 KB — os dois primeiros ficam
  abaixo de `CHUNK_MAX_BYTES` e o terceiro obriga a selar — verificando que as três chamadas a
  `registar` devolvem LSN 1, 2 e 3 **sem lançar**, que os três registos ficam no log e que o
  diário ganha um `ERRO` com «chunk abandonado»; `falhaDoLogNaoAvancaOLsnNemOMandaParaOChunk`
  fecha o `LogRegistos` (falha da fase do log) e confirma que `registar` lança `IOException`,
  que `ultimoLsn()` não avança e que não há duplicados;
  `umSoRegistoProduzUmChunkSeladoPelaSelagemTemporal` regista **um** único registo e espera até
  `Wire.SELAGEM_MS + 5 s` (~5 s por omissão) pelo `.bin` em `selados/`, confirmando que o nome
  é o SHA-256 do conteúdo, que o `SELADO` tem `lsn=1-1` e que o chunk reabre devolvendo
  exatamente esse registo; e `rajadaProduzVariosChunksPorTamanhoTodosMultiplosDePad` injeta 200
  registos pelo método que `/rajada` usa (`injetarRajada`, cujo texto sintético é `rajada i/n`,
  ~50 bytes de corpo por registo) com o limite de chunk injetado a 4096 bytes — o mesmo valor
  que `-Dtpf.chunkMax=4096` — e verifica que aparecem **vários** chunks, que cada nome é o
  SHA-256 do conteúdo, que os eventos `SELADO` têm intervalos **contíguos** a cobrir 1..200 com
  `bytes=` múltiplo de `PAD_BYTES`, e que reabrir cada chunk devolve exatamente os registos do
  seu intervalo — comparando, chunk a chunk, os registos reconstruídos com os do log nesse
  intervalo (LSN, autor, canal, texto e instante em milissegundos, que é a resolução do
  formato binário) — 200 no total, com LSN 1..200 sem saltos.

Os testes do **E2-03** (sem Discord nem rede real) são `WireMensagensTest`, `PlacementTest`,
`ReplicaRegistryTest`, `CatalogTest`, `PubSubHubTest` e `LeaderBotReplicacaoTest`; a lista
detalhada está em §10.12. `PubSubHubTest` injeta um `CanalLigacao` falso (o hub não abre
sockets) e `LeaderBotReplicacaoTest` injeta um `Publicador` que captura os `STORE` em vez de
os enviar pela rede.

Os testes de fail-stop usam um canal injetado (`LogRegistos.paraTestes` com uma implementação
de `LogRegistos.Canal` em memória), porque não há forma de provocar falhas de I/O
determinísticas num `FileChannel` real. O canal de teste permite escolher **quais** as chamadas
a `forcar()` falham, o que é o que distingue "rollback completo" de "truncagem ok, `force`
falhado". Pela mesma razão, `LeaderBotChunkTest` usa `LeaderBot.paraTestes(...)`: injeta
`Eventos`, `LogRegistos`, a chave mestra e a pasta de dados sem arrancar JDA nem registar
comandos, e provoca a falha do chunk ocupando o nome `selados/` com um ficheiro comum.

## 9. Notas de desenho

- **Durabilidade antes da resposta.** O LSN só é devolvido depois de `force(true)`. Uma falha
  depois do `force` e antes de a resposta chegar ao Discord deixa o registo gravado — o log é a
  fonte de verdade. Uma falha antes do `force` deixa uma cauda parcial, que o arranque trunca;
  nenhum LSN confirmado é perdido.
- **Um único caminho de escrita.** `/registar`, `#arquivo` e `/rajada` convergem todos no mesmo
  método `synchronized`, que faz (a) log durável e (b) entrada no chunk aberto, pela mesma
  ordem. Assim as garantias de LSN, de `fsync` e de ordem dentro do chunk não dependem do
  comando usado, e nenhuma selagem concorre com um append.
- **LSN recuperado do ficheiro.** A numeração vem de `último + 1` lido de `registos.log`, nunca
  de um contador em memória, e a atribuição é atómica dentro do método sincronizado.
- **O chunk é derivado, o log é a fonte de verdade.** Se a cifra ou a escrita do chunk falhar,
  os registos continuam duráveis no log — um chunk **nunca** é a única cópia de um registo — e
  o log permite reconstruir qualquer chunk. Numa falha de selagem o chunk aberto é
  **abandonado** (deixa de aceitar registos) e o intervalo de LSN afetado fica registado como
  `ERRO`: mantê-lo aberto propagaria a falha a todos os registos seguintes e deixaria um
  temporizador armado para selar um chunk condenado.
- **Falha do log ≠ falha do chunk.** O caminho de escrita tem duas fases com semânticas
  opostas: o log é a fonte de verdade (falha ⇒ nada foi gravado, o utilizador é informado e
  pode repetir sem duplicar) e o chunk é artefacto derivado (falha ⇒ o registo está durável, o
  LSN é definitivo, a resposta é o sucesso e a falha só aparece como `ERRO` em
  `eventos-lider.txt`). Só a `IOException` da fase 1 chega ao chamador — é isso que impede um
  erro de chunk de provocar um registo duplicado por repetição do utilizador.
- **O limite do chunk é injetável.** `Wire.CHUNK_MAX_BYTES` é `static final` e é lido uma
  única vez por JVM, pelo que um valor vindo de `-Dtpf.chunkMax` não é fiável dentro de uma
  suite que reutiliza o processo. O líder guarda o limite num campo de instância
  (`limiteChunk`, por omissão `Wire.CHUNK_MAX_BYTES`) e a fábrica
  `paraTestes(..., limiteChunk)` permite exercitar a selagem por tamanho com limites pequenos;
  em produção o valor vem sempre de `Wire`.
- **Selagem: o registo que provoca o limite fica no chunk.** O registo entra primeiro e só
  depois se compara com o limite, pelo que nenhum registo é deixado de fora nem duplicado na
  fronteira. A selagem por tempo é armada no **primeiro** registo do chunk e o callback confirma
  que o chunk ainda é o mesmo antes de selar.
- **O critério é o terminador, não a validade sintática.** Base64 inválido numa cauda sem `\n` é
  o resultado normal de um crash a meio do append e descarta-se; Base64 inválido numa linha
  terminada é corrupção e aborta (§5.1.1). Tratar os dois casos da mesma forma daria ou um
  arranque impossível em cada crash, ou uma perda silenciosa de registos confirmados.
- **Intenção antes da ação.** Qualquer alteração destrutiva ao log (a truncagem da cauda) é
  precedida de um registo durável do que se vai fazer e do que já se sabe — incluindo o conteúdo
  do prefixo (hash) e o tamanho da cauda (`descartados`). É o que torna a reparação retomável,
  o evento não duplicável e a reparação **verificável**.
- **Validar o prefixo, não só o tamanho.** `tamanho ≥ fimValido` garante que nada falta, mas
  não que o que está lá é o que a intenção descreve. Somar terminador (P2), consumo integral
  (P3), estrutura (P4), continuidade (P5), impressão digital (P6) e tamanho da cauda (P7) é o
  que distingue "compatível" de "do mesmo comprimento".
- **Distinguir "já reparado" de "por reparar".** `tamanho == fimValido` e
  `tamanho == fimValido + descartados` são os dois estados que a intenção descreve; qualquer
  outro é um estado que ninguém descreveu. Sem esta distinção, uma cauda mais curta que o
  registado seria truncada e **reportada** como uma reparação de 37 bytes que nunca aconteceu.
- **Uma reparação nunca destrói estado que não descreve.** Se o log não corresponder à
  intenção — ausente, mais curto, com prefixo corrompido, com hash diferente, com cauda de
  tamanho diferente, ou com registos completos para lá de `fimValido` — o arranque aborta e os
  ficheiros ficam para inspeção. Truncar "para o tamanho que a intenção diz" seria adivinhar, e
  adivinhar mal significa perder registos confirmados.
- **Validar antes de alterar, em todo o arranque.** O arranque tem duas fases: **validar**
  (linhas completas do diário, intenção pendente, compatibilidade P1–P7 e varrimento do log) e
  só depois **alterar** (cortar a cauda do diário, gravar a intenção, truncar o log, emitir
  `REPARACAO`). Perante corrupção ou incompatibilidade, os **três** ficheiros ficam byte a byte
  como o operador os deixou — incluindo a cauda parcial do diário, que de outro modo seria o
  primeiro alvo de escrita do arranque.
- **Evidência não se confunde com menção.** A deduplicação de `REPARACAO` compara campos exatos
  (`evento == REPARACAO` e `intent == <id>`), porque uma substring transforma qualquer
  referência numa prova e suprime a evidência que se queria garantir.
- **A mesma regra para o log e para o diário.** Linha terminada por `\n` é sempre um commit: se
  o formato não for válido, é corrupção e o arranque para. Linha sem terminador é sempre
  escrita parcial e é cortada (depois de validar). Uma única regra, dois ficheiros, sem
  exceções.
- **O padding é validado, não apenas ignorado.** O cabeçalho diz onde acaba o conteúdo e o
  intervalo restante tem de ser todo nulo. Sem isso, "ignorar o padding" transformaria bytes
  desconhecidos em conteúdo silenciosamente descartado.
- **Fail-stop em vez de melhor esforço.** Quando a reposição do ficheiro não pode ser tornada
  durável, o processo prefere parar de escrever a assumir que sabe onde acaba o último registo
  válido. Perder disponibilidade alguns segundos é preferível a agravar o log.
- **Fail-fast no arranque.** A `master.key`, o `registos.log` (com a intenção pendente) e o
  `eventos-lider.txt` são validados **antes** de abrir qualquer socket de rede: o bot nunca fica
  online num estado em que não consegue garantir as invariantes. O processo termina com código 1
  e uma mensagem em `stderr`.
- **Estado no objeto, não em `static`.** O `LeaderBot` é uma instância única que guarda
  `Eventos`, `LogRegistos`, a chave mestra, a pasta de dados, o chunk aberto e o agendador; o
  `bloqueado` do log é um campo de instância. Isso torna o fecho limpo (shutdown hook) e os
  testes possíveis.
- **Nunca se gera uma chave nova por cima de dados existentes.** `Wire.carregarOuCriarMasterKey`
  cria a chave quando ela falta, o que é o comportamento correto no primeiro arranque e o
  comportamento errado quando já há `registos.log`, `eventos-lider.txt` ou `selados/`: a chave
  nova torna os chunks anteriores indecifráveis e o líder ficaria a arrancar sobre um arquivo
  que não sabe ler. O arranque valida essa combinação antes de tocar no `Wire` e recusa,
  exigindo `-Dtpf.novaChave=true` para uma rotura consciente.
## 10. Sprint E2-03 — hub, réplicas, colocação e catálogo

Esta secção documenta o que o sprint E2-03 acrescenta. O caminho de escrita local
(§5) e a cifra (§5.4) mantêm-se exatamente como no E2-02; o que muda é o que
acontece **depois** de o chunk ser selado e gravado em `selados/`.

### 10.1. Protocolo textual

Todas as mensagens são **texto numa trama WebSocket**, com campos separados por `;`
e bytes binários em Base64. O hub acrescenta apenas duas mensagens de
encaminhamento, `SUB;<topico>` e `PUB;<topico>;<mensagem>`.

| Mensagem | Quem envia | Formato | Uso |
| --- | --- | --- | --- |
| `OLA` | réplica | `OLA;<id>` ou `OLA;<id>;<chavePublicaB64>;<token>` | início de ligação; a segunda forma é o 1.º registo |
| `DESAFIO` | hub | `DESAFIO;<nonceB64>` | nonce de 32 bytes, novo por ligação, enviado diretamente |
| `PROVA` | réplica | `PROVA;<id>;<assinaturaB64>` | assinatura Ed25519 de `tpf-registo\|<id>\|<nonceB64>` (UTF-8) |
| `SUB` | réplica | `SUB;<topico>` | só `tpf-r-<o seu id>` e `tpf-ctl` |
| `PUB` | ambos | `PUB;<topico>;<mensagem>` | encaminhamento; a réplica só publica em `tpf-lider` |
| `HB` | réplica | `HB;<id>;<usados>;<quota>` | dentro de `PUB;tpf-lider;...` |
| `INVENTARIO` | réplica | `INVENTARIO;<id>;<chunkId1>,<chunkId2>,...` | dentro de `PUB;tpf-lider;...`; completo após cada ligação |
| `ACK` | réplica | `ACK;<id>;<chunkId>;<sha256>` | dentro de `PUB;tpf-lider;...`; só depois de gravar |
| `STORE` | líder | `STORE;<chunkId>;<hdrB64>;<binB64>` | dentro de `PUB;tpf-r-<id>;...` |

O `hdrB64` é o conteúdo do `.hdr` (`keyVersion;salt;nonce\n`) e o `binB64` é o
conteúdo do `.bin` (texto cifrado + tag GCM). Não há mensagem de confirmação de
autenticação: a réplica envia `PROVA` e, **de seguida**, `SUB`; o WebSocket preserva
a ordem e o hub processa a prova antes das subscrições. Se a prova falhar, o hub
fecha a ligação e as `SUB` nunca são aceites.

Uma trama `STORE` com um chunk de 64 KiB em claro tem cerca de 87 KiB em Base64; o
servidor Java-WebSocket é criado com um limite de 4 MiB por trama
(`Draft_6455`), para não fragmentar mal os chunks.

### 10.2. `PubSubHub`

O hub corre no **mesmo processo** do `LeaderBot` e estende
`org.java_websocket.server.WebSocketServer`. Cada ligação tem uma máquina de
estados:

```
NOVA --OLA--> DESAFIADA --PROVA válida--> AUTENTICADA
```

Regras implementadas:

- antes de `AUTENTICADA`, só são aceites `OLA` e `PROVA`; `SUB`, `PUB`, um segundo
  `OLA` ou uma `PROVA` sem desafio desligam a ligação (código 1008) e registam
  `ERRO`;
- o nonce tem 32 bytes de `SecureRandom`, é novo em cada ligação e é **descartado
  após uma tentativa**, mesmo que a assinatura seja inválida (uma segunda `PROVA`
  na mesma ligação é recusada);
- uma réplica nova (`id` ausente de `replicas.txt`) tem de enviar chave pública e
  token; o token tem de existir, não ter sido usado e ter menos de
  `TOKEN_VALIDADE_MS`; a chave e o token só são aceites **depois** de uma `PROVA`
  válida, e nesse momento o token é consumido e `replicas.txt` é reescrito
  atomicamente;
- um `OLA` de um `id` já registado com **outra** chave é recusado; com a mesma
  chave, `OLA;<id>` (sem chave/token) é suficiente e o desafio-resposta continua a
  ser obrigatório;
- uma réplica só pode subscrever `tpf-r-<o seu id>` e `tpf-ctl`, e só pode publicar
  em `tpf-lider`; qualquer outra subscrição/publicação desliga e regista `ERRO`;
- `publish(topico, mensagem)` envia `PUB;<topico>;<mensagem>` apenas às ligações
  autenticadas que subscreveram o tópico;
- as mensagens `PUB;tpf-lider;...` são entregues ao líder **em processo**, com o
  `id` autenticado da ligação (nunca com o id escrito pelo cliente);
- `/replica-remover` chama `removerReplica(id)`: remove a chave de `replicas.txt`,
  fecha as ligações autenticadas com esse id e remove a réplica do
  `ReplicaRegistry`. As cópias no catálogo ficam para a reconciliação do próximo
  sprint.

`replicas.txt` tem uma linha por réplica, em UTF-8:

```
<id>;<chavePublicaB64>\n
```

A quota **não** é guardada neste ficheiro: chega em cada `HB`, como pede o E2-03
(o quadro geral do Epico2 mostra `replica;chavePublica;quota`, mas o enunciado
deste sprint fixa `id;chavePublicaB64` e a quota vem no heartbeat). A escrita é
temporário + `ATOMIC_MOVE`, com `fsync` ao diretório.

O token é gerado por `/replica-token` com 24 bytes de `SecureRandom` em Base64
URL-safe; a resposta do Discord é **efémera** e o token **não** é escrito em
`eventos-lider.txt`. Os tokens vivem em memória: reiniciar o líder invalida os
tokens ainda não usados (é preciso gerar outro).

### 10.3. `HeartbeatMonitor`, `ReplicaRegistry` e estados

Para cada réplica, o líder guarda:

| Campo | Origem |
| --- | --- |
| `estado` | `VIVA`, `SUSPEITA` ou `MORTA` |
| `quota` / `usados` | último `HB` (`PUB;tpf-lider;HB;...`) |
| `reservados` | soma dos chunks já enviados e ainda sem `ACK` |
| último heartbeat | `HeartbeatMonitor` |

O espaço livre efetivo é `livre = quota − usados − reservados`. Um `HB` atualiza
quota/usados, reinicia o monitor e passa a réplica a `VIVA`. `atualizarEstados(agora)`
aplica:

- `SUSPEITA` a partir de `SUSPEITA_MS` (6000 ms = 3 × 2000 ms);
- `MORTA` a partir de `MORTA_MS` (10000 ms = 5 × 2000 ms);
- `MORTA → VIVA` no primeiro `HB` seguinte (regresso).

O líder corre `atualizarEstados` a cada `PERIODO_HB_MS / 2` e também no início de
cada `reserveAndChoose`, para nunca escolher com um estado desatualizado. Cada
transição emite `ESTADO <id> <antigo> -> <novo>`.

**Reserva atómica.** `reserveAndChoose(chunkId, tamanho, count, exclude)` constrói
os candidatos (não `MORTA`, com `livre >= tamanho + MARGEM_BYTES`), chama
`Placement.choose` e soma `tamanho` aos `reservados` das escolhidas, **tudo dentro
da mesma secção sincronizada**. Ao receber um `ACK` válido, `confirmarReserva`
passa `tamanho` de `reservados` para `usados`; ao expirar `ACK_TIMEOUT_MS`,
`libertar` retira as reservas que não foram confirmadas. Sem a reserva, dois
chunks selados entre dois heartbeats poderiam escolher a mesma réplica com base no
mesmo espaço livre e exceder a quota.

As réplicas registadas em `replicas.txt` mas ainda sem heartbeat neste arranque
entram no registo como `MORTA` (e aparecem assim em `/estado`), até ao primeiro
`HB`.

### 10.4. `Placement`

A pontuação de cada par (chunk, réplica) é

```
u(c, r) = ((primeiros 53 bits de SHA-256(c + "|" + r)) + 0.5) / 2^53   ∈ ]0,1[
pontuação(c, r) = livre(r)^ALFA / -ln(u(c, r))
```

`choose` filtra `livre >= tamanho + MARGEM_BYTES`, ordena por pontuação
decrescente (desempate por id) e devolve até `count` ids. É determinística para o
mesmo chunk e o mesmo estado, e a ordem de iteração da coleção de candidatos não
altera o resultado.

Distribuição medida com 10 000 chunkIds e quotas 8/2/4/6 MB (a mesma da
experiência 5; `mvn -q test` fixa estes valores):

| α | 8 MB | 2 MB | 4 MB | 6 MB |
| --- | --- | --- | --- | --- |
| 0 | 25,6 % | 24,8 % | 24,7 % | 24,9 % |
| 1 | 40,5 % | 9,8 % | 19,8 % | 30,0 % |
| 2 | 53,8 % | 3,2 % | 13,2 % | 29,9 % |

Com α = 0 a escolha é uniforme entre as candidatas, mas o filtro de espaço
continua a atuar; com α = 2 as réplicas grandes dominam e as pequenas só são
escolhidas quando as grandes enchem.

### 10.5. `Catalog`

`catalogo.txt`, uma linha por chunk, em UTF-8:

```
chunkId;lsnDe;lsnAte;tamanho;replicas;estado\n
```

Exemplo:

```
ab12…;1;75;8208;r1,r3;DURAVEL
cd34…;76;90;4112;r2;SELADO
```

- `tamanho` é o tamanho do `.bin` (texto cifrado + tag), que é o que ocupa quota na
  réplica;
- `replicas` são as cópias **confirmadas por ACK**, separadas por vírgulas;
- `SELADO` = ainda não cumpre o quórum; `DURAVEL` = cumpre
  `max(W_MIN, ⌊R/2⌋+1)` (2 com R = 3, 3 com R = 5);
- **subreplicado** é derivado (`cópias < R`), não é um estado: um chunk pode estar
  `DURAVEL` com 2/3 cópias;
- `adicionar`, `confirmar` e `gravarAtomico` são sincronizados; o ficheiro é
  reescrito a cada alteração com temporário + `ATOMIC_MOVE` e `fsync` ao
  diretório;
- o catálogo é carregado no arranque; um ficheiro com linhas inválidas, `chunkId`
  repetido ou estado desconhecido aborta o arranque (fail-fast, como o log).

### 10.6. Caminho de escrita replicada

Depois de o chunk ser selado e gravado em `selados/`:

1. entra no catálogo como `SELADO`;
2. `reserveAndChoose(chunkId, tamanho, R, {})`; se devolver menos de R, regista
   `SUBREPLICADO <chunkId> <previstas>/<R>` e envia às disponíveis;
3. `STORE` é publicado no tópico `tpf-r-<id>` de cada escolhida;
4. um `ACK` só é aceite se **(1)** o hash for o `chunkId`, **(2)** o `<id>` da
   mensagem for o id **autenticado** da ligação e **(3)** essa réplica tiver
   recebido o `STORE` deste chunk; a réplica é então acrescentada às cópias;
5. o chunk passa a `DURAVEL` quando os `ACK` válidos atingem
   `max(W_MIN, ⌊R/2⌋+1)`;
6. ao fim de `ACK_TIMEOUT_MS`, se houver menos de R cópias, as reservas pendentes
   são libertadas e uma nova ronda chama
   `reserveAndChoose(chunkId, tamanho, R − confirmadas, tentadas)`, excluindo
   **todas** as réplicas já tentadas; repete até R cópias ou `MAX_RONDAS`
   (continua mesmo depois de o chunk estar `DURAVEL`);
7. o catálogo é reescrito atomicamente em cada alteração.

Eventos no líder (coluna `evento`; o detalhe é o resto da linha):

| Evento | Detalhe | Quando |
| --- | --- | --- |
| `STORE` | `<chunkId> -> <id>` | pedido enviado a uma réplica |
| `ACK` | `<chunkId> <id>` | ACK válido aplicado |
| `ACK-REJEITADO` | `<chunkId> <id> <motivo>` | hash errado, id diferente ou sem STORE |
| `DURAVEL` | `<chunkId>` | o chunk atinge o quórum pela primeira vez |
| `SUBREPLICADO` | `<chunkId> <n>/<R>` | não há candidatas suficientes |
| `RONDA` | `<chunkId> <k>` | início de cada ronda (k = 1, 2, …) |
| `ESTADO` | `<id> <antigo> -> <novo>` | transição de estado |
| `REGISTO-REPLICA` | `<id> registada em replicas.txt` | 1.º registo concluído |
| `REMOVIDA` | `<id>` | `/replica-remover` |

Um `ACK` rejeitado **não** altera o catálogo nem as reservas. As cópias em
réplicas `MORTA` continuam no catálogo neste sprint (a reconciliação que as
repara/remove é o sprint seguinte).

### 10.7. Comandos Discord

| Comando | Quem | Comportamento no E2-03 |
| --- | --- | --- |
| `/replica-token` | `admin-arquivo` | gera token real, resposta **efémera**, validade 10 min, uso único |
| `/replica-remover <id>` | `admin-arquivo` | revoga a chave, fecha a ligação e exclui da colocação; cópias ficam para reconciliação |
| `/estado` | todos | por réplica: estado, `usados/quota` e nº de chunks; total de `SELADO`, `DURAVEL` e subreplicados |
| `/config r <n>` | `admin-arquivo` | provisório (não altera R neste sprint) |
| `/recuperar <de> <ate>` | todos | provisório |

O token nunca é escrito em `eventos-lider.txt` nem em qualquer ficheiro; só existe
na resposta efémera do Discord e no argumento com que a réplica é arrancada.

### 10.8. Como lançar N réplicas

Compile os dois projetos:

```bash
cd tpf-lider  && mvn -q package
cd ../tpf-replica && mvn -q package
```

Arranque o líder (com o token do bot e o id do servidor, §2 e §3):

```bash
cd tpf-lider
java -Dtpf.guild=<ID_DO_SERVIDOR> -jar target/tpf-lider-1.0.jar
# ou: mvn -q compile exec:java -Dtpf.guild=<ID_DO_SERVIDOR>
```

No Discord, execute `/replica-token` uma vez por réplica e guarde cada token. Depois,
num terminal (ou pasta) por réplica:

```bash
mkdir -p /tmp/tpf/A && cd /tmp/tpf/A
java -jar /caminho/para/tpf-replica/target/tpf-replica-1.0.jar \
     A ws://localhost:8765 8388608 <tokenA>

mkdir -p /tmp/tpf/B && cd /tmp/tpf/B
java -jar /caminho/para/tpf-replica/target/tpf-replica-1.0.jar \
     B ws://localhost:8765 2097152 <tokenB>

mkdir -p /tmp/tpf/C && cd /tmp/tpf/C
java -jar /caminho/para/tpf-replica/target/tpf-replica-1.0.jar \
     C ws://localhost:8765 4194304 <tokenC>

mkdir -p /tmp/tpf/D && cd /tmp/tpf/D
java -jar /caminho/para/tpf-replica/target/tpf-replica-1.0.jar \
     D ws://localhost:8765 6291456 <tokenD>
```

As quotas são 8, 2, 4 e 6 MB (8 388 608, 2 097 152, 4 194 304 e 6 291 456 bytes).
Cada pasta tem `dados-<id>/` e `eventos-<id>.txt`; também pode correr todas na
mesma pasta, porque os nomes incluem o id. O `<token>` só é necessário no primeiro
registo (ou se o líder tiver perdido `replicas.txt`); nas ligações seguintes basta
`ReplicaService <id> <url> <quotaBytes>`.

Se alterar `-Dtpf.periodo`, `-Dtpf.backoffMin` ou `-Dtpf.backoffMax` no líder,
passe os mesmos valores à réplica (cada processo lê as suas propriedades).

### 10.9. Como verificar os critérios e as experiências

Os critérios do E2-03, com a evidência correspondente:

1. **Registo com token.** `/replica-token` + arranque de uma réplica nova ⇒
   `REGISTO-REPLICA` em `eventos-lider.txt`, linha em `replicas.txt` e `/estado` a
   mostrá-la. Repetir o mesmo token noutra réplica, usar um token com mais de
   10 min (ou reiniciar o líder, que os perde) ou assinar a `PROVA` com outra
   chave ⇒ `ERRO` e nenhuma linha nova em `replicas.txt`.
2. **Reconexão.** Parar a réplica (`kill -9`) e observar em `/estado`
   `VIVA → SUSPEITA` ao fim de ~6 s e `SUSPEITA → MORTA` ao fim de ~10 s; voltar a
   arrancá-la ⇒ `MORTA → VIVA` e `INVENTARIO` no líder. Os eventos `ESTADO` ficam
   no diário.
3. **Backoff.** Parar o líder e olhar para `eventos-<id>.txt` da réplica: os
   intervalos são 1, 2, 4, 8, 16 e 30 s (máximo); ao voltar a ligar, o próximo é
   outra vez 1 s (`LIGADO` repõe o atraso).
4. **Rajada.** Com 3 ou 4 réplicas vivas, `/rajada 50 0` (ou 50 `/registar`):
   `grep -c ';DURAVEL;' dados-lider/eventos-lider.txt` deve acompanhar os chunks.
   Para cada chunk, `awk -F';' '$6=="DURAVEL"{print $5}' catalogo.txt` deve dar 3
   ids distintos. Em cada réplica, o nome de cada `.bin` continua a ser o
   SHA-256 do conteúdo:
   `for f in dados-<id>/*.bin; do [ "$(sha256sum "$f" | cut -d' ' -f1).bin" = "$(basename "$f")" ] || echo ERRO; done`.
5. **Distribuição.** Os testes `PlacementTest` medem 10 000 chunks com quotas
   8/2/4/6 MB e α = 0, 1 e 2; a tabela de §10.4 tem os valores. Não é preciso
   escrever chunks: `Placement.choose` é chamado diretamente, sem reservas.
6. **Menos de R.** Com 2 réplicas vivas, cada chunk fica com 2 cópias, `DURAVEL`
   (2 ≥ quórum), e `subreplicados` em `/estado` aumenta; com 1, fica `SELADO`,
   `SUBREPLICADO 1/3` e subreplicado.
7. **Quota.** Arranque uma réplica com quota muito pequena: deixa de ser escolhida
   quando `livre < tamanho + MARGEM_BYTES`; se receber um `STORE` que não cabe,
   regista `QUOTA` em `eventos-<id>.txt` e não publica `ACK`.
8. **Hash errado.** Com 4 réplicas, altere uma (teste/falha propositada) para
   responder com hash errado: o líder regista `ACK-REJEITADO`, o chunk fica
   `DURAVEL` com as outras duas e, ao fim de `ACK_TIMEOUT_MS`, a ronda 2 envia
   `STORE` à quarta réplica (visível em `RONDA <chunkId> 2` e `STORE ... -> <4.ª>`).
9. **Id de ACK.** Uma réplica que envie `ACK` com o id de outra é recusada
   (`ACK-REJEITADO ... id da mensagem (...) diferente do id autenticado`), mesmo
   que o hash esteja certo.

Inspeção rápida do catálogo e das cópias:

```bash
# uma linha por chunk, com o número de cópias confirmadas
awk -F';' '{n=split($5,a,","); print $1, n, $6}' dados-lider/catalogo.txt

# contagens por estado e subreplicados
awk -F';' '{n[$6]++; if (split($5,a,",")<3) sub++} END {for (e in n) print e, n[e]; print "subreplicados", sub}' dados-lider/catalogo.txt

# nenhum texto em claro nos chunks da réplica
grep -R "o-meu-texto" /tmp/tpf/A/dados-A/ && echo "ERRO: texto em claro" || echo "ok"

# a réplica não tem master.key nem catálogo
ls /tmp/tpf/A/dados-A/ | grep -E 'master|catalogo' && echo "ERRO" || echo "ok: só chaves e chunks"
```

### 10.10. Concorrência: estruturas partilhadas, threads e sincronização

Fontes de threads no processo do líder:

| Thread | O que faz |
| --- | --- |
| threads do JDA (eventos de comandos/mensagens) | `/registar`, `#arquivo`, `/rajada`, `/estado`, `/replica-token`, `/replica-remover` |
| threads do Java-WebSocket (`onOpen`, `onMessage`, `onClose`, `onError`) | OLA/PROVA/SUB/PUB das réplicas; entrega de HB/ACK ao líder |
| `tpf-agendador` (um `ScheduledExecutorService`) | selagem por tempo, `ACK_TIMEOUT_MS`, atualização periódica de estados, `/rajada` |
| main / shutdown hook | arranque, fecho limpo |

Estruturas partilhadas e proteção:

| Estrutura | Threads | Proteção |
| --- | --- | --- |
| `LeaderBot.chunkAberto`, `selagemAgendada`, `colocacoes` | JDA, agendador, hub (ACK) | métodos `synchronized` da instância (`registar`, `selarPorTempo`, `colocarChunk`, `receberAck`, `expirar`, `fechar`) |
| `Catalog` (`chunks`, ficheiro `catalogo.txt`) | JDA (selagem), hub (ACK), agendador, `/estado` | todos os métodos `synchronized`; escrita atómica dentro do lock |
| `ReplicaRegistry` (`replicas`, reservas) | hub (HB/auth), agendador, JDA (`/estado`), selagem | todos os métodos `synchronized`; `reserveAndChoose` faz escolha + reserva numa só secção |
| `PubSubHub.replicas` / `tokens` | threads WebSocket, JDA (`/replica-token`, `/replica-remover`) | `synchronized` no hub (`replicas`) e no mapa `tokens`; a chave só é publicada depois de `replicas.txt` estar gravado |
| `PubSubHub.subscricoes` | threads WebSocket, publicação do líder | `synchronized` no mapa; `publish` copia a lista de destinos e envia fora do lock |
| `Eventos` | todas | `synchronized` + `force(true)` por linha |
| `LogRegistos` | JDA, agendador | métodos `synchronized` do próprio log |

Ordem de locks: `LeaderBot` → `Catalog`/`ReplicaRegistry` → `Eventos`; o hub
adquire `subscricoes` sem deter o lock do `LeaderBot`, e as callbacks do hub
entram no `LeaderBot` sem deter `subscricoes`. Não há um caminho que adquira os
locks pela ordem inversa, pelo que não há inversão cíclica. O `Placement` é
stateless depois de construído e é chamado dentro do lock do registry.

Custo consciente: o `LeaderBot` mantém o lock enquanto lê o `.bin`/`.hdr` de
`selados/`, os converte para Base64 e publica os `STORE`. Isso serializa a
selagem com os ACK, mas o trabalho é de alguns milissegundos por chunk de 64 KiB
e garante que a reserva, a publicação e o estado do catálogo são coerentes. Os
envios do WebSocket são assíncronos (o Java-WebSocket coloca a trama na fila da
ligação); a publicação não espera pela rede.

No processo da réplica: a thread do `java.net.http.WebSocket` entrega as tramas
ao `processador` (uma thread, ordem preservada), a `SessaoEnvio` (uma thread)
serializa `sendText` (o JDK só permite um envio pendente por ligação) e o
`agendador` publica heartbeats e agenda religações. `Armazem` e `EventosReplica`
são `synchronized`; o `Armazem` só é tocado pelo processador.

### 10.11. Questões de reflexão

**1. Porque é que o token é necessário, se a réplica já se autentica com Ed25519?
O que acontece se um token for intercetado nos 10 minutos de validade?**

O Ed25519 prova **continuidade** de uma identidade já conhecida, mas qualquer
máquina pode gerar um par de chaves novo e dizer que é a réplica `X`. O token é a
**autorização de inscrição**: liga a chave pública nova a uma decisão do
administrador, tomada no Discord. Sem ele, o conjunto de réplicas seria aberto a
qualquer pessoa que soubesse o URL do hub — e o modelo assume réplicas
voluntárias, não hostis à partida, mas o líder não deve aceitar espaço de quem não
foi autorizado. O token é de uso único para que a sua reutilização (por exemplo,
a partir de um log ou de uma captura) não volte a registar outra chave, e vale
10 minutos para limitar a janela de exposição.

Se for intercetado dentro da validade, quem o tiver pode usá-lo **uma vez** para
registar a sua própria chave com um `id` novo (ou com um `id` ainda não usado) e
passar a receber chunks. Não consegue, porém, substituir a chave de uma réplica
já registada: um `OLA` com o mesmo `id` e chave diferente é recusado. Também
consegue consumir o token e impedir o registo legítimo (negação de serviço). A
defesa é entregar o token por um canal privado (a resposta efémera do Discord),
usá-lo imediatamente, e usar `wss://` para que não seja lido da rede. Depois de
consumido, o token desaparece; o administrador pode revogar a chave com
`/replica-remover`.

**2. Porque é que a réplica assina o `id` e um prefixo juntamente com o nonce, e
não apenas o nonce?**

Assinar só o nonce provaria posse da chave, mas não **para que identidade** nem
**em que contexto**. O prefixo `tpf-registo|` faz separação de domínio: a mesma
chave pode ser usada noutro protocolo e uma assinatura produzida aí não deve valer
aqui. O `id` liga a prova à identidade anunciada no `OLA`; sem ele, uma assinatura
obtida numa ligação que se diz `A` poderia ser reapresentada numa ligação que se
diz `B` (o hub também compara os ids, mas a assinatura passa a ser
autocontida). O nonce, novo em cada ligação e descartado após uma tentativa,
impede repetir uma `PROVA` antiga; o prefixo e o id impedem que a assinatura seja
reciclada noutro contexto. É a mesma razão pela qual o `aad` do AES-GCM inclui a
versão, o salt e o nonce: cada prova fica ligada a todos os parâmetros que a
tornam única.

**3. Porque é que a escolha e a reserva têm de acontecer na mesma secção
sincronizada? Dê um exemplo de execução que falharia sem isso.**

Porque a decisão depende do espaço livre, que é alterado pela própria decisão.
Exemplo: uma réplica B com `livre = 100` bytes, margem 0, e dois chunks de 60
bytes selados em paralelo. Sem atomicidade, as duas threads leem `livre = 100`,
ambas calculam a pontuação e ambas escolhem B; cada uma reserva 60 e B fica com
120 reservados, acima da quota. Com a escolha e a reserva no mesmo `synchronized`,
a primeira reserva 60 e deixa `livre = 40`; a segunda já não considera B elegível
(`40 < 60`) e escolhe outra réplica ou devolve vazio, originando `SUBREPLICADO`.
O mesmo argumento se aplica ao `ACK`: `confirmarReserva` move a reserva para
`usados` para que o espaço não fique sobrestimado até ao heartbeat seguinte. O
teste `ReplicaRegistryTest.reserveAndChooseEhAtomicaEntreDuasThreads` fixa
exatamente este cenário.

**4. Porque é que a durabilidade exige um mínimo absoluto (`W_MIN`) e também a
maioria de R? O que muda com `R = 5`? Que risco se corre com W = 1, e que custo
teria exigir R confirmações?**

`W_MIN` é um piso de tolerância a falhas: com R = 3, exigir só a maioria (2) já
é o mesmo que W_MIN = 2, mas se R for 2 a maioria é 2 e W_MIN garante que nunca se
desce a 1. A maioria de R garante que dois grupos de réplicas não possam ambos
dizer que têm uma cópia durável sem se intersetarem — é o mesmo princípio dos
quóruns, aplicado aqui à durabilidade. Com R = 5, o quórum sobe para
`max(2, 3) = 3`; um chunk passa a `DURAVEL` com 3 ACK e tolera a perda de 2
réplicas, mas precisa de mais tempo e mais espaço (5 cópias). Com W = 1, um único
`ACK` torna o chunk durável: se essa réplica falhar logo a seguir (ou tiver
mentido), o chunk perde-se e o líder já o anunciou como durável. Exigir R
confirmações maximiza a durabilidade (todas as cópias), mas aumenta a latência
até `ACK_TIMEOUT_MS` no pior caso, o tráfego e a probabilidade de uma ronda
falhar por uma réplica lenta; além disso, com menos de R réplicas vivas nenhum
chunk ficaria durável, mesmo com maioria. O compromisso `max(W_MIN, maioria)`
tolera uma minoria de falhas sem esperar por todas.

**5. Num sistema com vários escritores, uma maioria serve para garantir
consistência (quórum). Porque é que aqui não há esse problema?**

Porque não há versões concorrentes do mesmo dado: os chunks são **imutáveis** e
**endereçados pelo hash** do conteúdo. Um `chunkId` identifica exatamente um
conteúdo; duas cópias com o mesmo id são iguais, e escrever outra vez o mesmo
chunk é um no-op. O líder é único e serializa a atribuição de LSN, a selagem, a
colocação e a confirmação; não há dois escritores a propor valores diferentes para
a mesma posição. A maioria não serve para escolher entre valores, serve apenas
como **limiar de durabilidade** (quantas cópias independentes existem). Se o
líder cair, as escritas param (ponto único de falha) e a recuperação do próximo
sprint reconstrói o estado a partir do manifesto e dos chunks; como os chunks não
podem ser forjados (hash + GCM), uma réplica não consegue introduzir uma versão
alternativa.

**6. Qual é o efeito de α na distribuição? Com α = 0, o espaço livre ainda
influencia a escolha de alguma forma?**

A pontuação é `livre^α / -ln(u)`. Com α = 1, a probabilidade de ser a primeira
escolhida é proporcional ao espaço livre; com α = 0, `livre^0 = 1` e a pontuação
depende só do hash, pelo que a escolha é uniforme entre as candidatas. O espaço
livre continua a influenciar através do **filtro** `livre >= tamanho +
MARGEM_BYTES`: uma réplica cheia deixa de ser candidata, independentemente do α.
Com α = 2, as réplicas grandes são favorecidas de forma quadrática; as pequenas
raramente são primeiras e enchem mais devagar (ou nunca, se as grandes tiverem
sempre espaço). A experiência 5 mede ~40 %/10 % para 8/2 MB com α = 1, ~25 % para
todas com α = 0 e ~54 %/3 % com α = 2. α = 0 tende a esgotar primeiro a réplica
mais pequena em termos **relativos**, porque recebe a mesma fração absoluta de
chunks mas tem menos capacidade; α = 1 tende a igualar a ocupação relativa.

**7. Porque é que a réplica só pode enviar `ACK` depois de gravar os ficheiros
com `ATOMIC_MOVE`?**

Porque o `ACK` é a única evidência que o líder tem de que a cópia existe. Se a
réplica respondesse antes de gravar e falhasse a seguir (crash, disco cheio,
processo morto), o líder contaria uma cópia que não existe e poderia declarar
`DURAVEL` um chunk com menos de W cópias reais; no limite, nenhuma réplica teria
os bytes e o registo perder-se-ia apesar de anunciado como durável. O
`ATOMIC_MOVE` garante que um leitor nunca vê um `.bin` a meio: ou o ficheiro
antigo (no caso de reposição) ou o novo completo. Escrever primeiro o `.hdr` e
depois o `.bin`, com `force(true)` e `fsync` ao diretório, e só então publicar o
`ACK`, fecha a janela: quando o líder recebe o `ACK`, os dois ficheiros estão
duráveis. Se a réplica morrer entre os dois moves, o `.hdr` órfão é inofensivo e
um novo `STORE` completa o par (o teste `completarHdrEmFaltaMantemOBin` fixa
isso).

**8. Porque é que o líder usa o id autenticado da ligação, e não o `<id>` escrito
no `ACK`?**

Porque o `<id>` do `ACK` é conteúdo controlado pelo cliente. Uma réplica
autenticada como `A` poderia escrever `ACK;B;<chunkId>;<hash>` e fazer o líder
creditar a cópia a `B`, que não a tem: o catálogo ficaria errado, o chunk poderia
ficar `DURAVEL` com menos cópias reais e a colocação seria enganada. O id
autenticado é o que o hub obteve da `PROVA` Ed25519 e está ligado à chave pública
registada; é a única identidade em que o líder pode confiar. A validação
`ACK;B;...` numa ligação de `A` é recusada com `ACK-REJEITADO` (experiência 9). O
mesmo princípio se aplica ao `HB`: um heartbeat com o id de outra réplica é
ignorado e registado como `ERRO`.

### 10.12. Testes automáticos do E2-03

```bash
cd tpf-lider && mvn -q test
cd ../tpf-replica && mvn -q test
```

Novos testes do líder (sem Discord nem rede real):

- `WireMensagensTest` — round-trip de OLA/DESAFIO/PROVA/SUB/PUB/HB/ACK/STORE,
  rejeição de campos em falta e quórum `max(W_MIN, ⌊R/2⌋+1)`.
- `PlacementTest` — determinismo, filtro de espaço/exclusão, distribuição com
  α = 0/1/2 (10 000 chunks, quotas 8/2/4/6 MB) e `u ∈ ]0,1[`.
- `ReplicaRegistryTest` — transições `VIVA → SUSPEITA → MORTA → VIVA` com os
  limites exatos, `reserveAndChoose` atómica entre duas threads, `confirmar`/
  `libertar`, exclusão de `MORTA` e permanência de `SUSPEITA`.
- `CatalogTest` — formato, transição `SELADO`/`DURAVEL` com R = 3 e R = 5,
  escrita atómica, recarregamento, duplicação e ficheiro inválido.
- `PubSubHubTest` — primeiro registo com token, prova inválida que não consome o
  token, token reutilizado/expirado, replay de `PROVA`, `OLA` com outra chave,
  mensagens fora de ordem, subscrições/publicações proibidas, encaminhamento de
  `publish`, `verificarProva`, `removerReplica` e recarregamento de
  `replicas.txt`.
- `LeaderBotReplicacaoTest` — STORE para R réplicas, rejeição de ACK com hash
  errado, id diferente e sem STORE, `DURAVEL` com 2 ACK, nova ronda após timeout
  para a quarta réplica (falha propositada), 2 réplicas ⇒ `DURAVEL` e
  subreplicado, 1 réplica ⇒ `SELADO`, e HB com id alheio ignorado.
- `LeaderBotHubIntegracaoTest` — percurso completo líder + hub + três ligações
  de teste: OLA/PROVA/SUB, `HB` pelo hub, selagem, `STORE` publicado pelo hub,
  `ACK` pelo hub com o id autenticado e catálogo `DURAVEL`.

Na réplica: `WireTest` (formato), `ChavesTest` (persistência, permissões,
par incompleto/incoerente) e `ArmazemTest` (quota, hash, escrita atómica,
duplicado sem reescrita, `usados` recalculados no arranque, `.hdr` em falta,
`.bin` corrompido).

### 10.13. Limitações conhecidas

- **Sem reconciliação/manifesto/`DELETE`/`FETCH`/`POSSE?`/`ArquivoCheck`.** Ficam
  para o sprint seguinte. Consequências: um chunk cujas cópias fiquem em réplicas
  `MORTA` continua no catálogo; um chunk que fique `SELADO` por falta de
  candidatas não é reparado automaticamente; as cópias de uma réplica removida
  não são apagadas nem retiradas do catálogo.
- **As rondas pendentes vivem em memória.** Se o líder reiniciar, os `STORE`
  já publicados e os temporizadores perdem-se; o catálogo conserva os chunks
  `SELADO`/`DURAVEL` e a reconciliação do próximo sprint os repõe. `MAX_RONDAS`
  limita o esforço de uma execução.
- **`/config r` não altera R** neste sprint (provisório); o catálogo foi
  construído com o R do arranque.
- **Tokens em memória.** Reiniciar o líder invalida tokens não usados; é preciso
  `/replica-token` outra vez. O token é passado na linha de comandos da réplica e
  fica visível em `ps`/histórico; para os ensaios locais é aceitável, num ambiente
  real seria um segredo a proteger (ficheiro com permissões, variável de
  ambiente, etc.).
- **Uma chave nova para um id já registado é recusada.** Se a réplica perder
  `dados-<id>/` e gerar outro par, o `OLA` com chave diferente é recusado mesmo
  com um token novo; é preciso `/replica-remover <id>` antes. É a consequência
  deliberada de o `id` estar ligado à chave em `replicas.txt`.
- **`ws://` sem TLS.** Como o enunciado permite nos ensaios, o token, os `HB` e
  os chunks cifrados circulam em claro na rede local. Os chunks continuam
  cifrados (a confidencialidade do conteúdo depende da `master.key`), mas um
  observador vê o ritmo, os ids e os tamanhos; `wss://` resolveria a
  confidencialidade do token e dos metadados em trânsito.
- **`usados`/`quota` são reportados pela réplica.** Uma réplica maliciosa pode
  mentir no `HB`; sem desafios de posse (`POSSE?`, próximo sprint) o líder não
  prova que os bytes existem. O `ACK` prova que a réplica disse que gravou; a
  verificação de posse é que prova que ainda os tem.
- **Porto do hub ocupado.** O `WebSocketServer` faz bind de forma assíncrona;
  o arranque espera até 5 s por `onStart` e aborta com código 1 se o porto não
  abrir (para não ficar um bot online sem hub). O erro detalhado fica em
  `eventos-lider.txt`; verifique o porto com `ss -ltnp`.
- **`/estado` é limitado a 15 réplicas** na listagem (com `… e N outras`) para
  não exceder o limite de 2000 caracteres do Discord; os totais do catálogo são
  sempre completos.
- **`MORTA_MS = 10000`** é o valor fixado pelo E2-03 (5 heartbeats); o texto
  geral do Epico2 refere 30 s para o trabalho completo. O sprint atual
  sobrepõe-se e é este o valor usado no código e nos testes.
- **O líder é um ponto único de falha para escritas**, como no enunciado: sem
  ele não há LSN, selagem nem colocação. A recuperação total a partir das
  réplicas é o sprint seguinte.
