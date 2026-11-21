# tpf-lider · Sprint E2-02 (escrita local durável)

Bot Discord (JDA) que implementa o **caminho de escrita local do líder**: cada `/registar`,
cada mensagem em `#arquivo` e cada registo de `/rajada` recebe um LSN, é gravado em
`registos.log` com `fsync` **antes** de o utilizador receber a resposta, e entra no chunk
aberto — que é selado, cifrado e escrito em `selados/`.

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
  aberto.

Ainda **não** implementado (fica para os sprints seguintes): catálogo, manifesto, réplicas,
colocação e recuperação a partir das réplicas.
`/config`, `/recuperar`, `/replica-token` e `/replica-remover` mantêm a resposta provisória do
sprint anterior.

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

- `ARRANQUE` — `master.key` validada e `registos.log` recuperado (com o último LSN);
- `REPARACAO` — só se havia uma cauda parcial a reparar (ou uma reparação pendente);
- `REGISTO` — um por escrita durável, com o LSN, autor, canal e tamanho da linha;
- `SELADO` — um por chunk selado, com `chunkId`, intervalo de LSN e tamanho em claro;
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

Exemplo com tudo explícito:

```bash
java -Dtpf.guild=123456789012345678 \
     -Dtpf.masterKey=master.key \
     -Dtpf.pasta=. \
     -Dtpf.chunkMax=65536 -Dtpf.pad=4096 -Dtpf.selagem=5000 \
     -jar target/tpf-lider-1.0.jar
```

## 4. Ficheiros da pasta do líder

```
dados-lider/
├── registos.log        # diário de registos: fonte de verdade (texto, 1 linha por registo)
├── eventos-lider.txt   # diário de eventos do líder
├── reparacao.intent    # só existe DURANTE uma reparação (ver §5.2); se a encontrar antes
│                       # de arrancar, houve um crash a meio — o arranque conclui-a
└── selados/            # chunks cifrados: <chunkId>.bin + <chunkId>.hdr (§5.4)
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

**Princípio: validar antes de alterar.** O arranque nunca modifica um ficheiro antes de as suas
linhas completas estarem validadas. A ordem é: ler os bytes → validar as linhas de
`[0, fimValido)` → **só depois** cortar a cauda parcial. Se houver corrupção, o ficheiro fica
byte-a-byte como estava, incluindo a cauda parcial que havia para cortar. A regra contrária
(cortar primeiro, validar depois) deixaria marcas da nossa passagem num ficheiro que
decidimos recusar — e o operador já não poderia inspecionar o estado original.

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
  4096.
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
| `/estado` | todos | — | mostra último LSN, chunks selados e ocupação do chunk aberto |
| `/recuperar <de> <ate>` | todos | inteiros ≥ 1 e `de` ≤ `ate` | provisório |
| `/config r <n>` | `admin-arquivo` | inteiro ≥ 1 | provisório |
| `/replica-token` | `admin-arquivo` | — | provisório |
| `/replica-remover <id>` | `admin-arquivo` | letras, dígitos, `_` e `-`, até 32 caracteres | provisório |
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

## 8. Como verificar os critérios de aceitação

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

Testes automáticos (não precisam de Discord nem de rede):

```bash
mvn -q test
# o stderr com "ERRO CRÍTICO ..." e "AVISO: eventos-lider.txt: N bytes ..." é esperado:
# vem dos testes que simulam falhas de escrita e caudas parciais, não de falhas da suite.

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
    parcial também não é alterado;
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

Os testes de fail-stop usam um canal injetado (`LogRegistos.paraTestes` com uma implementação
de `LogRegistos.Canal` em memória), porque não há forma de provocar falhas de I/O
determinísticas num `FileChannel` real. O canal de teste permite escolher **quais** as chamadas
a `forcar()` falham, o que é o que distingue "rollback completo" de "truncagem ok, `force`
falhado".

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
  o chunk aberto mantém-se (não é descartado) e os registos continuam duráveis no log; o log
  permite reconstruir qualquer chunk. Um chunk **nunca** é a única cópia de um registo.
- **Selagem: o registo que provoca o limite fica no chunk.** O registo entra primeiro e só
  depois se compara com `CHUNK_MAX_BYTES`, pelo que nenhum registo é deixado de fora nem
  duplicado na fronteira. A selagem por tempo é armada no **primeiro** registo do chunk e o
  callback confirma que o chunk ainda é o mesmo antes de selar.
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
- **Validar antes de alterar.** Nenhum ficheiro é cortado — nem a cauda parcial do diário —
  antes de as suas linhas completas estarem validadas. Perante corrupção, o estado em disco é
  exatamente o que o operador deixou, para poder ser inspecionado.
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