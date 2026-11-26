# tpf-replica · Sprint E2-03 (armazenamento cego e heartbeats)

Serviço que cede espaço ao arquivo distribuído do TPF. Liga-se ao **hub de
publish/subscribe** do líder por WebSocket, autentica-se com Ed25519, guarda os
chunks **cifrados** que lhe são enviados e confirma-os com `ACK` depois de os ter
em disco. A réplica **nunca** recebe nem guarda a `master.key`, o catálogo em
claro ou os registos em claro: só vê bytes opacos, o `chunkId` (hash do conteúdo
cifrado) e o cabeçalho público do chunk (`keyVersion;salt;nonce`).

Faz parte do E2-03. O líder está em [`../tpf-lider`](../tpf-lider/README.md).

## 1. Requisitos e compilação

Requisitos: **Java 17 ou superior** e **Maven**. Não há dependências de runtime
além do JDK (`java.net.http.WebSocket` e `java.security`); os testes usam JUnit 5.

```bash
cd tpf-replica
mvn -q package           # gera target/tpf-replica-1.0.jar
mvn -q test              # testes unitários
```

## 2. Arrancar

```
ReplicaService <id> <url> <quotaBytes> [token]
```

| Argumento | Exemplo | Significado |
| --- | --- | --- |
| `<id>` | `A` | identificador da réplica: letras, dígitos, `_` e `-`, até 32 caracteres |
| `<url>` | `ws://localhost:8765` | URL do hub (`ws://` ou `wss://`) |
| `<quotaBytes>` | `8388608` | espaço máximo em disco para `.bin` (8 MB no exemplo) |
| `[token]` | `kQ…` | token de uso único gerado por `/replica-token`; obrigatório no primeiro registo |

Com o jar:

```bash
java -jar target/tpf-replica-1.0.jar A ws://localhost:8765 8388608 <token>
```

Com o Maven:

```bash
mvn -q compile exec:java -Dexec.args="A ws://localhost:8765 8388608 <token>"
```

O `<token>` só é necessário no **primeiro** registo do `id` (ou se o líder tiver
perdido `replicas.txt`). Depois de a chave estar registada, basta:

```bash
java -jar target/tpf-replica-1.0.jar A ws://localhost:8765 8388608
```

O id do líder tem de ser `A` no `replicas.txt`; o token é entregue pelo
administrador a partir da resposta efémera de `/replica-token` no Discord. Não
ponha o token em ficheiros versionados.

### 2.1. Lançar quatro réplicas

O exemplo do enunciado usa quotas de 8, 2, 4 e 6 MB. O líder deve estar a correr
(§10.8 do README do líder). Gere quatro tokens, um por réplica, e arranque cada
uma numa pasta própria:

```bash
mkdir -p /tmp/tpf/A && cd /tmp/tpf/A
java -jar /caminho/para/tpf-replica/target/tpf-replica-1.0.jar A ws://localhost:8765 8388608 <tokenA>

mkdir -p /tmp/tpf/B && cd /tmp/tpf/B
java -jar /caminho/para/tpf-replica/target/tpf-replica-1.0.jar B ws://localhost:8765 2097152 <tokenB>

mkdir -p /tmp/tpf/C && cd /tmp/tpf/C
java -jar /caminho/para/tpf-replica/target/tpf-replica-1.0.jar C ws://localhost:8765 4194304 <tokenC>

mkdir -p /tmp/tpf/D && cd /tmp/tpf/D
java -jar /caminho/para/tpf-replica/target/tpf-replica-1.0.jar D ws://localhost:8765 6291456 <tokenD>
```

Também pode correr todas na mesma pasta: os ficheiros de dados e de eventos
incluem o `<id>`, pelo que não colidem.

### 2.2. Propriedades do sistema

| Propriedade | Omissão | Significado |
| --- | --- | --- |
| `tpf.periodo` | `2000` | `PERIODO_HB_MS`: intervalo entre heartbeats |
| `tpf.backoffMin` | `1000` | espera mínima de religação |
| `tpf.backoffMax` | `30000` | espera máxima de religação |

Devem coincidir com as do líder quando são alteradas:

```bash
java -Dtpf.periodo=2000 -Dtpf.backoffMin=1000 -Dtpf.backoffMax=30000 \
     -jar target/tpf-replica-1.0.jar A ws://localhost:8765 8388608 <token>
```

## 3. Ficheiros

O serviço cria na pasta onde arranca:

```
dados-<id>/
├── chave-privada.der     # Ed25519 em PKCS#8, permissões rw------- (só o dono)
├── chave-publica.der     # Ed25519 em X.509, permissões rw-------
├── <chunkId>.bin         # texto cifrado + tag GCM (nunca texto em claro)
└── <chunkId>.hdr         # keyVersion;salt;nonce (metadados públicos do chunk)

eventos-<id>.txt          # diário de eventos: instante;evento;detalhe
```

- O nome de cada `.bin` é o **SHA-256 do seu conteúdo**, pelo que qualquer pessoa
  pode verificar a integridade sem decifrar:
  `sha256sum dados-A/<chunkId>.bin` devolve `<chunkId>`.
- O par de chaves é criado no primeiro arranque. Se um dos ficheiros faltar ou não
  formar um par válido, a réplica **recusa arrancar** em vez de gerar uma
  identidade nova por cima da antiga (tal como o líder faz com a `master.key`).
  Para aceitar um registo novo, remova os dois ficheiros conscientemente.
- `usados` é sempre recalculado somando os `.bin` da pasta no arranque; a
  contabilidade em memória é atualizada a cada `STORE`.
- A quota é aplicada ao tamanho do `.bin` (texto cifrado + 16 bytes de tag); o
  `.hdr` é pequeno e não entra na conta.

## 4. Comportamento

### 4.1. Ligação, registo e autenticação

1. No arranque, lê ou cria o par Ed25519 e calcula `usados` somando os `.bin`.
2. Abre `java.net.http.WebSocket` para o URL indicado.
3. Ao abrir, envia:
   - `OLA;<id>;<chavePublicaB64>;<token>` se o token foi passado;
   - `OLA;<id>` caso contrário.
4. O hub responde com `DESAFIO;<nonceB64>` (32 bytes de `SecureRandom`, novo por
   ligação). A réplica assina `tpf-registo|<id>|<nonceB64>` em UTF-8 com a chave
   privada e envia `PROVA;<id>;<assinaturaB64>`.
5. Em seguida subscreve `tpf-r-<id>` e `tpf-ctl`, publica o `INVENTARIO` completo
   (`PUB;tpf-lider;INVENTARIO;<id>;<chunkId1>,...`) e um `HB` inicial.
6. A partir daí publica `PUB;tpf-lider;HB;<id>;<usados>;<quota>` a cada
   `PERIODO_HB_MS`.

Se a ligação cair (erro, fecho, líder em baixo), volta a ligar com **backoff
exponencial**: 1 s, 2 s, 4 s, 8 s, 16 s, 30 s, 30 s, … O atraso volta a 1 s
depois de uma ligação bem-sucedida (`onOpen`). Os intervalos ficam registados
como `RELIGAR <ms>` em `eventos-<id>.txt`.

### 4.2. `STORE`

O líder envia `PUB;tpf-r-<id>;STORE;<chunkId>;<hdrB64>;<binB64>`. A réplica:

1. descodifica o Base64;
2. calcula o SHA-256 do `.bin`; se for diferente de `chunkId`, recusa e regista
   `ERRO` (não grava);
3. se `<chunkId>.bin` **já existe** e o seu conteúdo continua a ter SHA-256 igual
   ao `chunkId`, responde `OK` **sem reescrever** e sem passar pela quota — um
   duplicado é um `ACK` idempotente e não consome espaço novo (mesmo com a quota
   cheia). Se faltar o `.hdr`, completa-o; se o `.bin` existente estiver
   corrompido, recusa com `HASH`;
4. só para um chunk **novo**, se `usados + tamanho > quota`, recusa e regista
   `QUOTA` (não responde `ACK`; o líder expira o prazo e tenta outra réplica);
5. grava `<chunkId>.hdr` e `<chunkId>.bin` com ficheiro temporário +
   `ATOMIC_MOVE`, `force(true)` e `fsync` ao diretório; atualiza `usados`;
6. **só depois** publica `PUB;tpf-lider;ACK;<id>;<chunkId>;<sha256>`.

Um `STORE` de um chunk que já existe responde `ACK` **sem reescrever** os
ficheiros (o `chunkId` é o hash do conteúdo, logo o chunk é imutável). Se o
`.bin` existir mas faltar o `.hdr` (crash entre os dois moves), o `.hdr` é
completado e o `.bin` não é tocado. Se o `.bin` existente já não corresponder ao
`chunkId`, a réplica deteta a corrupção e não confirma. Um duplicado registra
`STORE` («já existia»), nunca `QUOTA`.

### 4.3. Eventos

`eventos-<id>.txt` tem uma linha por evento, em UTF-8:

```
instante;evento;detalhe\n
```

| Evento | Quando | Exemplo de detalhe |
| --- | --- | --- |
| `ARRANQUE` | arranque do processo | `id=A url=ws://localhost:8765 quota=8388608 chaves=criadas usados=0 chunks=0` |
| `LIGADO` | ligação WebSocket aberta | `ws://localhost:8765` |
| `DESLIGADO` | ligação fechada | `ligação fechada (1006)` ou `shutdown` |
| `RELIGAR` | próxima tentativa agendada | `1000` (milissegundos) |
| `STORE` | chunk gravado (ou já existente) | `chunkId=… bytes=8208 usados=8208` |
| `QUOTA` | `STORE` recusado por falta de espaço | `chunkId=… bytes=… usados=… quota=…` |
| `ERRO` | hash errado, Base64 inválido, falha de I/O ou de ligação | `INTEGRIDADE: …` |

O token **nunca** é escrito no diário.

## 5. O que a réplica vê e não vê

| Vê | Não vê |
| --- | --- |
| `chunkId` (SHA-256 do texto cifrado) | o conteúdo dos registos, autores e canais |
| `keyVersion`, `salt`, `nonce` (do `.hdr`) | a `master.key` e a DEK |
| o tamanho do `.bin` e o instante em que o recebeu | os LSN e o número de registos do chunk |
| as mensagens do seu tópico e de `tpf-ctl` | o catálogo em claro e o manifesto |
| o seu próprio `usados`/`quota` | que outras réplicas têm o mesmo chunk |

A réplica consegue verificar a integridade (o nome é o hash) e a autenticidade
(o líder verifica o GCM), mas não consegue decifrar. Alterar um byte do `.bin`
muda o hash e é detetado; trocar dois chunks válidos entre si mantém cada hash
válido, mas o líder deteta-o pelo `chunkId` pedido (no sprint seguinte, também
pelos desafios de posse).

## 6. Concorrência

A réplica tem quatro contextos:

- a thread do `java.net.http.WebSocket`, que entrega as tramas ao `Ouvinte`;
- o `processador` (uma thread), que processa as mensagens por ordem — `DESAFIO`
  antes das `SUB`/`PUB`, e cada `STORE` de forma atómica;
- a `SessaoEnvio` (uma thread), que serializa `sendText` (o JDK só permite um
  envio pendente por ligação) e preserva a ordem `PROVA`, `SUB`, `PUB`, `ACK`;
- o `agendador` (uma thread), que publica heartbeats e agenda religações.

`Armazem` e `EventosReplica` são `synchronized`; o `Armazem` só é chamado pelo
`processador`, mas a sincronização protege também os testes. A fila de envio é
uma `LinkedBlockingQueue`; não há escritas concorrentes no mesmo ficheiro. O
`Armazem` atualiza `usados` e `escritas` dentro do mesmo lock, pelo que um `ACK`
nunca observa um estado intermédio.

## 7. Testes

```bash
mvn -q test
grep -h "Tests run" target/surefire-reports/*.txt
```

- `WireTest` — round-trip de OLA/PROVA/SUB/PUB/HB/INVENTARIO/ACK/STORE e rejeição
  de mensagens malformadas.
- `ChavesTest` — criação e recarregamento do par, assinatura verificável,
  permissões `rw-------`, par incompleto, par incoerente e ficheiro corrompido.
- `ArmazemTest` — gravação de `.bin`/`.hdr`, nome = SHA-256, recusa por quota,
  recusa por hash errado, `STORE` duplicado sem reescrita, `usados` recalculados
  no arranque, `.hdr` em falta e `.bin` corrompido.

Os testes não abrem sockets nem precisam do líder; a interação real foi
verificada manualmente com o hub e está descrita no README do líder (§10).

## 8. Limitações e decisões

- **Fora do âmbito deste sprint:** `FETCH`, `DELETE`, `MANIFESTO`/`MANIFESTO?`,
  `POSSE?` e a resposta `DATA`. A réplica reconhece o tópico `tpf-ctl` mas ainda
  não age sobre ele; essas mensagens chegam no sprint seguinte. Também não há
  reconciliação nem recuperação a partir das réplicas.
- **Sem TLS nos ensaios (`ws://`).** O conteúdo dos chunks continua cifrado; um
  observador da rede vê o token, os ids, os tamanhos e o ritmo. `wss://` é
  suportado pelo JDK e pode ser usado sem alterar o código.
- **O token vai na linha de comandos.** Fica visível em `ps`/histórico; nos
  ensaios locais é aceitável. O token é de uso único e expira em 10 minutos; se
  for preciso um ambiente mais exigente, passe-o por um ficheiro com permissões
  restritas e adapte o arranque.
- **A quota é uma promessa local.** `usados` é a soma dos `.bin`; se um ficheiro
  for apagado por fora, a réplica só o deteta no arranque seguinte (ou quando
  fizer o inventário). Uma réplica maliciosa pode mentir no `HB`; os desafios de
  posse do sprint seguinte são o que prova que os bytes existem.
- **Um `STORE` recusado não envia `ACK` negativo.** O líder não tem mensagem de
  recusa no protocolo; expira `ACK_TIMEOUT_MS` e tenta outra réplica, como previsto
  no enunciado.
- **Chaves e dados em pastas relativas.** Arrancar a réplica noutra pasta cria uma
  identidade nova; para manter a mesma, arranque sempre na mesma pasta ou copie
  `dados-<id>/` (sobretudo as chaves). Se as chaves forem perdidas ou
  regeneradas, o `id` já registado continua associado à chave antiga: é preciso
  `/replica-remover <id>` no líder antes de registar a chave nova (mesmo com um
  token novo, um `OLA` com chave diferente é recusado).
