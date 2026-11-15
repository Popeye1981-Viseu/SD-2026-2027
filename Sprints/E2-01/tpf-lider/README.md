# tpf-lider · Sprint 1 (base do líder)

Bot Discord (JDA) que regista os 7 slash commands do TPF e responde com mensagens provisórias.
Nenhuma funcionalidade do arquivo está implementada neste sprint.

## 1. Configurar o Discord (uma vez)

1. Em <https://discord.com/developers/applications>, crie a aplicação (**New Application**) e abra o separador **Bot**.
2. Em **Privileged Gateway Intents**, ative **Message Content Intent** e guarde.
3. Carregue em **Reset Token** e copie o token. **Não o ponha no código nem no repositório.**
4. Crie o servidor de teste do grupo, o canal `#arquivo` e o papel `admin-arquivo`. Atribua o papel aos membros do grupo.
5. Convide o bot. Substitua `<APPLICATION_ID>` pelo id que aparece em **General Information**:

   ```
   https://discord.com/oauth2/authorize?client_id=<APPLICATION_ID>&scope=bot%20applications.commands&permissions=68608
   ```

   `68608` corresponde a **View Channels**, **Send Messages** e **Read Message History**. Esta última é necessária para responder às mensagens de `#arquivo`.
6. Copie o id do servidor: em **Definições do utilizador → Avançado**, ative o **Modo de programador**, clique com o botão direito no servidor e escolha **Copiar ID do servidor**.

## 2. Configurar o token

Escolha uma das opções:

- **Variável de ambiente** (recomendado):
  ```bash
  export DISCORD_TOKEN='o-token-do-bot'
  ```
- **Ficheiro** `discord.token` na pasta onde arranca o líder, com o token numa só linha. O ficheiro está no `.gitignore`. Para usar outro caminho, passe `-Dtpf.tokenFile=<caminho>`.

## 3. Arrancar o líder

Requisitos: Java 17 ou superior e Maven.

```bash
cd tpf-lider
mvn -q compile exec:java -Dtpf.guild=<ID_DO_SERVIDOR>
```

Em alternativa, gere um jar executável:

```bash
mvn -q package
java -Dtpf.guild=<ID_DO_SERVIDOR> -jar target/tpf-lider-1.0.jar
```

O id do servidor também pode vir da variável `TPF_GUILD_ID`. Ao arrancar, o líder escreve na consola `ARRANQUE` e `COMANDOS` (7 comandos registados). Os comandos são registados no servidor (guild commands), por isso aparecem de imediato no autocomplete.

## Comandos

| Comando | Quem pode usar | Validação |
| --- | --- | --- |
| `/registar <texto>` | todos | texto não vazio |
| `/estado` | todos | — |
| `/recuperar <de> <ate>` | todos | inteiros ≥ 1 e `de` ≤ `ate` |
| `/config r <n>` | `admin-arquivo` | inteiro ≥ 1 |
| `/replica-token` | `admin-arquivo` | — |
| `/replica-remover <id>` | `admin-arquivo` | letras, dígitos, `_` e `-`, até 32 caracteres |
| `/rajada <n> <intervalo_ms>` | `admin-arquivo` | `n` ≥ 1 e `intervalo_ms` ≥ 0 |

O Discord só aceita nomes de parâmetros em minúsculas, por isso o `<intervaloMs>` do enunciado aparece como `intervalo_ms`.
