#!/bin/bash
# Verifica os critérios de chunk do E2-02 sobre a pasta de dados do líder.
# Uso: ./verificar-chunks.sh [pasta-de-dados] [PAD_BYTES]
set -u
cd "$(dirname "$0")"
DADOS=${1:-dados-lider}
PAD=${2:-4096}
LOG="$DADOS/registos.log"; EV="$DADOS/eventos-lider.txt"; SEL="$DADOS/selados"
falhas=0
erro() { echo "ERRO: $*"; falhas=$((falhas + 1)); }

echo "== 1. um SELADO por chunk =="
n_ev=$(grep -c ';SELADO;' "$EV")
n_bin=$(ls "$SEL"/*.bin 2>/dev/null | wc -l)
n_hdr=$(ls "$SEL"/*.hdr 2>/dev/null | wc -l)
echo "SELADO=$n_ev  .bin=$n_bin  .hdr=$n_hdr"
[ "$n_ev" -eq "$n_bin" ] || erro "nº de .bin diferente do nº de SELADO"
[ "$n_bin" -eq "$n_hdr" ] || erro "nº de .hdr diferente do nº de .bin"

echo "== 2. intervalos contíguos e bytes múltiplo de $PAD =="
esperado=1
while read -r de ate bytes; do
  [ "$de" -eq "$esperado" ] || erro "intervalo não contíguo: esperava $esperado, veio $de"
  [ "$ate" -ge "$de" ] || erro "intervalo inválido: $de-$ate"
  esperado=$((ate + 1))
  [ $((bytes % PAD)) -eq 0 ] || erro "bytes=$bytes não é múltiplo de $PAD"
  [ "$bytes" -eq $(( $(grep -c . /dev/null) + bytes )) ] 2>/dev/null
done < <(grep ';SELADO;' "$EV" \
         | sed -E 's/.*lsn=([0-9]+)-([0-9]+).*bytes=([0-9]+).*/\1 \2 \3/')

echo "== 3. nome do .bin = SHA-256 do conteúdo =="
for f in "$SEL"/*.bin; do
  [ -e "$f" ] || continue
  [ "$(basename "$f" .bin)" = "$(sha256sum "$f" | cut -d' ' -f1)" ] \
    || erro "$(basename "$f"): nome ≠ sha256 do conteúdo"
done

echo "== 4. .hdr = keyVersion;salt;nonce =="
for f in "$SEL"/*.hdr; do
  [ -e "$f" ] || continue
  awk -F';' 'NF != 3 {exit 1}' "$f" || erro "$(basename "$f"): formato inválido"
done

echo "== 5. nenhum texto registado em claro =="
if grep -rq "rajada " "$SEL"; then erro "há texto 'rajada ' dentro de selados/"; else echo "ok"; fi

echo "== 6. registos.log sem saltos e coberto pelos SELADO =="
awk -F';' 'NR>1 && $1 != prev+1 {print "salto na linha " NR} {prev=$1}' "$LOG"
ultimo=$(tail -n 1 "$LOG" | cut -d';' -f1)
echo "último LSN do log = $ultimo"
[ "$esperado" -eq $((ultimo + 1)) ] || erro "os SELADO não cobrem 1..$ultimo"

echo
[ "$falhas" -eq 0 ] && echo "RESULTADO: OK (0 falhas)" || echo "RESULTADO: $falhas falha(s)"
exit $((falhas > 0))
