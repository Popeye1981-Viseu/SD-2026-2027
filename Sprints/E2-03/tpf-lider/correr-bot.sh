#!/bin/bash
# Limite de chunk reduzido para que /rajada 500 0 produza vários chunks por TAMANHO
# (com o valor por omissão, 64 KiB, os 500 registos cabem todos num único chunk).
set -e
cd "$(dirname "$0")"
mvn -q compile exec:java \
  -Dtpf.guild=1550233395143901226 \
  -Dtpf.chunkMax=4096
