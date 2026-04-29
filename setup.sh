#!/usr/bin/env bash

DATA_DIR="data"

if [ ! -d "$DATA_DIR" ]; then
  echo "No existe la carpeta $DATA_DIR"
  exit 1
fi

for archivo in "$DATA_DIR"/*; do
  if [ -f "$archivo" ]; then
    nombre=$(basename "$archivo")

    if [[ "$nombre" == RENTAS* ]]; then
      echo "Procesando $archivo"
      uv run main.py stage -n "$archivo" -d 
    else
      echo "Procesando: $archivo"
      python main.py -f "$archivo"
    fi
  fi
done