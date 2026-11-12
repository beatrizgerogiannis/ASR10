#!/usr/bin/env bash
# Roda o mesmo lote de tarefas com 1, 2 e 3 workers (tudo local) e compara o tempo total.
# As cargas são iguais em todas as rodadas porque cada fonte usa uma semente fixa.
#
# Uso: ./compara_workers.sh [tarefas_por_fonte=20] [fontes=2] [max_workers=3]
set -u
cd "$(dirname "$0")"
PY=../.venv/bin/python; [ -x "$PY" ] || PY=python3

TAREFAS=${1:-20}; FONTES=${2:-2}; MAXW=${3:-3}
LOG=$(mktemp -d)
echo "lote: $FONTES fonte(s) x $TAREFAS tarefas; logs em $LOG"

for W in $(seq 1 "$MAXW"); do
  echo; echo "########## $W worker(s) ##########"
  $PY -u sink.py --fontes "$FONTES" > "$LOG/sink_$W.log" 2>&1 &
  SINK=$!
  WPIDS=()
  for i in $(seq 1 "$W"); do
    $PY -u taskwork.py "$i" --fontes "$FONTES" > "$LOG/worker_${W}_$i.log" 2>&1 &
    WPIDS+=($!)
  done
  for f in $(seq 1 "$FONTES"); do
    $PY -u tasksrc.py "$f" --tarefas "$TAREFAS" --seed $((100 + f)) --espera 1 > "$LOG/src_${W}_$f.log" 2>&1 &
  done
  wait "$SINK"                                  # o sink termina quando o lote acaba
  kill "${WPIDS[@]}" 2>/dev/null; wait 2>/dev/null
  sed -n '/===== resumo/,$p' "$LOG/sink_$W.log"
done

echo; echo "########## comparação ##########"
grep -h "TEMPO TOTAL" "$LOG"/sink_*.log
