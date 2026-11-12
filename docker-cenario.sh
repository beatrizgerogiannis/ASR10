#!/usr/bin/env bash
# Roda os três padrões entre as "máquinas" do docker-compose.yml, na mesma ordem
# de comandos do AWS.md. Cada programa roda no seu container; a comunicação
# atravessa a rede zmqnet (172.28.0.0/24).
#
# Uso: ./docker-cenario.sh          (sobe os containers, roda tudo e derruba no fim)
set -eu
cd "$(dirname "$0")"

DC="docker compose"
em()    { local m=$1 d=$2; shift 2; $DC exec -T -w "/app/$d" "$m" "$@"; }           # primeiro plano
fundo() { local m=$1 d=$2; shift 2; $DC exec -d -w "/app/$d" "$m" sh -c "$*"; }     # segundo plano
log()   { $DC exec -T "$1" cat "$2"; }
titulo(){ echo; echo "=================================================================="; echo "== $*"; echo "=================================================================="; }

$DC up -d --build --quiet-pull >/dev/null 2>&1
trap '$DC down -t 2 >/dev/null 2>&1' EXIT

titulo "Máquinas"
for m in maquina-a maquina-b maquina-c; do
  $DC exec -T "$m" sh -c 'echo "$(hostname)  IP $(hostname -i)  ZMQ_HOST=${ZMQ_HOST:-<não definido>}"'
done

# ---------------------------------------------------------------- REQ/REP
titulo "REQ/REP — servidor em A; cliente B (via ZMQ_HOST) e cliente C (via --host)"
fundo maquina-a reqrep "python server.py --name A > /tmp/server.log 2>&1"
sleep 1
echo "--- [maquina-b] python client.py --ports 4567,4568 --demo"
em maquina-b reqrep python client.py --ports 4567,4568 --demo
echo; echo "--- [maquina-c] python client.py --host 172.28.0.10 --ports 4568   (modo interativo)"
printf 'add 2 3\nupper ola docker\nreverse maquina-c\nsair\n' | em maquina-c reqrep python client.py --host 172.28.0.10 --ports 4568
echo; echo "--- [maquina-b] python client.py --demo --stop   (encerra o servidor)"
em maquina-b reqrep python client.py --demo --stop | tail -1
sleep 1
echo; echo "--- [maquina-a] log do servidor (\"de <IP>\" = IP do cliente que enviou)"
log maquina-a /tmp/server.log
echo; echo "--- [maquina-c] servidor fora do ar: python client.py --host 172.28.0.10 --demo --timeout 1000 --retries 2"
em maquina-c reqrep python client.py --host 172.28.0.10 --demo --timeout 1000 --retries 2

# ---------------------------------------------------------------- PUB/SUB
titulo "PUB/SUB — publisher em A; subscriber B (TIME,ALERTA) e C (TEMP)"
fundo maquina-b pubsub "python subscriber.py --topics TIME,ALERTA -n 8 > /tmp/sub.log 2>&1"
fundo maquina-c pubsub "python subscriber.py --host 172.28.0.10 --topics TEMP -n 8 > /tmp/sub.log 2>&1"
sleep 1
echo "--- [maquina-a] python publisher.py --intervalo 0.5 --limite 26 --rodadas 9"
em maquina-a pubsub python publisher.py --intervalo 0.5 --limite 26 --rodadas 9
sleep 1
echo; echo "--- [maquina-b] python subscriber.py --topics TIME,ALERTA -n 8"; log maquina-b /tmp/sub.log
echo; echo "--- [maquina-c] python subscriber.py --host 172.28.0.10 --topics TEMP -n 8"; log maquina-c /tmp/sub.log

titulo "PUB/SUB — slow joiner entre máquinas"
fundo maquina-b pubsub "python subscriber.py -n 0 > /tmp/sj.log 2>&1"
sleep 1
echo "--- [maquina-a] python publisher.py --intervalo 0 --rodadas 5 --quieto   (subscriber de B já rodando)"
em maquina-a pubsub python publisher.py --intervalo 0 --rodadas 5 --quieto
sleep 1; $DC exec -T maquina-b pkill -f subscriber.py || true; sleep 0.5
echo "--- [maquina-b] subscriber.py -n 0"; log maquina-b /tmp/sj.log

# ---------------------------------------------------------------- Pipeline
titulo "PIPELINE — fontes 1 e 2 + sink em A; workers em B e C"
lote() {   # roda em A: sink + duas fontes (mesmas sementes em todas as rodadas)
  em maquina-a pipeline sh -c '
    python sink.py --fontes 2 > /tmp/sink.log 2>&1 & S=$!
    python tasksrc.py 1 --tarefas 20 --seed 101 --espera 2 > /tmp/src1.log 2>&1 &
    python tasksrc.py 2 --tarefas 20 --seed 102 --espera 2 > /tmp/src2.log 2>&1 &
    wait $S; cat /tmp/src1.log /tmp/src2.log /tmp/sink.log'
}

fundo maquina-b pipeline "python taskwork.py 1 --fontes 2 > /tmp/w1.log 2>&1"
sleep 1
echo "--- RODADA COM 1 WORKER  [maquina-a] sink.py + tasksrc.py 1 + tasksrc.py 2"
lote

fundo maquina-c pipeline "python taskwork.py 2 --fontes 2 --src-hosts 172.28.0.10 --sink-host 172.28.0.10 > /tmp/w2.log 2>&1"
sleep 1
echo; echo "--- RODADA COM 2 WORKERS (B e C)"
lote | sed -n '/===== resumo/,$p'

fundo maquina-b pipeline "python taskwork.py 3 --fontes 2 > /tmp/w3.log 2>&1"
sleep 1
echo; echo "--- RODADA COM 3 WORKERS (1 e 3 em B, 2 em C)"
lote | sed -n '/===== resumo/,$p'

$DC exec -T maquina-b pkill -f taskwork.py || true
$DC exec -T maquina-c pkill -f taskwork.py || true
sleep 1
echo; echo "--- [maquina-b] worker 1 (início do log)"; log maquina-b /tmp/w1.log | head -6
echo "--- [maquina-c] worker 2 (início do log)"; log maquina-c /tmp/w2.log | head -6
