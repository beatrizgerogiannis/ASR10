"""Fonte de tarefas PUSH (baseado em 04-24/tasksrc.py).

Novidades:
  * bind em tcp://*:<porta da fonte> (porta = 4570 + id, ver constPipe.py);
  * mensagens em JSON em vez de pickle (pickle.loads em dados da rede permite
    executar código arbitrário -- ver README);
  * número de tarefas configurável (--tarefas) e semente opcional (--seed) para
    gerar sempre as mesmas cargas e comparar execuções;
  * avisa o sink (PUSH -> PULL) quantas tarefas vai mandar, para ele saber
    quando o lote terminou e medir o tempo total;
  * espera --espera segundos (ou ENTER com --enter) antes de distribuir, para
    os workers terem tempo de conectar. Sem isso, o primeiro worker a conectar
    recebe tudo (o PUSH só faz round-robin entre quem JÁ está conectado).

Uso:
  python tasksrc.py 1 --tarefas 20 --sink-host 10.0.1.5
"""
import argparse
import json
import random
import socket
import time

import zmq

from constPipe import SINK_HOST, SINK_PORT, porta_da_fonte


def meu_ip():
    """IP desta máquina/container (só para exibir nas mensagens)."""
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "?"


def main():
    ap = argparse.ArgumentParser(description="Fonte de tarefas (PUSH)")
    ap.add_argument("id", help="número desta fonte (1, 2, 3...)")
    ap.add_argument("--tarefas", type=int, default=20, help="tarefas a gerar (original: 100)")
    ap.add_argument("--seed", type=int, default=None, help="semente do gerador aleatório")
    ap.add_argument("--intervalo", type=float, default=0.0, help="pausa entre envios (original: 1 s)")
    ap.add_argument("--espera", type=float, default=2.0, help="segundos esperando workers conectarem")
    ap.add_argument("--enter", action="store_true", help="espera ENTER em vez de --espera")
    ap.add_argument("--sink-host", default=SINK_HOST, help="IP/nome do sink (padrão: %(default)s)")
    ap.add_argument("--sink-port", default=SINK_PORT)
    a = ap.parse_args()

    me = str(a.id)
    porta = porta_da_fonte(me)
    rnd = random.Random(a.seed)

    context = zmq.Context()
    s = context.socket(zmq.PUSH)               # distribui tarefas aos workers
    s.bind(f"tcp://*:{porta}")
    sink = context.socket(zmq.PUSH)            # canal de controle para o sink
    sink.connect(f"tcp://{a.sink_host}:{a.sink_port}")
    print(f"fonte {me} em {socket.gethostname()} ({meu_ip()}): tcp://*:{porta}, sink em {a.sink_host}:{a.sink_port}", flush=True)

    if a.enter:
        input("pressione ENTER quando os workers estiverem prontos... ")
    else:
        time.sleep(a.espera)

    # Avisa o sink: o relógio do lote começa aqui
    sink.send_string(json.dumps({"tipo": "inicio", "fonte": me, "tarefas": a.tarefas,
                                 "host": socket.gethostname()}))
    total = 0
    for i in range(1, a.tarefas + 1):
        carga = rnd.randint(1, 100)            # mesma faixa do original
        total += carga
        s.send_string(json.dumps({"fonte": me, "tarefa": i, "carga": carga}))
        if a.intervalo:
            time.sleep(a.intervalo)
    print(f"fonte {me}: {a.tarefas} tarefas enviadas (carga total {total})", flush=True)

    # linger padrão (-1): close/term esperam as mensagens pendentes serem entregues
    s.close()
    sink.close()
    context.term()


if __name__ == "__main__":
    main()
