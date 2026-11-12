"""Worker PULL -> PUSH (baseado em 04-24/taskwork.py).

Novidades:
  * conecta em N fontes (--fontes), cujos endereços vêm de --src-hosts / ZMQ_SRC_HOSTS;
  * lê JSON em vez de pickle;
  * depois de "trabalhar" (sleep proporcional à carga, como no original),
    envia o resultado ao sink (PUSH), dizendo qual worker e máquina fez a tarefa.

Uso:
  python taskwork.py 1 --fontes 2 --src-hosts 10.0.1.5 --sink-host 10.0.1.5
"""
import argparse
import json
import signal
import socket
import time

import zmq

from constPipe import SINK_HOST, SINK_PORT, SRC_HOSTS, porta_da_fonte


def meu_ip():
    """IP desta máquina/container (só para exibir nas mensagens)."""
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "?"


def recv_com_origem(s):
    """Recebe uma mensagem e devolve (texto, IP de quem a enviou).

    "Peer-Address" é uma propriedade do frame do ZeroMQ com o IP da outra ponta
    da conexão TCP: mostra de qual máquina a mensagem chegou pela rede.
    """
    frame = s.recv(copy=False)
    return frame.bytes.decode(), frame.get("Peer-Address")


def _sigterm(*_):
    """kill (SIGTERM) encerra do mesmo jeito limpo que Ctrl+C."""
    raise KeyboardInterrupt


def main():
    signal.signal(signal.SIGTERM, _sigterm)
    ap = argparse.ArgumentParser(description="Worker do pipeline (PULL das fontes, PUSH ao sink)")
    ap.add_argument("id", help="identificador deste worker")
    ap.add_argument("--fontes", type=int, default=2, help="número de fontes a conectar (padrão: %(default)s)")
    ap.add_argument("--src-hosts", default=SRC_HOSTS,
                    help="IP(s) das fontes, separados por vírgula; um só vale para todas (padrão: %(default)s)")
    ap.add_argument("--sink-host", default=SINK_HOST, help="IP/nome do sink (padrão: %(default)s)")
    ap.add_argument("--sink-port", default=SINK_PORT)
    ap.add_argument("--escala", type=float, default=0.01, help="segundos por unidade de carga (original: 0.01)")
    a = ap.parse_args()

    me = str(a.id)
    host = socket.gethostname()
    hosts = a.src_hosts.split(",")

    context = zmq.Context()
    r = context.socket(zmq.PULL)               # recebe tarefas (fair-queuing entre as fontes)
    for i in range(1, a.fontes + 1):
        h = hosts[i - 1] if i <= len(hosts) else hosts[-1]
        r.connect(f"tcp://{h}:{porta_da_fonte(i)}")
    sink = context.socket(zmq.PUSH)            # envia resultados
    sink.connect(f"tcp://{a.sink_host}:{a.sink_port}")
    print(f"worker {me} em {host} ({meu_ip()}) iniciado: {a.fontes} fonte(s) em {hosts}, sink em {a.sink_host}", flush=True)

    feitas = 0
    try:
        while True:
            texto, via = recv_com_origem(r)
            tarefa = json.loads(texto)
            print(f"worker {me} recebeu carga {tarefa['carga']:3} da fonte {tarefa['fonte']} "
                  f"(tarefa {tarefa['tarefa']}) via {via}", flush=True)
            time.sleep(tarefa["carga"] * a.escala)   # finge que trabalha
            feitas += 1
            sink.send_string(json.dumps({"tipo": "resultado", "worker": me, "host": host, **tarefa}))
    except KeyboardInterrupt:
        pass
    finally:
        print(f"worker {me} encerrado após {feitas} tarefas", flush=True)
        r.close(linger=0)
        sink.close(linger=1000)
        context.term()


if __name__ == "__main__":
    main()
