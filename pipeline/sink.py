"""Sink do pipeline (PULL) -- novo, não existe no original do livro.

Recebe:
  * de cada fonte, uma mensagem {"tipo": "inicio", "fonte", "tarefas"} -- assim
    o sink sabe quantas tarefas esperar e quando o lote começou;
  * de cada worker, uma mensagem {"tipo": "resultado", "worker", "fonte", ...}.

Quando todas as --fontes avisaram o início e todas as tarefas anunciadas
chegaram, imprime:
  * tarefas concluídas por worker e por fonte (e o cruzamento worker x fonte);
  * a carga somada por worker (quanto "trabalho" cada um fez);
  * o tempo total do lote (do primeiro "inicio" até o último resultado).
O tempo é medido só com o relógio do sink, então não depende de os relógios
das máquinas estarem sincronizados.

Uso:
  python sink.py --fontes 2
"""
import argparse
import json
import signal
import socket
import time
from collections import Counter

import zmq

from constPipe import SINK_PORT


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
    ap = argparse.ArgumentParser(description="Sink do pipeline: conta resultados e mede o tempo")
    ap.add_argument("--fontes", type=int, default=2, help="quantas fontes vão participar (padrão: %(default)s)")
    ap.add_argument("--port", default=SINK_PORT, help="porta do bind (padrão: %(default)s)")
    ap.add_argument("--verbose", action="store_true", help="imprime cada resultado recebido")
    a = ap.parse_args()

    context = zmq.Context()
    r = context.socket(zmq.PULL)
    r.bind(f"tcp://*:{a.port}")
    print(f"sink em {socket.gethostname()} ({meu_ip()}): tcp://*:{a.port}, aguardando {a.fontes} fonte(s)", flush=True)

    esperadas = {}                   # fonte -> nº de tarefas anunciadas
    por_worker = Counter()
    por_fonte = Counter()
    por_par = Counter()              # (worker, fonte) -> nº de tarefas
    carga_worker = Counter()
    host_worker = {}
    inicio = fim = None

    try:
        while True:
            texto, via = recv_com_origem(r)
            m = json.loads(texto)
            agora = time.time()
            if m["tipo"] == "inicio":
                esperadas[m["fonte"]] = m["tarefas"]
                inicio = inicio or agora
                print(f"  fonte {m['fonte']} ({m['host']}, {via}) começou: {m['tarefas']} tarefas", flush=True)
            else:
                w, f = m["worker"], m["fonte"]
                por_worker[w] += 1
                por_fonte[f] += 1
                por_par[w, f] += 1
                carga_worker[w] += m["carga"]
                host_worker[w] = f"{m['host']}, {via}"
                fim = agora
                if a.verbose:
                    print(f"  resultado: worker {w} fez tarefa {m['tarefa']} da fonte {f} (carga {m['carga']})")

            # terminou? todas as fontes anunciaram e todas as tarefas voltaram
            if len(esperadas) >= a.fontes and sum(por_fonte.values()) >= sum(esperadas.values()):
                break
    except KeyboardInterrupt:
        print("  (interrompido antes do fim do lote)")

    total = sum(por_worker.values())
    fontes = sorted(por_fonte)
    print("\n===== resumo do lote =====")
    print(f"tarefas concluídas: {total}")
    print("por worker:")
    for w in sorted(por_worker):
        cruz = ", ".join(f"fonte {f}: {por_par[w, f]}" for f in fontes)
        print(f"  worker {w} ({host_worker[w]}): {por_worker[w]:3} tarefas, carga {carga_worker[w]:5}  [{cruz}]")
    print("por fonte:")
    for f in fontes:
        print(f"  fonte {f}: {por_fonte[f]} de {esperadas.get(f, '?')}")
    if inicio and fim:
        print(f"TEMPO TOTAL DO LOTE: {fim - inicio:.2f} s com {len(por_worker)} worker(s)")

    r.close(linger=0)
    context.term()


if __name__ == "__main__":
    main()
