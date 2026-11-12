"""Demonstração do problema do "slow joiner" (assinante atrasado) no PUB/SUB.

O que acontece:
  1. o SUB chama connect() e setsockopt(SUBSCRIBE) -- ambos retornam na hora;
  2. mas a conexão TCP e o envio da assinatura ao PUB acontecem em segundo
     plano e levam alguns milissegundos;
  3. o PUB descarta mensagens para quem ainda não está (completamente)
     conectado e assinado -- ele não guarda histórico.
Logo, se o publisher começar a enviar imediatamente, as primeiras mensagens
se perdem, mesmo com o subscriber "já rodando".

Este script roda os dois lados no mesmo processo (via TCP em localhost):
  * cenário 1: publica logo após o connect  -> perde mensagens;
  * cenário 2: espera --espera segundos antes de publicar -> recebe todas.

Uso:
  python slow_joiner.py --mensagens 20 --espera 0.5
"""
import argparse
import time

import zmq


def cenario(context, mensagens, espera):
    pub = context.socket(zmq.PUB)
    porta = pub.bind_to_random_port("tcp://127.0.0.1")   # porta livre qualquer

    sub = context.socket(zmq.SUB)
    sub.connect(f"tcp://127.0.0.1:{porta}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "DEMO")

    time.sleep(espera)                       # 0 = publica imediatamente
    for i in range(1, mensagens + 1):
        pub.send_string(f"DEMO {i}")

    recebidas = []
    while sub.poll(500, zmq.POLLIN):         # coleta o que chegou (até 0,5 s de silêncio)
        recebidas.append(int(sub.recv_string().split()[1]))

    pub.close(linger=0)
    sub.close(linger=0)
    perdidas = [i for i in range(1, mensagens + 1) if i not in recebidas]
    print(f"espera={espera:.2f}s: recebidas {len(recebidas)}/{mensagens}; "
          f"perdidas: {perdidas if perdidas else 'nenhuma'}")


def main():
    ap = argparse.ArgumentParser(description="Mostra o slow joiner do PUB/SUB")
    ap.add_argument("--mensagens", type=int, default=20)
    ap.add_argument("--espera", type=float, default=0.5)
    a = ap.parse_args()

    context = zmq.Context()
    print("Cenário 1 - publica logo depois do connect/subscribe:")
    cenario(context, a.mensagens, 0.0)
    print("Cenário 2 - espera a assinatura se estabelecer antes de publicar:")
    cenario(context, a.mensagens, a.espera)
    context.term()


if __name__ == "__main__":
    main()
