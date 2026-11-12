"""Publisher PUB/SUB estendido (baseado em 04-23/publisher.py).

Novidades:
  * bind em tcp://*:<porta> (aceita subscribers de outras máquinas);
  * vários tópicos a cada rodada:
        TIME    hora atual (como no original)
        TEMP    "temperatura" aleatória (passeio aleatório entre 15 e 45 °C)
        ALERTA  só é publicado quando TEMP passa de --limite
  * cada mensagem é UMA string "<TÓPICO> <JSON>": o filtro do SUB compara
    prefixos, então o tópico precisa vir no começo;
  * o JSON leva a máquina de origem, um número de sequência (para o subscriber
    perceber mensagens perdidas) e o instante de envio;
  * intervalo configurável (--intervalo) e número de rodadas (--rodadas, 0 = infinito).

Uso:
  python publisher.py --intervalo 1 --limite 30
"""
import argparse
import json
import random
import signal
import socket
import time

import zmq

from constPS import PORT


def meu_ip():
    """IP desta máquina/container (só para exibir nas mensagens)."""
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "?"


def _sigterm(*_):
    """kill (SIGTERM) encerra do mesmo jeito limpo que Ctrl+C."""
    raise KeyboardInterrupt


def main():
    signal.signal(signal.SIGTERM, _sigterm)
    ap = argparse.ArgumentParser(description="Publisher com tópicos TIME, TEMP e ALERTA")
    ap.add_argument("--port", default=PORT, help="porta do bind (padrão: %(default)s)")
    ap.add_argument("--intervalo", type=float, default=1.0, help="segundos entre rodadas (padrão: %(default)s)")
    ap.add_argument("--limite", type=float, default=30.0, help="TEMP acima disto gera ALERTA (padrão: %(default)s)")
    ap.add_argument("--rodadas", type=int, default=0, help="número de rodadas; 0 = infinito")
    ap.add_argument("--quieto", action="store_true", help="não imprime cada mensagem publicada")
    a = ap.parse_args()

    origem = socket.gethostname()
    context = zmq.Context()
    s = context.socket(zmq.PUB)
    s.bind(f"tcp://*:{a.port}")
    print(f"[{origem}] ({meu_ip()}) publicando em tcp://*:{a.port} a cada {a.intervalo}s "
          f"(ALERTA se TEMP > {a.limite})", flush=True)

    seq = {"TIME": 0, "TEMP": 0, "ALERTA": 0}   # uma sequência por tópico
    temp = 25.0

    def publicar(topico, dados):
        seq[topico] += 1
        corpo = {"origem": origem, "seq": seq[topico], "ts": time.time(), **dados}
        msg = f"{topico} {json.dumps(corpo, ensure_ascii=False)}"
        s.send_string(msg)                      # PUB nunca bloqueia: sem assinante, a mensagem é descartada
        if not a.quieto:
            print("  pub:", msg, flush=True)

    rodada = 0
    try:
        while a.rodadas == 0 or rodada < a.rodadas:
            time.sleep(a.intervalo)
            publicar("TIME", {"hora": time.asctime()})
            temp = min(45.0, max(15.0, temp + random.uniform(-3, 3)))
            publicar("TEMP", {"valor": round(temp, 1), "unidade": "C"})
            if temp > a.limite:
                publicar("ALERTA", {"msg": f"temperatura {temp:.1f} C acima de {a.limite}",
                                    "valor": round(temp, 1)})
            rodada += 1
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[{origem}] publisher encerrado após {rodada} rodadas; enviados {seq}", flush=True)
        s.close(linger=1000)            # dá tempo de as últimas mensagens saírem
        context.term()


if __name__ == "__main__":
    main()
