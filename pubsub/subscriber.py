"""Subscriber PUB/SUB estendido (baseado em 04-23/subscriber.py).

Novidades:
  * endereço do publisher por --host / ZMQ_HOST (padrão: constPS.py);
  * escolha dos tópicos: --topics TIME,ALERTA (vazio = todos);
  * número de mensagens: -n 5 (como o original); -n 0 = infinito;
  * mostra a máquina de origem e avisa quando há "buracos" na sequência
    (mensagens que o publisher enviou e este subscriber não recebeu).

Uso:
  python subscriber.py --host 10.0.1.5 --topics TEMP,ALERTA -n 0
"""
import argparse
import json
import signal
import socket

import zmq

from constPS import HOST, PORT


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
    ap = argparse.ArgumentParser(description="Subscriber com escolha de tópicos")
    ap.add_argument("--host", default=HOST, help="IP/nome do publisher (padrão: %(default)s)")
    ap.add_argument("--port", default=PORT, help="porta (padrão: %(default)s)")
    ap.add_argument("--topics", default="", help="tópicos separados por vírgula; vazio = todos")
    ap.add_argument("-n", type=int, default=5, help="mensagens a receber; 0 = infinito (padrão: %(default)s)")
    a = ap.parse_args()

    context = zmq.Context()
    s = context.socket(zmq.SUB)
    s.connect(f"tcp://{a.host}:{a.port}")

    topicos = [t.strip() for t in a.topics.split(",") if t.strip()] or [""]
    for t in topicos:
        # O filtro é por PREFIXO da mensagem. "" assina tudo.
        # Usamos "TÓPICO " (com espaço) para "TEMP" não casar com "TEMPO", por exemplo.
        s.setsockopt_string(zmq.SUBSCRIBE, t + " " if t else "")
    print(f"subscriber em {socket.gethostname()} ({meu_ip()}) -> tcp://{a.host}:{a.port} "
          f"tópicos={topicos if topicos != [''] else 'TODOS'} n={a.n or 'infinito'}", flush=True)

    ultimo = {}                     # último seq visto por tópico
    recebidas = 0
    try:
        while a.n == 0 or recebidas < a.n:
            texto, via = recv_com_origem(s)
            topico, corpo = texto.split(" ", 1)
            dados = json.loads(corpo)
            recebidas += 1

            seq = dados["seq"]
            aviso = ""
            if topico not in ultimo and seq > 1:
                aviso = f"   (entrou atrasado: {seq - 1} msgs de {topico} anteriores não chegaram)"
            elif topico in ultimo and seq != ultimo[topico] + 1:
                aviso = f"   (PERDIDAS {seq - ultimo[topico] - 1} msgs de {topico})"
            ultimo[topico] = seq

            extra = {k: v for k, v in dados.items() if k not in ("origem", "seq", "ts")}
            print(f"[{recebidas:3}] {topico:<6} de {dados['origem']} via {via} seq={seq} {extra}{aviso}", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        print(f"subscriber encerrado após {recebidas} mensagens", flush=True)
        s.close(linger=0)
        context.term()


if __name__ == "__main__":
    main()
