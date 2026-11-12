"""Servidor REQ/REP estendido (baseado em 04-22/server.py).

Novidades em relação ao original:
  * faz bind em tcp://*:<porta>, aceitando conexões de outras máquinas;
  * é um "serviço" com várias operações (calculadora e texto), mensagens em JSON;
  * erros (operação desconhecida, argumentos inválidos, divisão por zero) viram
    uma resposta {"ok": false, "error": ...} em vez de derrubar o servidor;
  * cada resposta diz qual instância (máquina/pid) e qual porta atendeu;
  * continua aceitando texto puro como o original ("Hello world" -> "Hello world*")
    e o comando STOP.

Uso:
  python server.py                       # portas 4567 e 4568 (constCS.py)
  python server.py --ports 4567,4568 --name A --delay 0
"""
import argparse
import json
import os
import signal
import socket
import time

import zmq

from constCS import PORT1, PORT2


# ---------------------------------------------------------------- operações
def _numeros(args, minimo=2):
    """Converte os argumentos para números; lança ValueError se não der."""
    if len(args) < minimo:
        raise ValueError(f"são necessários pelo menos {minimo} argumentos numéricos")
    numeros = []
    for a in args:
        if isinstance(a, (int, float)) and not isinstance(a, bool):
            numeros.append(a)
            continue
        try:                                # aceita também "3" ou "2.5" como texto
            numeros.append(int(a) if str(a).lstrip("-").isdigit() else float(a))
        except (TypeError, ValueError):
            raise ValueError(f"argumento não numérico: {a!r}")
    return numeros


def _texto(args):
    """Junta os argumentos em uma única string."""
    if not args:
        raise ValueError("é necessário pelo menos um argumento de texto")
    return " ".join(str(a) for a in args)


def op_add(args):
    return sum(_numeros(args))


def op_sub(args):
    n = _numeros(args)
    return n[0] - sum(n[1:])


def op_mul(args):
    resultado = 1
    for x in _numeros(args):
        resultado *= x
    return resultado


def op_div(args):
    n = _numeros(args)
    resultado = n[0]
    for x in n[1:]:
        if x == 0:
            raise ZeroDivisionError("divisão por zero")
        resultado /= x
    return resultado


OPERACOES = {
    "add":     op_add,
    "sub":     op_sub,
    "mul":     op_mul,
    "div":     op_div,
    "upper":   lambda args: _texto(args).upper(),
    "lower":   lambda args: _texto(args).lower(),
    "reverse": lambda args: _texto(args)[::-1],
    "len":     lambda args: len(_texto(args)),
    "time":    lambda args: time.asctime(),
}


def atender(texto, info):
    """Processa uma requisição (str) e devolve (resposta_str, deve_parar)."""
    # Compatibilidade com o original: texto que não é JSON
    try:
        req = json.loads(texto)
    except json.JSONDecodeError:
        if "STOP" in texto:
            return "STOP recebido", True
        return texto + "*", False          # mesmo comportamento do livro

    resp = dict(info)                      # quem atendeu (servidor, porta, pid)
    if not isinstance(req, dict) or "op" not in req:
        resp.update(ok=False, error='formato esperado: {"op": ..., "args": [...]}')
        return json.dumps(resp, ensure_ascii=False), False

    op = str(req["op"]).lower()
    args = req.get("args", [])
    if not isinstance(args, list):
        args = [args]

    if op == "stop":
        resp.update(ok=True, result="servidor encerrando")
        return json.dumps(resp, ensure_ascii=False), True
    if op == "ops":
        resp.update(ok=True, result=sorted(OPERACOES) + ["ops", "stop"])
        return json.dumps(resp, ensure_ascii=False), False

    funcao = OPERACOES.get(op)
    if funcao is None:
        resp.update(ok=False, error=f"operação desconhecida: {op!r}")
        return json.dumps(resp, ensure_ascii=False), False
    try:
        resp.update(ok=True, result=funcao(args))
    except (ValueError, ZeroDivisionError) as e:   # erro do cliente, não do servidor
        resp.update(ok=False, error=str(e))
    return json.dumps(resp, ensure_ascii=False), False


# ---------------------------------------------------------------- principal
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
    ap = argparse.ArgumentParser(description="Servidor REQ/REP com operações em JSON")
    ap.add_argument("--ports", default=f"{PORT1},{PORT2}",
                    help="portas separadas por vírgula (padrão: %(default)s)")
    ap.add_argument("--name", default=socket.gethostname(),
                    help="nome desta instância (padrão: hostname)")
    ap.add_argument("--delay", type=float, default=0.0,
                    help="atraso artificial (s) antes de responder, para testar timeout")
    a = ap.parse_args()

    context = zmq.Context()
    poller = zmq.Poller()
    sockets = {}                            # socket -> porta

    # Um socket REP por porta: assim sabemos QUAL porta recebeu a mensagem.
    # (O original usava um único socket com dois binds, que não permite distinguir.)
    for porta in a.ports.split(","):
        s = context.socket(zmq.REP)
        s.bind(f"tcp://*:{porta}")          # "*" = todas as interfaces de rede
        poller.register(s, zmq.POLLIN)
        sockets[s] = porta
    print(f"[{a.name}] servidor em {socket.gethostname()} ({meu_ip()}) ouvindo nas portas {a.ports} (pid {os.getpid()})", flush=True)

    parar = False
    try:
        while not parar:
            for s, _ in poller.poll():      # bloqueia até chegar algo em alguma porta
                texto, origem = recv_com_origem(s)
                info = {"server": a.name, "port": int(sockets[s]), "pid": os.getpid()}
                if a.delay:
                    time.sleep(a.delay)
                resposta, parar = atender(texto, info)
                print(f"[{a.name}:{sockets[s]}] de {origem}: {texto} -> {resposta}", flush=True)
                s.send_string(resposta)     # REP sempre precisa responder antes do próximo recv
                if parar:
                    break
    except KeyboardInterrupt:
        pass
    finally:
        print(f"[{a.name}] servidor encerrado", flush=True)
        for s in sockets:
            s.close(linger=500)             # dá tempo de a última resposta sair
        context.term()


if __name__ == "__main__":
    main()
