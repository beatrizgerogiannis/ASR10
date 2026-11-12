"""Cliente REQ/REP estendido (baseado em 04-22/client*.py).

Novidades:
  * endereço do servidor por --host / variável ZMQ_HOST (padrão: constCS.py);
  * pode conectar em uma ou nas duas portas (--ports 4567,4568); com duas, o
    REQ alterna entre elas (round-robin) e a resposta mostra qual porta atendeu;
  * modo interativo (padrão) e modo --demo;
  * timeout com o padrão "Lazy Pirate" do guia do ZeroMQ: se não vier resposta
    em --timeout ms, o cliente avisa, FECHA o socket REQ, cria outro e tenta de
    novo (até --retries vezes). Recriar é necessário porque um REQ que enviou e
    não recebeu fica "travado" no estado de espera e não aceita outro send.

Uso:
  python client.py --demo
  python client.py --host 10.0.1.5 --ports 4567,4568
  python client.py --timeout 1000 --retries 2
"""
import argparse
import json
import socket

import zmq

from constCS import HOST, PORT1


class Cliente:
    def __init__(self, host, portas, timeout_ms, tentativas):
        self.enderecos = [f"tcp://{host}:{p}" for p in portas]
        self.timeout_ms = timeout_ms
        self.tentativas = tentativas
        self.context = zmq.Context()
        self.s = None
        self._novo_socket()

    def _novo_socket(self):
        """(Re)cria o socket REQ e conecta em todos os endereços."""
        if self.s is not None:
            self.s.close(linger=0)          # descarta a mensagem pendente
        self.s = self.context.socket(zmq.REQ)
        for e in self.enderecos:
            self.s.connect(e)               # connect não bloqueia nem falha se o servidor estiver fora

    def pedir(self, requisicao):
        """Envia a requisição (dict) e devolve a resposta (dict) ou None."""
        texto = json.dumps(requisicao, ensure_ascii=False)
        for tentativa in range(1, self.tentativas + 1):
            self.s.send_string(texto)
            # Espera no máximo timeout_ms por uma resposta (em vez de recv bloqueante)
            if self.s.poll(self.timeout_ms, zmq.POLLIN):
                return json.loads(self.s.recv_string())
            print(f"  ! sem resposta em {self.timeout_ms} ms "
                  f"(tentativa {tentativa}/{self.tentativas}); recriando o socket REQ")
            self._novo_socket()
        print("  ! servidor parece fora do ar; desistindo desta requisição")
        return None

    def fechar(self):
        self.s.close(linger=0)
        self.context.term()


def meu_ip():
    """IP desta máquina/container (só para exibir nas mensagens)."""
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "?"


def converter(token):
    """'3' -> 3, '2.5' -> 2.5, 'abc' -> 'abc'."""
    for tipo in (int, float):
        try:
            return tipo(token)
        except ValueError:
            pass
    return token


def mostrar(req, resp):
    if resp is None:
        return
    quem = f"{resp.get('server')}:{resp.get('port')}"
    if resp.get("ok"):
        print(f"  {req['op']}{req.get('args', [])} = {resp['result']!r}   [atendido por {quem}]")
    else:
        print(f"  {req['op']}{req.get('args', [])} -> ERRO: {resp['error']}   [atendido por {quem}]")


DEMO = [
    {"op": "add", "args": [2, 3, 4]},
    {"op": "sub", "args": [10, 4]},
    {"op": "mul", "args": [6, 7]},
    {"op": "div", "args": [22, 7]},
    {"op": "upper", "args": ["sistemas distribuídos"]},
    {"op": "reverse", "args": ["ZeroMQ"]},
    {"op": "div", "args": [1, 0]},            # erro: divisão por zero
    {"op": "add", "args": [1, "abc"]},        # erro: argumento inválido
    {"op": "raiz", "args": [9]},              # erro: operação desconhecida
    {"op": "ops"},
]


def main():
    ap = argparse.ArgumentParser(description="Cliente REQ/REP com timeout (Lazy Pirate)")
    ap.add_argument("--host", default=HOST, help="IP/nome do servidor (padrão: %(default)s)")
    ap.add_argument("--ports", default=PORT1, help="porta(s) separadas por vírgula (padrão: %(default)s)")
    ap.add_argument("--timeout", type=int, default=2500, help="timeout em ms (padrão: %(default)s)")
    ap.add_argument("--retries", type=int, default=3, help="tentativas por requisição (padrão: %(default)s)")
    ap.add_argument("--demo", action="store_true", help="executa uma lista fixa de requisições")
    ap.add_argument("--stop", action="store_true", help="no fim do --demo, envia stop ao servidor")
    a = ap.parse_args()

    c = Cliente(a.host, a.ports.split(","), a.timeout, a.retries)
    print(f"cliente em {socket.gethostname()} ({meu_ip()}) -> {', '.join(c.enderecos)}")
    try:
        if a.demo:
            pedidos = DEMO + ([{"op": "stop"}] if a.stop else [])
            for req in pedidos:
                resp = c.pedir(req)
                if resp is None:            # não adianta seguir com o servidor fora do ar
                    print("  ! demo interrompida")
                    break
                mostrar(req, resp)
            return

        print('modo interativo. Ex.: "add 2 3", "upper ola mundo", "ops", "stop"; "sair" encerra.')
        while True:
            try:
                linha = input("> ").split()
            except EOFError:
                break
            if not linha:
                continue
            if linha[0].lower() in ("sair", "exit", "quit"):
                break
            req = {"op": linha[0], "args": [converter(t) for t in linha[1:]]}
            mostrar(req, c.pedir(req))
    except KeyboardInterrupt:
        pass
    finally:
        c.fechar()


if __name__ == "__main__":
    main()
