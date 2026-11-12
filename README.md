# Middleware orientado a mensagens com ZeroMQ

Tarefa de **Sistemas Distribuídos** (UFG). Partimos dos exemplos do livro
*Distributed Systems* (Tanenbaum & van Steen, Cap. 4, Figs. 4.21–4.23), testamos
em máquinas diferentes e acrescentamos novas funcionalidades aos três padrões de
comunicação do ZeroMQ.

```
04-22/   04-23/   04-24/    -> código ORIGINAL do livro (não alterado)
reqrep/  pubsub/  pipeline/ -> versões estendidas
Dockerfile, docker-compose.yml, docker-cenario.sh
                            -> três "máquinas" em containers numa rede própria (executado)
AWS.md                      -> guia para implantação em instâncias EC2 (não executado)
```

## Sumário

1. [ZeroMQ e os três padrões](#1-zeromq-e-os-três-padrões)
2. [Original × acrescentado](#2-original--acrescentado)
3. [Instalação](#3-instalação)
4. [Como rodar localmente](#4-como-rodar-localmente)
5. [Como rodar em várias máquinas](#5-como-rodar-em-várias-máquinas)
6. [Formato das mensagens e opções de linha de comando](#6-formato-das-mensagens-e-opções-de-linha-de-comando)
7. [Saídas de exemplo (testes locais)](#7-saídas-de-exemplo-testes-locais)
8. [Execução em máquinas diferentes (Docker)](#8-execução-em-máquinas-diferentes-docker)

---

## 1. ZeroMQ e os três padrões

O **ZeroMQ** (ØMQ) é uma biblioteca de mensagens: oferece "sockets" que trocam
**mensagens inteiras** (não um fluxo de bytes como o TCP) e já implementam
padrões de comunicação. Não há um broker central: os processos se conectam
diretamente. Algumas propriedades importantes, que aparecem nos testes:

- `bind` × `connect`: um lado faz `bind` num endereço fixo (o "servidor") e o
  outro faz `connect`. A ordem de inicialização não importa: o `connect` fica
  reconectando sozinho em segundo plano até o outro lado existir.
- As mensagens são enfileiradas e enviadas por uma thread de E/S do ZeroMQ,
  então `send` normalmente retorna na hora.
- Cada tipo de socket só conversa com o seu par (REQ↔REP, PUB↔SUB, PUSH↔PULL).

### REQ/REP — requisição e resposta (Fig. 4.21, pasta `04-22`)

```
   cliente (REQ) --- requisição --->  servidor (REP)
   cliente (REQ) <--- resposta -----  servidor (REP)

   REQ:  send, recv, send, recv ...   (alternância obrigatória)
   REP:  recv, send, recv, send ...
```

Comunicação **síncrona**, ponto a ponto. Vários clientes podem conectar no mesmo
servidor; o REP atende um por vez.

### PUB/SUB — publicação e assinatura (Fig. 4.22, pasta `04-23`)

```
                        +--> SUB  (assina "TIME")
   PUB --- "TIME ..." --+--> SUB  (assina "TEMP")   <- recebe só o que assina
       --- "TEMP ..."   +--> SUB  (assina "")       <- "" = tudo
```

**Um para muitos**, sem resposta. O filtro é por **prefixo** da mensagem e é
aplicado no publisher. O PUB não guarda histórico: quem não está conectado
(e assinado) no momento do envio simplesmente não recebe.

### PUSH/PULL — pipeline (Fig. 4.23, pasta `04-24`)

```
   fonte 1 (PUSH) --+                +--> worker 1 (PULL) --+
                    +--> round-robin-+--> worker 2 (PULL) --+--> sink (PULL)
   fonte 2 (PUSH) --+                +--> worker 3 (PULL) --+
                                          (PUSH para o sink)
```

Distribuição de tarefas: cada mensagem vai para **um** worker. O PUSH distribui
em **round-robin** entre os workers conectados; o PULL que conecta em várias
fontes faz *fair-queuing* (recebe um pouco de cada). O **sink** foi acrescentado
nesta tarefa (no livro o pipeline termina nos workers).

---

## 2. Original × acrescentado

### Comum aos três padrões

| Original | Estendido |
|---|---|
| IPs fixos em `const*.py` (IPs privados da AWS do professor) | `const*.py` continua existindo, mas com padrão `localhost`, sobrescrito por **variável de ambiente** (`ZMQ_HOST`, `ZMQ_SRC_HOSTS`, `ZMQ_SINK_HOST`...) ou **argumento** (`--host`, `--src-hosts`, `--sink-host`) |
| Quem faz bind usa o IP da máquina | Quem faz bind usa `tcp://*:<porta>` (todas as interfaces), aceitando conexões de outras máquinas |
| Mensagens de texto simples / pickle | **JSON** |
| — | Cada programa mostra seu hostname e IP; quem recebe mostra o **IP de origem** da conexão TCP (propriedade `Peer-Address` do frame do ZeroMQ), deixando claro de qual máquina a mensagem veio |
| Encerramento só matando o processo | Nos processos de longa duração (servidor, publisher, subscriber, workers, sink), Ctrl+C ou `kill` encerram limpo, fechando os sockets |
| Comentários em inglês | Comentários em português, `--help` em todos os programas |

### REQ/REP (`04-22` → `reqrep/`)

| Original | Acrescentado |
|---|---|
| Servidor devolve a string com `*` no fim | Servidor é um **serviço com operações**: `add`, `sub`, `mul`, `div`, `upper`, `lower`, `reverse`, `len`, `time`, `ops` (lista as operações), `stop` |
| Um socket REP com bind em duas portas | **Um socket REP por porta** + `zmq.Poller`, para saber **qual porta atendeu**; a resposta traz `server` (nome/hostname), `port` e `pid` |
| Qualquer erro derruba o servidor | **Erros viram resposta** `{"ok": false, "error": ...}`: operação desconhecida, argumento não numérico, divisão por zero, JSON sem `op` |
| `client.py`, `client1.py`, `client2.py` fixos | Um `client.py` com **modo interativo** e **`--demo`**, escolha de porta(s) (`--ports 4567,4568` faz o REQ alternar entre as duas) |
| `recv()` bloqueia para sempre se o servidor cair | **Timeout + retentativas (Lazy Pirate)**: `poll` com `--timeout` ms; sem resposta, o cliente avisa, **fecha e recria o socket REQ** e tenta de novo até `--retries` vezes |
| `STOP` encerra o servidor | Mantido (`{"op":"stop"}` ou texto `STOP`). O servidor continua aceitando texto puro como o original, então os clientes do livro funcionam com ele |
| — | `--delay` no servidor para simular servidor lento |

**Por que recriar o socket REQ?** O REQ é uma máquina de estados rígida: depois
de um `send` ele só aceita `recv`. Se a resposta nunca vier, o socket fica preso
nesse estado e um novo `send` gera erro. A solução do guia do ZeroMQ ("Lazy
Pirate") é descartar o socket (`close(linger=0)`) e criar outro. Efeito
colateral: um servidor lento pode acabar processando a mesma requisição duas
vezes (a original e a retentativa), por isso esse padrão é adequado para
operações **idempotentes** como as desta calculadora.

### PUB/SUB (`04-23` → `pubsub/`)

| Original | Acrescentado |
|---|---|
| Só o tópico `TIME`, a cada 5 s | Tópicos **`TIME`**, **`TEMP`** (temperatura simulada, passeio aleatório entre 15 e 45 °C) e **`ALERTA`** (só publicado quando `TEMP` passa de `--limite`) |
| Mensagem `"TIME <hora>"` | Mensagem `"<TÓPICO> <JSON>"` com `origem` (hostname do publisher), `seq` (sequência por tópico) e `ts` |
| Intervalo fixo | `--intervalo` e `--rodadas` configuráveis |
| Subscriber fixo em `TIME`, 5 mensagens | `--topics TIME,ALERTA` (vazio = todos) e `-n` (0 = infinito) |
| — | Subscriber mostra a máquina de origem e **detecta mensagens perdidas** pela sequência |
| — | `slow_joiner.py` demonstra o problema do *slow joiner* |

**O problema do *slow joiner*.** No subscriber, `connect()` e
`setsockopt(SUBSCRIBE)` retornam imediatamente, mas o estabelecimento da conexão
TCP e o envio da assinatura ao publisher acontecem em segundo plano e levam
alguns milissegundos (mais, entre máquinas). Como o PUB descarta mensagens para
quem ainda não está conectado e assinado, **as mensagens publicadas nesse
intervalo se perdem**, mesmo que o subscriber já esteja "rodando". Os testes da
seção 7 mostram isso de duas formas: com `slow_joiner.py` (0 de 20 mensagens
recebidas quando o publisher envia logo após o connect; 20 de 20 com 0,5 s de
espera) e com os próprios `publisher.py`/`subscriber.py` (subscriber já
rodando, publisher com `--intervalo 0`: 0 mensagens recebidas). Soluções usuais:
esperar um pouco antes de publicar, sincronizar por um canal REQ/REP à parte
("o subscriber avisa que está pronto"), ou aceitar a perda (dados periódicos
como temperatura costumam tolerar isso).

Observação: o filtro é por prefixo, então o subscriber assina `"TEMP "` (com
espaço) para que `TEMP` não case, por exemplo, com um tópico `TEMPO`.

### Pipeline (`04-24` → `pipeline/`)

| Original | Acrescentado |
|---|---|
| Mensagens com **pickle** | Mensagens em **JSON** (ver abaixo) |
| 100 tarefas, 1 s entre envios, 2 fontes fixas | `--tarefas`, `--intervalo` e número de fontes (`id` da fonte; porta = 4570 + id) configuráveis; `--seed` para gerar sempre as mesmas cargas |
| Workers só imprimem | Workers enviam o resultado a um **sink** |
| — | **`sink.py`**: conta tarefas concluídas **por worker**, **por fonte** e no cruzamento worker × fonte, soma a carga de cada worker e mede o **tempo total do lote** |
| — | Fontes esperam `--espera` s (ou ENTER com `--enter`) antes de distribuir, para os workers conectarem |
| — | **`compara_workers.sh`**: roda o mesmo lote com 1, 2 e 3 workers e compara os tempos |

**Como o sink sabe quando o lote acabou?** Cada fonte conecta também no sink e,
antes de distribuir, envia `{"tipo": "inicio", "fonte": ..., "tarefas": N}`. O
sink espera os avisos de `--fontes` fontes e termina quando chegarem todos os
resultados anunciados. O tempo é medido do primeiro aviso ao último resultado,
**só com o relógio do sink** — não depende de os relógios das máquinas estarem
sincronizados.

**Por que trocar pickle por JSON?** `pickle.loads()` não apenas lê dados: ele
**reconstrói objetos Python executando instruções contidas na própria
mensagem**. Um objeto pode definir `__reduce__` para que, ao ser desserializado,
chame qualquer função, por exemplo `os.system("rm -rf ~")`. No original, cada
worker faz `pickle.loads(r.recv())` em bytes recebidos pela rede, de qualquer
processo que consiga falar com ele, sem autenticação nem criptografia (o TCP do
ZeroMQ é texto claro por padrão). Portanto, quem conseguir se passar por uma
fonte de tarefas consegue **executar código arbitrário no worker**. A própria
documentação do Python avisa: *"Never unpickle data that could have come from an
untrusted or unauthenticated source."* JSON só produz dados (dicionários,
listas, strings, números), então uma mensagem maliciosa no máximo causa um erro
de formato. (Para autenticar e cifrar o tráfego, o ZeroMQ oferece o mecanismo
CURVE, fora do escopo deste trabalho.)

**Distribuição round-robin e tempo total.** O PUSH divide as tarefas **pela
quantidade**, não pela carga: cada worker recebe ~1/N das mensagens, mesmo que
umas sejam bem mais pesadas que outras. Por isso o tempo do lote fica perto da
**maior carga somada de um worker** × 0,01 s (veja a seção 7: com 3 workers o
tempo foi 8,27 s, e o worker mais carregado tinha carga 827). Outro efeito:
um worker que conecta **depois** que a fonte já distribuiu não recebe nada
(teste "sem espera" na seção 7). Para balanceamento por carga real, o ZeroMQ
usa outro padrão (ROUTER/DEALER, "load balancing broker").

---

## 3. Instalação

Requer Python 3 (testado com Python 3.12.3, pyzmq 27.3.0 / libzmq 4.3.5).

```bash
git clone https://github.com/beatrizgerogiannis/ASR10.git
cd ASR10
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 4. Como rodar localmente

Abra um terminal por processo (todos com o `.venv` ativado). Sem argumentos de
endereço, tudo usa `localhost`.

**REQ/REP**

```bash
cd reqrep
python server.py                          # terminal 1
python client.py --demo --ports 4567,4568 # terminal 2
python client.py                          # modo interativo: add 2 3 | upper ola | ops | stop | sair
python client.py --demo --timeout 1000    # com o servidor parado: mostra o timeout
```

**PUB/SUB**

```bash
cd pubsub
python publisher.py --intervalo 1 --limite 28    # terminal 1
python subscriber.py --topics TIME,ALERTA -n 10  # terminal 2
python subscriber.py --topics TEMP -n 0          # terminal 3
python slow_joiner.py                            # demonstração isolada do slow joiner
```

**Pipeline**

```bash
cd pipeline
python sink.py --fontes 2                        # terminal 1
python taskwork.py 1 --fontes 2                  # terminal 2
python taskwork.py 2 --fontes 2                  # terminal 3
python tasksrc.py 1 --tarefas 20 --seed 101      # terminal 4
python tasksrc.py 2 --tarefas 20 --seed 102      # terminal 5

./compara_workers.sh 20 2 3   # tudo automático: mesmo lote com 1, 2 e 3 workers
```

## 5. Como rodar em várias máquinas

Duas formas, ambas com o mesmo código e os mesmos comandos:

- **Docker** (executado): três containers numa rede própria simulam as máquinas
  A, B e C — ver [seção 8](#8-execução-em-máquinas-diferentes-docker).
- **AWS EC2** (guia, **não executado**): passo a passo em **[AWS.md](AWS.md)**
  (instâncias, Security Group, instalação e ordem dos comandos).

Em resumo:

- a máquina que faz **bind** (servidor, publisher, fontes, sink) não precisa de
  configuração: ela escuta em `tcp://*:<porta>`;
- nas máquinas que fazem **connect**, informe o IP da outra máquina por
  argumento ou variável de ambiente:

```bash
python client.py     --host 172.31.10.20            # ou: export ZMQ_HOST=172.31.10.20
python subscriber.py --host 172.31.10.20
python taskwork.py 1 --src-hosts 172.31.10.20 --sink-host 172.31.10.20
#  fontes em máquinas diferentes:  --src-hosts IP_FONTE1,IP_FONTE2
```

- libere no firewall/Security Group as portas **4567–4573** (TCP).

| Porta | Uso |
|---|---|
| 4567, 4568 | REQ/REP (servidor) |
| 4569 | PUB/SUB (publisher) |
| 4570 | sink do pipeline |
| 4571, 4572, 4573 | fontes 1, 2, 3 do pipeline |

## 6. Formato das mensagens e opções de linha de comando

### REQ/REP

Requisição e resposta (JSON):

```json
{"op": "div", "args": [22, 7]}
{"server": "A", "port": 4568, "pid": 27007, "ok": true,  "result": 3.142857142857143}

{"op": "div", "args": [1, 0]}
{"server": "A", "port": 4567, "pid": 27007, "ok": false, "error": "divisão por zero"}
```

| Operação | Args | Resultado |
|---|---|---|
| `add`, `sub`, `mul`, `div` | 2 ou mais números | soma; 1º menos os demais; produto; divisões sucessivas |
| `upper`, `lower`, `reverse`, `len` | texto (vários args são unidos por espaço) | texto transformado / tamanho |
| `time` | — | hora do servidor |
| `ops` | — | lista de operações |
| `stop` | — | servidor responde e encerra |

Texto que não é JSON é tratado como no livro: `"Hello world"` → `"Hello world*"`, e `"STOP"` encerra.

| Programa | Opções |
|---|---|
| `server.py` | `--ports 4567,4568`  `--name <nome>` (padrão: hostname)  `--delay <s>` |
| `client.py` | `--host` (padrão `ZMQ_HOST` ou localhost)  `--ports 4567[,4568]`  `--timeout <ms>` (2500)  `--retries <n>` (3)  `--demo`  `--stop` |

### PUB/SUB

Uma mensagem por tópico: `"<TÓPICO> <JSON>"`

```
TIME {"origem": "maquina-a", "seq": 4, "ts": 1794488070.75, "hora": "Thu Nov 12 09:54:30 2026"}
TEMP {"origem": "maquina-a", "seq": 4, "ts": 1794488070.75, "valor": 27.7, "unidade": "C"}
ALERTA {"origem": "maquina-a", "seq": 1, "ts": 1794488070.75, "msg": "temperatura 27.7 C acima de 26.0", "valor": 27.7}
```

| Programa | Opções |
|---|---|
| `publisher.py` | `--port` (4569)  `--intervalo <s>` (1.0)  `--limite <°C>` (30)  `--rodadas <n>` (0 = infinito)  `--quieto` |
| `subscriber.py` | `--host`  `--port`  `--topics TIME,TEMP,ALERTA` (vazio = todos)  `-n <n>` (5; 0 = infinito) |
| `slow_joiner.py` | `--mensagens <n>` (20)  `--espera <s>` (0.5) |

### Pipeline

```json
fonte  -> worker : {"fonte": "1", "tarefa": 7, "carga": 58}
fonte  -> sink   : {"tipo": "inicio", "fonte": "1", "tarefas": 20, "host": "maquina-a"}
worker -> sink   : {"tipo": "resultado", "worker": "2", "host": "maquina-b", "fonte": "1", "tarefa": 7, "carga": 58}
```

| Programa | Opções |
|---|---|
| `tasksrc.py <id>` | `--tarefas` (20)  `--seed`  `--intervalo <s>` (0)  `--espera <s>` (2)  `--enter`  `--sink-host`  `--sink-port` |
| `taskwork.py <id>` | `--fontes <n>` (2)  `--src-hosts ip1[,ip2,...]`  `--sink-host`  `--sink-port`  `--escala <s por unidade>` (0.01) |
| `sink.py` | `--fontes <n>` (2)  `--port` (4570)  `--verbose` |
| `compara_workers.sh` | `[tarefas_por_fonte=20] [fontes=2] [max_workers=3]` |

Variáveis de ambiente (padrões dos `const*.py`): `ZMQ_HOST`, `ZMQ_PORT1`,
`ZMQ_PORT2` (reqrep); `ZMQ_HOST`, `ZMQ_PORT` (pubsub); `ZMQ_SRC_HOSTS`,
`ZMQ_SINK_HOST`, `ZMQ_SINK_PORT`, `ZMQ_SRC_BASE_PORT` (pipeline).

---

## 7. Saídas de exemplo (testes locais)

Saídas reais, tudo no mesmo computador (hostname `Beatriz`), em 12/11/2026.
Localmente, o IP de origem aparece como `127.0.0.1` (loopback); o hostname é
resolvido para `127.0.1.1`. Na seção 8 os mesmos programas mostram os IPs
reais da rede entre containers.

### 7.0 Exemplos originais do livro

Rodados a partir de uma cópia com os IPs trocados por `127.0.0.1` (as pastas
originais não foram alteradas).

```
# 04-22: client1.py, client2.py, client.py (este último envia STOP)
Hello world 1*
Hello world 2*
Hello world 1*

# 04-23: subscriber.py (5 mensagens, uma a cada 5 s)
TIME Thu Nov 12 09:43:16 2026
TIME Thu Nov 12 09:43:21 2026
TIME Thu Nov 12 09:43:26 2026
TIME Thu Nov 12 09:43:31 2026
TIME Thu Nov 12 09:43:36 2026

# 04-24: tasksrc.py 1, tasksrc.py 2, taskwork.py 1, taskwork.py 2 (6 s)
--- worker 1                     --- worker 2
1 started                        2 started
1 received 96 from 2             2 received 92 from 2
1 received 82 from 1             2 received 91 from 1
1 received 46 from 2             2 received 95 from 2
1 received 11 from 1             2 received 6 from 1
```

### 7.1 REQ/REP

`client.py --demo --ports 4567,4568` — o REQ alterna entre as duas portas:

```
cliente em Beatriz (127.0.1.1) -> tcp://localhost:4567, tcp://localhost:4568
  add[2, 3, 4] = 9   [atendido por A:4567]
  sub[10, 4] = 6   [atendido por A:4568]
  mul[6, 7] = 42   [atendido por A:4567]
  div[22, 7] = 3.142857142857143   [atendido por A:4568]
  upper['sistemas distribuídos'] = 'SISTEMAS DISTRIBUÍDOS'   [atendido por A:4567]
  reverse['ZeroMQ'] = 'QMoreZ'   [atendido por A:4568]
  div[1, 0] -> ERRO: divisão por zero   [atendido por A:4567]
  add[1, 'abc'] -> ERRO: argumento não numérico: 'abc'   [atendido por A:4568]
  raiz[9] -> ERRO: operação desconhecida: 'raiz'   [atendido por A:4567]
  ops[] = ['add', 'div', 'len', 'lower', 'mul', 'reverse', 'sub', 'time', 'upper', 'ops', 'stop']   [atendido por A:4568]
```

Servidor **fora do ar** (`--timeout 1000 --retries 3`):

```
cliente em Beatriz (127.0.1.1) -> tcp://localhost:4567
  ! sem resposta em 1000 ms (tentativa 1/3); recriando o socket REQ
  ! sem resposta em 1000 ms (tentativa 2/3); recriando o socket REQ
  ! sem resposta em 1000 ms (tentativa 3/3); recriando o socket REQ
  ! servidor parece fora do ar; desistindo desta requisição
  ! demo interrompida
```

Servidor **sobe 1,5 s depois** do cliente (modo interativo): a primeira
tentativa expira, a segunda é atendida:

```
>   ! sem resposta em 1000 ms (tentativa 1/3); recriando o socket REQ
  add[2, 3] = 5   [atendido por A:4567]
>   mul[4, 5] = 20   [atendido por A:4567]
```

Servidor **lento** (`--delay 2`) com cliente `--timeout 1000 --retries 2`:

```
>   ! sem resposta em 1000 ms (tentativa 1/2); recriando o socket REQ
  ! sem resposta em 1000 ms (tentativa 2/2); recriando o socket REQ
  ! servidor parece fora do ar; desistindo desta requisição
```

**Clientes originais do livro** contra o servidor novo (compatibilidade):

```
Hello world 1*
Hello world 2*
Hello world 1*
```

Log desse servidor (as duas primeiras linhas são do teste "sobe 1,5 s depois";
as demais, dos clientes originais):

```
[A] servidor em Beatriz (127.0.1.1) ouvindo nas portas 4567,4568 (pid 32129)
[A:4567] de 127.0.0.1: {"op": "add", "args": [2, 3]} -> {"server": "A", "port": 4567, "pid": 32129, "ok": true, "result": 5}
[A:4567] de 127.0.0.1: {"op": "mul", "args": [4, 5]} -> {"server": "A", "port": 4567, "pid": 32129, "ok": true, "result": 20}
[A:4567] de 127.0.0.1: Hello world 1 -> Hello world 1*
[A:4568] de 127.0.0.1: Hello world 2 -> Hello world 2*
[A:4567] de 127.0.0.1: Hello world 1 -> Hello world 1*
[A:4567] de 127.0.0.1: STOP -> STOP recebido
[A] servidor encerrado
```

### 7.2 PUB/SUB

`publisher.py --intervalo 0.5 --limite 26`, com dois subscribers seletivos
iniciados antes e um terceiro (todos os tópicos) que entra 3 s depois:

```
===== publisher (trecho)
[Beatriz] (127.0.1.1) publicando em tcp://*:4569 a cada 0.5s (ALERTA se TEMP > 26.0)
  pub: TIME {"origem": "Beatriz", "seq": 1, "ts": 1794488725.6478338, "hora": "Thu Nov 12 10:05:25 2026"}
  pub: TEMP {"origem": "Beatriz", "seq": 1, "ts": 1794488725.6481016, "valor": 27.4, "unidade": "C"}
  pub: ALERTA {"origem": "Beatriz", "seq": 1, "ts": 1794488725.6481829, "msg": "temperatura 27.4 C acima de 26.0", "valor": 27.4}
  ...
[Beatriz] publisher encerrado após 9 rodadas; enviados {'TIME': 9, 'TEMP': 9, 'ALERTA': 9}

===== subscriber.py --topics TIME,ALERTA -n 6
subscriber em Beatriz (127.0.1.1) -> tcp://localhost:4569 tópicos=['TIME', 'ALERTA'] n=6
[  1] TIME   de Beatriz via 127.0.0.1 seq=1 {'hora': 'Thu Nov 12 10:05:25 2026'}
[  2] ALERTA de Beatriz via 127.0.0.1 seq=1 {'msg': 'temperatura 27.4 C acima de 26.0', 'valor': 27.4}
[  3] TIME   de Beatriz via 127.0.0.1 seq=2 {'hora': 'Thu Nov 12 10:05:26 2026'}
[  4] ALERTA de Beatriz via 127.0.0.1 seq=2 {'msg': 'temperatura 29.6 C acima de 26.0', 'valor': 29.6}
[  5] TIME   de Beatriz via 127.0.0.1 seq=3 {'hora': 'Thu Nov 12 10:05:26 2026'}
[  6] ALERTA de Beatriz via 127.0.0.1 seq=3 {'msg': 'temperatura 26.9 C acima de 26.0', 'valor': 26.9}
subscriber encerrado após 6 mensagens

===== subscriber.py --topics TEMP -n 6
subscriber em Beatriz (127.0.1.1) -> tcp://localhost:4569 tópicos=['TEMP'] n=6
[  1] TEMP   de Beatriz via 127.0.0.1 seq=1 {'valor': 27.4, 'unidade': 'C'}
[  2] TEMP   de Beatriz via 127.0.0.1 seq=2 {'valor': 29.6, 'unidade': 'C'}
[  3] TEMP   de Beatriz via 127.0.0.1 seq=3 {'valor': 26.9, 'unidade': 'C'}
[  4] TEMP   de Beatriz via 127.0.0.1 seq=4 {'valor': 27.4, 'unidade': 'C'}
[  5] TEMP   de Beatriz via 127.0.0.1 seq=5 {'valor': 27.0, 'unidade': 'C'}
[  6] TEMP   de Beatriz via 127.0.0.1 seq=6 {'valor': 30.0, 'unidade': 'C'}
subscriber encerrado após 6 mensagens

===== subscriber.py -n 6   (entrou 3 s depois)
subscriber em Beatriz (127.0.1.1) -> tcp://localhost:4569 tópicos=TODOS n=6
[  1] TIME   de Beatriz via 127.0.0.1 seq=6 {'hora': 'Thu Nov 12 10:05:28 2026'}   (entrou atrasado: 5 msgs de TIME anteriores não chegaram)
[  2] TEMP   de Beatriz via 127.0.0.1 seq=6 {'valor': 30.0, 'unidade': 'C'}   (entrou atrasado: 5 msgs de TEMP anteriores não chegaram)
[  3] ALERTA de Beatriz via 127.0.0.1 seq=6 {'msg': 'temperatura 30.0 C acima de 26.0', 'valor': 30.0}   (entrou atrasado: 5 msgs de ALERTA anteriores não chegaram)
[  4] TIME   de Beatriz via 127.0.0.1 seq=7 {'hora': 'Thu Nov 12 10:05:28 2026'}
[  5] TEMP   de Beatriz via 127.0.0.1 seq=7 {'valor': 27.7, 'unidade': 'C'}
[  6] ALERTA de Beatriz via 127.0.0.1 seq=7 {'msg': 'temperatura 27.7 C acima de 26.0', 'valor': 27.7}
subscriber encerrado após 6 mensagens
```

**Slow joiner** com `slow_joiner.py` (resultado idêntico em 3 execuções):

```
Cenário 1 - publica logo depois do connect/subscribe:
espera=0.00s: recebidas 0/20; perdidas: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
Cenário 2 - espera a assinatura se estabelecer antes de publicar:
espera=0.50s: recebidas 20/20; perdidas: nenhuma
```

**Slow joiner** com os programas reais: subscriber já rodando, publisher
iniciado depois publicando 5 rodadas **sem esperar** (`--intervalo 0 --rodadas 5`):

```
[Beatriz] (127.0.1.1) publicando em tcp://*:4569 a cada 0.0s (ALERTA se TEMP > 30.0)
[Beatriz] publisher encerrado após 5 rodadas; enviados {'TIME': 5, 'TEMP': 5, 'ALERTA': 0}
subscriber em Beatriz (127.0.1.1) -> tcp://localhost:4569 tópicos=TODOS n=infinito
subscriber encerrado após 0 mensagens
```

Mesmo teste com `--intervalo 0.3` (a primeira espera dá tempo de a assinatura
chegar): todas as 6 mensagens chegam.

```
subscriber em Beatriz (127.0.1.1) -> tcp://localhost:4569 tópicos=TODOS n=infinito
[  1] TIME   de Beatriz via 127.0.0.1 seq=1 {'hora': 'Thu Nov 12 10:05:31 2026'}
[  2] TEMP   de Beatriz via 127.0.0.1 seq=1 {'valor': 23.0, 'unidade': 'C'}
[  3] TIME   de Beatriz via 127.0.0.1 seq=2 {'hora': 'Thu Nov 12 10:05:31 2026'}
[  4] TEMP   de Beatriz via 127.0.0.1 seq=2 {'valor': 21.1, 'unidade': 'C'}
[  5] TIME   de Beatriz via 127.0.0.1 seq=3 {'hora': 'Thu Nov 12 10:05:31 2026'}
[  6] TEMP   de Beatriz via 127.0.0.1 seq=3 {'valor': 20.7, 'unidade': 'C'}
subscriber encerrado após 6 mensagens
```

### 7.3 Pipeline

`./compara_workers.sh 20 2 3` — 2 fontes × 20 tarefas, mesmas cargas nas três rodadas:

```
########## 1 worker(s) ##########
===== resumo do lote =====
tarefas concluídas: 40
por worker:
  worker 1 (Beatriz, 127.0.0.1):  40 tarefas, carga  2164  [fonte 1: 20, fonte 2: 20]
por fonte:
  fonte 1: 20 de 20
  fonte 2: 20 de 20
TEMPO TOTAL DO LOTE: 21.60 s com 1 worker(s)

########## 2 worker(s) ##########
===== resumo do lote =====
tarefas concluídas: 40
por worker:
  worker 1 (Beatriz, 127.0.0.1):  20 tarefas, carga  1069  [fonte 1: 10, fonte 2: 10]
  worker 2 (Beatriz, 127.0.0.1):  20 tarefas, carga  1095  [fonte 1: 10, fonte 2: 10]
por fonte:
  fonte 1: 20 de 20
  fonte 2: 20 de 20
TEMPO TOTAL DO LOTE: 10.96 s com 2 worker(s)

########## 3 worker(s) ##########
===== resumo do lote =====
tarefas concluídas: 40
por worker:
  worker 1 (Beatriz, 127.0.0.1):  13 tarefas, carga   782  [fonte 1: 7, fonte 2: 6]
  worker 2 (Beatriz, 127.0.0.1):  14 tarefas, carga   827  [fonte 1: 7, fonte 2: 7]
  worker 3 (Beatriz, 127.0.0.1):  13 tarefas, carga   555  [fonte 1: 6, fonte 2: 7]
por fonte:
  fonte 1: 20 de 20
  fonte 2: 20 de 20
TEMPO TOTAL DO LOTE: 8.27 s com 3 worker(s)
```

| Workers | Tempo total | Maior carga de um worker × 0,01 s |
|---|---|---|
| 1 | 21,60 s | 21,64 s |
| 2 | 10,96 s | 10,95 s |
| 3 | 8,27 s | 8,27 s |

O ganho não é exatamente linear (3 workers → 2,6×, não 3×) porque o round-robin
divide pelo número de tarefas, não pela carga. A divisão exata de quais tarefas
vão para qual worker varia entre execuções (depende da ordem em que os workers
se conectam a cada fonte), mas o total de cada worker fica sempre em ~1/N das
tarefas.

**3 fontes × 5 tarefas, 2 workers** — cada worker recebe de todas as fontes (fair-queuing):

```
--- worker 1
worker 1 em Beatriz (127.0.1.1) iniciado: 3 fonte(s) em ['localhost'], sink em localhost
worker 1 recebeu carga  31 da fonte 3 (tarefa 1) via 127.0.0.1
worker 1 recebeu carga  70 da fonte 3 (tarefa 3) via 127.0.0.1
worker 1 recebeu carga  48 da fonte 3 (tarefa 5) via 127.0.0.1
worker 1 recebeu carga  18 da fonte 1 (tarefa 1) via 127.0.0.1
worker 1 recebeu carga  12 da fonte 2 (tarefa 2) via 127.0.0.1
worker 1 recebeu carga  98 da fonte 1 (tarefa 3) via 127.0.0.1
worker 1 recebeu carga  47 da fonte 2 (tarefa 4) via 127.0.0.1
worker 1 recebeu carga  33 da fonte 1 (tarefa 5) via 127.0.0.1
worker 1 encerrado após 8 tarefas
--- sink
sink em Beatriz (127.0.1.1): tcp://*:4570, aguardando 3 fonte(s)
  fonte 3 (Beatriz, 127.0.0.1) começou: 5 tarefas
  fonte 1 (Beatriz, 127.0.0.1) começou: 5 tarefas
  fonte 2 (Beatriz, 127.0.0.1) começou: 5 tarefas

===== resumo do lote =====
tarefas concluídas: 15
por worker:
  worker 1 (Beatriz, 127.0.0.1):   8 tarefas, carga   357  [fonte 1: 3, fonte 2: 2, fonte 3: 3]
  worker 2 (Beatriz, 127.0.0.1):   7 tarefas, carga   216  [fonte 1: 2, fonte 2: 3, fonte 3: 2]
por fonte:
  fonte 1: 5 de 5
  fonte 2: 5 de 5
  fonte 3: 5 de 5
TEMPO TOTAL DO LOTE: 3.58 s com 2 worker(s)
```

**Sem espera** (`--espera 0`), com o worker 2 conectando 0,5 s depois da fonte
começar: o worker 1 recebe tudo e o worker 2 nenhuma tarefa.

```
por worker:
  worker 1 (Beatriz, 127.0.0.1):  20 tarefas, carga  1050  [fonte 1: 20]
por fonte:
  fonte 1: 20 de 20
TEMPO TOTAL DO LOTE: 5.20 s com 1 worker(s)
```

Após todos os testes, nenhum processo ficou rodando e as portas 4567–4579
estavam livres (`ss -ltn`).

---

## 8. Execução em máquinas diferentes (Docker)

Os testes em máquinas diferentes foram feitos com **Docker**: três containers,
cada um com seu próprio hostname, IP e pilha de rede, ligados por uma rede
bridge própria. A comunicação entre eles passa por TCP/IP de verdade (interfaces
de rede virtuais), e não por loopback como na seção 7.

O **[AWS.md](AWS.md)** é o guia equivalente para implantação em instâncias EC2
da AWS. Ele **não foi executado** neste trabalho; os comandos são os mesmos
usados aqui, trocando os nomes dos containers pelos IPs privados das instâncias.

### 8.1 Topologia

| "Máquina" (container) | IP | Papel | Como acha a máquina A |
|---|---|---|---|
| `maquina-a` | 172.28.0.10 | servidor REQ/REP, publisher, fontes 1 e 2, sink | — (só faz bind em `tcp://*`) |
| `maquina-b` | 172.28.0.11 | cliente, subscriber, workers 1 e 3 | **variável de ambiente** `ZMQ_HOST=maquina-a` (nome resolvido pelo DNS do Docker) |
| `maquina-c` | 172.28.0.12 | cliente, subscriber, worker 2 | **argumento** `--host 172.28.0.10` / `--src-hosts 172.28.0.10` |

```
                      rede zmqnet (bridge) 172.28.0.0/24

   maquina-b  .11                maquina-a  .10                maquina-c  .12
   client.py        --REQ/REP--> server.py   :4567,4568 <--REQ/REP--  client.py
   subscriber.py    <--PUB/SUB-- publisher   :4569      --PUB/SUB-->  subscriber.py
   taskwork.py 1,3  <--PUSH----- tasksrc 1,2 :4571,4572 -----PUSH-->  taskwork.py 2
   taskwork.py 1,3  ---PUSH----> sink.py     :4570      <----PUSH---  taskwork.py 2

   A só faz bind (tcp://*:porta); B e C fazem connect em A.
```

Arquivos:

- [`Dockerfile`](Dockerfile): `python:3.12-slim` + `requirements.txt` (+ `procps`,
  só para o `pkill` do script de teste). O container fica parado em
  `sleep infinity`; os programas são executados com `docker compose exec`.
- [`docker-compose.yml`](docker-compose.yml): as três máquinas, a rede `zmqnet`
  com IPs fixos e as variáveis de ambiente de `maquina-b`.
- [`docker-cenario.sh`](docker-cenario.sh): sobe os containers, roda os três
  padrões na ordem do AWS.md e derruba tudo no fim.

### 8.2 Como rodar

Automático (é o que gerou as saídas abaixo):

```bash
./docker-cenario.sh
```

Manual, com um terminal por "máquina":

```bash
docker compose up -d --build

docker compose exec -w /app/reqrep maquina-a python server.py --name A
docker compose exec -w /app/reqrep maquina-b python client.py --demo --ports 4567,4568
docker compose exec -w /app/reqrep maquina-c python client.py --host 172.28.0.10

docker compose down
```

### 8.3 Saídas — REQ/REP

Identificação das máquinas:

```
maquina-a  IP 172.28.0.10  ZMQ_HOST=<não definido>
maquina-b  IP 172.28.0.11  ZMQ_HOST=maquina-a
maquina-c  IP 172.28.0.12  ZMQ_HOST=<não definido>
```

**maquina-b** — `python client.py --ports 4567,4568 --demo` (endereço via `ZMQ_HOST`):

```
cliente em maquina-b (172.28.0.11) -> tcp://maquina-a:4567, tcp://maquina-a:4568
  add[2, 3, 4] = 9   [atendido por A:4567]
  sub[10, 4] = 6   [atendido por A:4568]
  mul[6, 7] = 42   [atendido por A:4567]
  div[22, 7] = 3.142857142857143   [atendido por A:4568]
  upper['sistemas distribuídos'] = 'SISTEMAS DISTRIBUÍDOS'   [atendido por A:4567]
  reverse['ZeroMQ'] = 'QMoreZ'   [atendido por A:4568]
  div[1, 0] -> ERRO: divisão por zero   [atendido por A:4567]
  add[1, 'abc'] -> ERRO: argumento não numérico: 'abc'   [atendido por A:4568]
  raiz[9] -> ERRO: operação desconhecida: 'raiz'   [atendido por A:4567]
  ops[] = ['add', 'div', 'len', 'lower', 'mul', 'reverse', 'sub', 'time', 'upper', 'ops', 'stop']   [atendido por A:4568]
```

**maquina-c** — `python client.py --host 172.28.0.10 --ports 4568` (modo interativo):

```
cliente em maquina-c (172.28.0.12) -> tcp://172.28.0.10:4568
modo interativo. Ex.: "add 2 3", "upper ola mundo", "ops", "stop"; "sair" encerra.
>   add[2, 3] = 5   [atendido por A:4568]
>   upper['ola', 'docker'] = 'OLA DOCKER'   [atendido por A:4568]
>   reverse['maquina-c'] = 'c-aniuqam'   [atendido por A:4568]
```

**maquina-b** — `python client.py --demo --stop` (última linha):

```
  stop[] = 'servidor encerrando'   [atendido por A:4567]
```

**maquina-a** — log do servidor. A coluna `de <IP>` é o IP de origem da conexão:
`172.28.0.11` = maquina-b, `172.28.0.12` = maquina-c.

```
[A] servidor em maquina-a (172.28.0.10) ouvindo nas portas 4567,4568 (pid 22)
[A:4567] de 172.28.0.11: {"op": "add", "args": [2, 3, 4]} -> {"server": "A", "port": 4567, "pid": 22, "ok": true, "result": 9}
[A:4568] de 172.28.0.11: {"op": "sub", "args": [10, 4]} -> {"server": "A", "port": 4568, "pid": 22, "ok": true, "result": 6}
[A:4567] de 172.28.0.11: {"op": "mul", "args": [6, 7]} -> {"server": "A", "port": 4567, "pid": 22, "ok": true, "result": 42}
[A:4568] de 172.28.0.11: {"op": "div", "args": [22, 7]} -> {"server": "A", "port": 4568, "pid": 22, "ok": true, "result": 3.142857142857143}
[A:4567] de 172.28.0.11: {"op": "upper", "args": ["sistemas distribuídos"]} -> {"server": "A", "port": 4567, "pid": 22, "ok": true, "result": "SISTEMAS DISTRIBUÍDOS"}
[A:4568] de 172.28.0.11: {"op": "reverse", "args": ["ZeroMQ"]} -> {"server": "A", "port": 4568, "pid": 22, "ok": true, "result": "QMoreZ"}
[A:4567] de 172.28.0.11: {"op": "div", "args": [1, 0]} -> {"server": "A", "port": 4567, "pid": 22, "ok": false, "error": "divisão por zero"}
[A:4568] de 172.28.0.11: {"op": "add", "args": [1, "abc"]} -> {"server": "A", "port": 4568, "pid": 22, "ok": false, "error": "argumento não numérico: 'abc'"}
[A:4567] de 172.28.0.11: {"op": "raiz", "args": [9]} -> {"server": "A", "port": 4567, "pid": 22, "ok": false, "error": "operação desconhecida: 'raiz'"}
[A:4568] de 172.28.0.11: {"op": "ops"} -> {"server": "A", "port": 4568, "pid": 22, "ok": true, "result": ["add", "div", "len", "lower", "mul", "reverse", "sub", "time", "upper", "ops", "stop"]}
[A:4568] de 172.28.0.12: {"op": "add", "args": [2, 3]} -> {"server": "A", "port": 4568, "pid": 22, "ok": true, "result": 5}
[A:4568] de 172.28.0.12: {"op": "upper", "args": ["ola", "docker"]} -> {"server": "A", "port": 4568, "pid": 22, "ok": true, "result": "OLA DOCKER"}
[A:4568] de 172.28.0.12: {"op": "reverse", "args": ["maquina-c"]} -> {"server": "A", "port": 4568, "pid": 22, "ok": true, "result": "c-aniuqam"}
[A:4567] de 172.28.0.11: {"op": "add", "args": [2, 3, 4]} -> {"server": "A", "port": 4567, "pid": 22, "ok": true, "result": 9}
...                       (demais requisições do --demo --stop de maquina-b)
[A:4567] de 172.28.0.11: {"op": "stop"} -> {"server": "A", "port": 4567, "pid": 22, "ok": true, "result": "servidor encerrando"}
[A] servidor encerrado
```

**maquina-c** — servidor já encerrado: `python client.py --host 172.28.0.10 --demo --timeout 1000 --retries 2`

```
cliente em maquina-c (172.28.0.12) -> tcp://172.28.0.10:4567
  ! sem resposta em 1000 ms (tentativa 1/2); recriando o socket REQ
  ! sem resposta em 1000 ms (tentativa 2/2); recriando o socket REQ
  ! servidor parece fora do ar; desistindo desta requisição
  ! demo interrompida
```

### 8.4 Saídas — PUB/SUB

**maquina-a** — `python publisher.py --intervalo 0.5 --limite 26 --rodadas 9`

```
[maquina-a] (172.28.0.10) publicando em tcp://*:4569 a cada 0.5s (ALERTA se TEMP > 26.0)
  pub: TIME {"origem": "maquina-a", "seq": 1, "ts": 1794488629.3338525, "hora": "Thu Nov 12 13:03:49 2026"}
  pub: TEMP {"origem": "maquina-a", "seq": 1, "ts": 1794488629.334374, "valor": 22.7, "unidade": "C"}
  pub: TIME {"origem": "maquina-a", "seq": 2, "ts": 1794488629.835139, "hora": "Thu Nov 12 13:03:49 2026"}
  pub: TEMP {"origem": "maquina-a", "seq": 2, "ts": 1794488629.8355143, "valor": 25.2, "unidade": "C"}
  pub: TIME {"origem": "maquina-a", "seq": 3, "ts": 1794488630.335751, "hora": "Thu Nov 12 13:03:50 2026"}
  pub: TEMP {"origem": "maquina-a", "seq": 3, "ts": 1794488630.335925, "valor": 25.6, "unidade": "C"}
  pub: TIME {"origem": "maquina-a", "seq": 4, "ts": 1794488630.8363924, "hora": "Thu Nov 12 13:03:50 2026"}
  pub: TEMP {"origem": "maquina-a", "seq": 4, "ts": 1794488630.8369923, "valor": 28.4, "unidade": "C"}
  pub: ALERTA {"origem": "maquina-a", "seq": 1, "ts": 1794488630.837123, "msg": "temperatura 28.4 C acima de 26.0", "valor": 28.4}
  ...
  pub: TIME {"origem": "maquina-a", "seq": 9, "ts": 1794488633.339545, "hora": "Thu Nov 12 13:03:53 2026"}
  pub: TEMP {"origem": "maquina-a", "seq": 9, "ts": 1794488633.33972, "valor": 28.9, "unidade": "C"}
  pub: ALERTA {"origem": "maquina-a", "seq": 3, "ts": 1794488633.3397675, "msg": "temperatura 28.9 C acima de 26.0", "valor": 28.9}
[maquina-a] publisher encerrado após 9 rodadas; enviados {'TIME': 9, 'TEMP': 9, 'ALERTA': 3}
```

**maquina-b** — `python subscriber.py --topics TIME,ALERTA -n 8` (não recebe `TEMP`):

```
subscriber em maquina-b (172.28.0.11) -> tcp://maquina-a:4569 tópicos=['TIME', 'ALERTA'] n=8
[  1] TIME   de maquina-a via 172.28.0.10 seq=1 {'hora': 'Thu Nov 12 13:03:49 2026'}
[  2] TIME   de maquina-a via 172.28.0.10 seq=2 {'hora': 'Thu Nov 12 13:03:49 2026'}
[  3] TIME   de maquina-a via 172.28.0.10 seq=3 {'hora': 'Thu Nov 12 13:03:50 2026'}
[  4] TIME   de maquina-a via 172.28.0.10 seq=4 {'hora': 'Thu Nov 12 13:03:50 2026'}
[  5] ALERTA de maquina-a via 172.28.0.10 seq=1 {'msg': 'temperatura 28.4 C acima de 26.0', 'valor': 28.4}
[  6] TIME   de maquina-a via 172.28.0.10 seq=5 {'hora': 'Thu Nov 12 13:03:51 2026'}
[  7] TIME   de maquina-a via 172.28.0.10 seq=6 {'hora': 'Thu Nov 12 13:03:51 2026'}
[  8] TIME   de maquina-a via 172.28.0.10 seq=7 {'hora': 'Thu Nov 12 13:03:52 2026'}
subscriber encerrado após 8 mensagens
```

**maquina-c** — `python subscriber.py --host 172.28.0.10 --topics TEMP -n 8` (só `TEMP`):

```
subscriber em maquina-c (172.28.0.12) -> tcp://172.28.0.10:4569 tópicos=['TEMP'] n=8
[  1] TEMP   de maquina-a via 172.28.0.10 seq=1 {'valor': 22.7, 'unidade': 'C'}
[  2] TEMP   de maquina-a via 172.28.0.10 seq=2 {'valor': 25.2, 'unidade': 'C'}
[  3] TEMP   de maquina-a via 172.28.0.10 seq=3 {'valor': 25.6, 'unidade': 'C'}
[  4] TEMP   de maquina-a via 172.28.0.10 seq=4 {'valor': 28.4, 'unidade': 'C'}
[  5] TEMP   de maquina-a via 172.28.0.10 seq=5 {'valor': 25.5, 'unidade': 'C'}
[  6] TEMP   de maquina-a via 172.28.0.10 seq=6 {'valor': 24.9, 'unidade': 'C'}
[  7] TEMP   de maquina-a via 172.28.0.10 seq=7 {'valor': 25.3, 'unidade': 'C'}
[  8] TEMP   de maquina-a via 172.28.0.10 seq=8 {'valor': 26.7, 'unidade': 'C'}
subscriber encerrado após 8 mensagens
```

**Slow joiner entre máquinas** — subscriber de maquina-b já rodando
(`-n 0`); maquina-a publica 5 rodadas sem esperar:

```
# maquina-a: python publisher.py --intervalo 0 --rodadas 5 --quieto
[maquina-a] (172.28.0.10) publicando em tcp://*:4569 a cada 0.0s (ALERTA se TEMP > 30.0)
[maquina-a] publisher encerrado após 5 rodadas; enviados {'TIME': 5, 'TEMP': 5, 'ALERTA': 1}

# maquina-b: python subscriber.py -n 0
subscriber em maquina-b (172.28.0.11) -> tcp://maquina-a:4569 tópicos=TODOS n=infinito
subscriber encerrado após 0 mensagens
```

As 11 mensagens foram publicadas antes de a reconexão/assinatura de maquina-b
chegar a maquina-a, e todas se perderam.

### 8.5 Saídas — Pipeline

Fontes e sink em maquina-a; workers em maquina-b (via `ZMQ_SRC_HOSTS`/`ZMQ_SINK_HOST`)
e maquina-c (via `--src-hosts 172.28.0.10 --sink-host 172.28.0.10`). Mesmo
lote (sementes 101 e 102) nas três rodadas; os workers são acrescentados um a
um sem reiniciar os anteriores.

**Rodada com 1 worker** (worker 1 em maquina-b) — saída completa de maquina-a:

```
fonte 1 em maquina-a (172.28.0.10): tcp://*:4571, sink em localhost:4570
fonte 1: 20 tarefas enviadas (carga total 953)
fonte 2 em maquina-a (172.28.0.10): tcp://*:4572, sink em localhost:4570
fonte 2: 20 tarefas enviadas (carga total 1211)
sink em maquina-a (172.28.0.10): tcp://*:4570, aguardando 2 fonte(s)
  fonte 1 (maquina-a, 127.0.0.1) começou: 20 tarefas
  fonte 2 (maquina-a, 127.0.0.1) começou: 20 tarefas

===== resumo do lote =====
tarefas concluídas: 40
por worker:
  worker 1 (maquina-b, 172.28.0.11):  40 tarefas, carga  2164  [fonte 1: 20, fonte 2: 20]
por fonte:
  fonte 1: 20 de 20
  fonte 2: 20 de 20
TEMPO TOTAL DO LOTE: 21.66 s com 1 worker(s)
```

(As fontes aparecem como `127.0.0.1` para o sink porque estão no mesmo
container que ele; os workers aparecem com o IP da sua máquina.)

**Rodada com 2 workers** (+ worker 2 em maquina-c):

```
===== resumo do lote =====
tarefas concluídas: 40
por worker:
  worker 1 (maquina-b, 172.28.0.11):  20 tarefas, carga   992  [fonte 1: 10, fonte 2: 10]
  worker 2 (maquina-c, 172.28.0.12):  20 tarefas, carga  1172  [fonte 1: 10, fonte 2: 10]
por fonte:
  fonte 1: 20 de 20
  fonte 2: 20 de 20
TEMPO TOTAL DO LOTE: 11.68 s com 2 worker(s)
```

**Rodada com 3 workers** (+ worker 3 em maquina-b):

```
===== resumo do lote =====
tarefas concluídas: 40
por worker:
  worker 1 (maquina-b, 172.28.0.11):  14 tarefas, carga   827  [fonte 1: 7, fonte 2: 7]
  worker 2 (maquina-c, 172.28.0.12):  12 tarefas, carga   612  [fonte 1: 6, fonte 2: 6]
  worker 3 (maquina-b, 172.28.0.11):  14 tarefas, carga   725  [fonte 1: 7, fonte 2: 7]
por fonte:
  fonte 1: 20 de 20
  fonte 2: 20 de 20
TEMPO TOTAL DO LOTE: 8.28 s com 3 worker(s)
```

Início dos logs dos workers (`via` = IP da fonte, maquina-a):

```
# maquina-b
worker 1 em maquina-b (172.28.0.11) iniciado: 2 fonte(s) em ['maquina-a'], sink em maquina-a
worker 1 recebeu carga  75 da fonte 1 (tarefa 1) via 172.28.0.10
worker 1 recebeu carga  25 da fonte 1 (tarefa 2) via 172.28.0.10
worker 1 recebeu carga  70 da fonte 1 (tarefa 3) via 172.28.0.10
worker 1 recebeu carga  46 da fonte 1 (tarefa 4) via 172.28.0.10
worker 1 recebeu carga  60 da fonte 1 (tarefa 5) via 172.28.0.10

# maquina-c
worker 2 em maquina-c (172.28.0.12) iniciado: 2 fonte(s) em ['172.28.0.10'], sink em 172.28.0.10
worker 2 recebeu carga  87 da fonte 2 (tarefa 2) via 172.28.0.10
worker 2 recebeu carga  46 da fonte 2 (tarefa 4) via 172.28.0.10
worker 2 recebeu carga  71 da fonte 2 (tarefa 6) via 172.28.0.10
worker 2 recebeu carga  85 da fonte 2 (tarefa 8) via 172.28.0.10
worker 2 recebeu carga  24 da fonte 2 (tarefa 10) via 172.28.0.10
```

| Workers | Local (seção 7) | Docker (3 máquinas) |
|---|---|---|
| 1 | 21,60 s | 21,66 s |
| 2 | 10,96 s | 11,68 s |
| 3 | 8,27 s | 8,28 s |

### 8.6 Observações

- **A comunicação atravessou a rede**: o servidor viu as requisições chegando de
  `172.28.0.11` e `172.28.0.12`, os subscribers e workers receberam de
  `172.28.0.10`, e o sink identificou cada worker pelo IP da sua máquina.
- **As duas formas de configuração funcionaram**: maquina-b achou A só pela
  variável de ambiente (pelo nome `maquina-a`, resolvido pelo DNS interno do
  Docker); maquina-c, pelo argumento `--host`/`--src-hosts` com o IP.
- **Tempos praticamente iguais aos locais**: o "trabalho" dos workers é um
  `sleep`, e a latência da rede bridge é de microssegundos, desprezível perto
  dos ~0,5 s de cada tarefa. A diferença na rodada de 2 workers (11,68 × 10,96 s)
  vem da divisão das tarefas, não da rede: em cada execução o round-robin
  entregou cargas diferentes a cada worker (maior carga 1172 no Docker e 1095 no
  teste local), e o tempo acompanha essa maior carga nos dois casos.
- **Slow joiner**: aconteceu igual entre máquinas (0 de 11 mensagens).
- **Limitações da simulação**: os containers compartilham o mesmo kernel e a
  mesma CPU do computador hospedeiro e não há perda nem atraso de rede
  relevantes. Numa implantação real na AWS (ver [AWS.md](AWS.md)) é preciso
  ainda liberar as portas no Security Group, e a latência entre instâncias é
  maior (ainda assim pequena dentro da mesma zona de disponibilidade).
