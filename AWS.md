# Passo a passo: execução em máquinas diferentes na AWS (EC2)

> **Status:** este guia **não foi executado** neste trabalho. Os testes em
> máquinas diferentes foram feitos com Docker (três containers numa rede própria),
> com os mesmos programas e a mesma ordem de comandos — ver a seção 8 do
> [README](README.md). Este documento descreve como fazer a implantação
> equivalente em instâncias EC2; nos comandos, `<IP_A>` e `<IP_PUBLICO_...>`
> são os IPs que a AWS atribuir às suas instâncias.

Este guia monta 3 instâncias EC2 na mesma VPC e roda os três padrões entre elas,
usando os **IPs privados**. Distribuição sugerida:

| Máquina | Papel em REQ/REP | Papel em PUB/SUB | Papel no pipeline |
|---|---|---|---|
| **A** | `server.py` | `publisher.py` | `tasksrc.py 1` e `2`, `sink.py` |
| **B** | `client.py` | `subscriber.py` | `taskwork.py 1` (e `3`) |
| **C** | `client.py` | `subscriber.py` | `taskwork.py 2` |

Portas usadas (todas TCP):

| Porta | Uso | Quem faz bind |
|---|---|---|
| 4567, 4568 | REQ/REP | servidor (A) |
| 4569 | PUB/SUB | publisher (A) |
| 4570 | sink do pipeline | sink (A) |
| 4571, 4572, 4573 | fontes 1, 2, 3 do pipeline | `tasksrc.py` (A) |

Só a máquina A precisa *receber* conexões nessas portas; B e C apenas conectam.

---

## 1. Criar as instâncias

1. Console da AWS → **EC2** → **Launch instances**.
2. **Name**: `zmq-A` (depois repita para `zmq-B` e `zmq-C`, ou use *Number of instances = 3* e renomeie).
3. **AMI**: Ubuntu Server 24.04 LTS.
4. **Instance type**: `t2.micro` ou `t3.micro` (elegíveis ao free tier) bastam.
5. **Key pair**: crie ou escolha um par de chaves (`.pem`) — você vai usá-lo no SSH.
6. **Network settings** → *Edit*:
   - **VPC**: a VPC padrão serve; **as três instâncias devem ficar na mesma VPC**
     (de preferência na mesma subnet).
   - **Auto-assign public IP**: *Enable* (só para você conseguir fazer SSH).
   - **Firewall (security groups)**: *Create security group*, nome `zmq-sg`.
     Deixe por enquanto só a regra de SSH (porta 22) com **Source type = My IP**.
7. **Launch instance**. Para as outras duas, escolha *Select existing security group* → `zmq-sg`.

## 2. Liberar as portas do ZeroMQ no Security Group

EC2 → **Security Groups** → `zmq-sg` → **Edit inbound rules** → *Add rule*:

| Type | Protocol | Port range | Source | Descrição |
|---|---|---|---|---|
| SSH | TCP | 22 | My IP | acesso SSH só do seu IP |
| Custom TCP | TCP | 4567-4573 | `zmq-sg` (o próprio grupo, `sg-xxxx`) | ZeroMQ entre as instâncias |

Colocar o **próprio security group como origem** significa: "aceitar nessas portas
apenas tráfego vindo de instâncias que também estão no `zmq-sg`". Assim as portas
não ficam abertas para a internet. (Se quiser testar a partir do seu computador,
adicione outra regra 4567-4573 com *Source = My IP* e use o IP **público** de A.)

Anote o **Private IPv4 address** de cada instância (aba *Details* da instância).
No restante do guia:

```
IP_A = IP privado da máquina A   (ex.: 172.31.10.20)
IP_B = IP privado da máquina B
IP_C = IP privado da máquina C
```

## 3. Preparar cada máquina (repita em A, B e C)

No seu computador:

```bash
chmod 400 minha-chave.pem
ssh -i minha-chave.pem ubuntu@<IP_PUBLICO_DA_MAQUINA>
```

Na instância:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git
git clone https://github.com/beatrizgerogiannis/ASR10.git zmq
cd zmq
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -c "import zmq; print(zmq.zmq_version())"   # deve imprimir 4.3.x
hostname -I                                         # confere o IP privado
```

Dica: abra **dois terminais SSH** na máquina A (ou use `tmux`), porque ela roda
mais de um processo ao mesmo tempo. Em todos os terminais, faça
`cd ~/zmq && source .venv/bin/activate` antes dos comandos abaixo.

Para não repetir o IP em todo comando, em B e C você pode exportar:

```bash
export ZMQ_HOST=<IP_A>        # usado por reqrep/ e pubsub/
export ZMQ_SRC_HOSTS=<IP_A>   # usado por pipeline/ (fontes)
export ZMQ_SINK_HOST=<IP_A>   # usado por pipeline/ (sink)
```

Os comandos abaixo usam `--host` explicitamente, para ficar claro.

Teste rápido de conectividade (com o servidor de A já rodando, passo 4.1):

```bash
# em B
nc -zv <IP_A> 4567
```

---

## 4. Ordem dos comandos

### 4.1 Request-reply (REQ/REP)

```bash
# A (terminal 1)
cd ~/zmq/reqrep
python server.py --name A

# B
cd ~/zmq/reqrep
python client.py --host <IP_A> --ports 4567,4568 --demo

# C (ao mesmo tempo que B, se quiser)
cd ~/zmq/reqrep
python client.py --host <IP_A> --ports 4568       # modo interativo: add 2 3, upper ola, sair

# Teste de timeout: em A, pare o servidor (Ctrl+C) e em B rode
python client.py --host <IP_A> --demo --timeout 1000 --retries 3
# Depois suba o servidor de novo em A e repita: o cliente volta a ser atendido.

# Para encerrar o servidor a partir de B:
python client.py --host <IP_A> --demo --stop
```

### 4.2 Publish-subscribe (PUB/SUB)

```bash
# A
cd ~/zmq/pubsub
python publisher.py --intervalo 1 --limite 28

# B: só hora e alertas
cd ~/zmq/pubsub
python subscriber.py --host <IP_A> --topics TIME,ALERTA -n 10

# C: só temperatura, sem limite de mensagens (Ctrl+C para sair)
cd ~/zmq/pubsub
python subscriber.py --host <IP_A> --topics TEMP -n 0
```

Para ver o *slow joiner* entre máquinas: deixe `subscriber.py --host <IP_A> -n 0`
rodando em B e, em A, rode `python publisher.py --intervalo 0 --rodadas 5`.
O subscriber normalmente não recebe nada (ou recebe só as últimas mensagens).

### 4.3 Pipeline (PUSH/PULL) — comparando 1, 2 e 3 workers

Os workers podem ser iniciados **antes** das fontes: o `connect` do ZeroMQ fica
tentando reconectar sozinho até a fonte existir.

**Rodada com 1 worker**

```bash
# B
cd ~/zmq/pipeline
python taskwork.py 1 --fontes 2 --src-hosts <IP_A> --sink-host <IP_A>

# A (depois que o worker de B estiver rodando)
cd ~/zmq/pipeline
python sink.py --fontes 2 &
python tasksrc.py 1 --tarefas 20 --seed 101 --espera 2 &
python tasksrc.py 2 --tarefas 20 --seed 102 --espera 2
wait          # o sink imprime o resumo e termina sozinho
```

**Rodada com 2 workers** — deixe o worker 1 de B rodando e acrescente em C:

```bash
# C
cd ~/zmq/pipeline
python taskwork.py 2 --fontes 2 --src-hosts <IP_A> --sink-host <IP_A>
```

e repita em A exatamente os mesmos comandos da rodada anterior (mesmas sementes
= mesmas cargas).

**Rodada com 3 workers** — acrescente um segundo worker em B (outro terminal):

```bash
# B (terminal 2)
python taskwork.py 3 --fontes 2 --src-hosts <IP_A> --sink-host <IP_A>
```

e repita os comandos de A. Anote o `TEMPO TOTAL DO LOTE` de cada rodada.

Ao final, Ctrl+C nos workers.

## 5. Coletar as saídas

Para registrar os resultados (por exemplo, para comparar com os da seção 8 do
README, obtidos com Docker), rode os comandos com `| tee saida.txt` e depois
copie o arquivo para o seu computador:

```bash
scp -i minha-chave.pem ubuntu@<IP_PUBLICO_A>:~/zmq/pipeline/saida.txt .
```

## 6. Encerrar

- Ctrl+C em todos os processos.
- No console: **Instances** → selecione as três → **Instance state → Terminate**
  (ou *Stop*, se for usar de novo), para não gerar custo.

## Problemas comuns

| Sintoma | Causa provável |
|---|---|
| Cliente/subscriber/worker fica parado sem receber nada | Regra 4567-4573 ausente no Security Group, ou IP errado (use o **privado**, não o público, entre instâncias da mesma VPC). |
| `Address already in use` | Já existe um processo com bind nessa porta: `ss -ltnp \| grep 45` e encerre-o. |
| Só um worker recebe tarefas | Os outros workers conectaram depois que as fontes já tinham distribuído tudo. Inicie os workers antes ou aumente `--espera`. |
| Subscriber perde as primeiras mensagens | *Slow joiner*, comportamento normal do PUB/SUB (ver README). |
