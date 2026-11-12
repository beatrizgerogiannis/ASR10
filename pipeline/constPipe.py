# Valores padrão do padrão pipeline (baseado em 04-24/constPipe.py).
# Podem ser sobrescritos por variável de ambiente ou por argumento de linha de comando.
#
# No original havia SRC1/SRC2 e PORT1/PORT2 fixos. Aqui o número de fontes é
# configurável: a fonte i usa a porta SRC_BASE_PORT + i - 1 (4571, 4572, 4573...).
import os

SRC_HOSTS     = os.environ.get("ZMQ_SRC_HOSTS", "localhost")  # lista "ip1,ip2"; um só = todas as fontes no mesmo host
SINK_HOST     = os.environ.get("ZMQ_SINK_HOST", "localhost")
SINK_PORT     = os.environ.get("ZMQ_SINK_PORT", "4570")
SRC_BASE_PORT = int(os.environ.get("ZMQ_SRC_BASE_PORT", "4571"))


def porta_da_fonte(i):
    """Porta usada pela fonte de número i (1, 2, 3...)."""
    return SRC_BASE_PORT + int(i) - 1
