# Valores padrão do padrão request-reply (baseado em 04-22/constCS.py).
# Podem ser sobrescritos por variável de ambiente ou por argumento de linha de comando.
import os

HOST  = os.environ.get("ZMQ_HOST", "localhost")  # onde o CLIENTE conecta
PORT1 = os.environ.get("ZMQ_PORT1", "4567")
PORT2 = os.environ.get("ZMQ_PORT2", "4568")
