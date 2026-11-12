# Valores padrão do padrão publish-subscribe (baseado em 04-23/constPS.py).
# Podem ser sobrescritos por variável de ambiente ou por argumento de linha de comando.
import os

HOST = os.environ.get("ZMQ_HOST", "localhost")   # onde o SUBSCRIBER conecta
PORT = os.environ.get("ZMQ_PORT", "4569")        # 4567/4568 ficam para o reqrep
