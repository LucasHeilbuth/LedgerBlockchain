from dotenv import load_dotenv, dotenv_values
import os
# loading variables from .env file
load_dotenv()

HOST = 'localhost'
DATABASE = 'Report_Brain'
USER = os.getenv('USER_DB')
PASSWORD = os.getenv('PASSWORD_DB')
OPTIONAL_COLUMNS = ['Id_Titulo', 'Titulo', 'ECU', 'Funcao', 'DiagramaDeSequencia', 'CenarioDoUsuario', 'CasoDeUso']
COLUMNS_OPERATIONAL = "Id_Transacao, Id_Operacao, Tipo_Operacao, Operacao_Ledger)"
COLUMNS_OP_LEDGER = '''(Id_Requisito, Requisito, Id_Titulo, Titulo, ECU, Funcao, DiagramaDeSequencia, CenarioDoUsuario, CasoDeUso, Id_Transacao, Id_Operacao, Tipo_Operacao, Operacao_Ledger)'''
VALUES_LEDGER = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
COLUMNS_TRANSACTION = "(Id_Transacao_Inicio, Id_Transacao_Final, Id_Operacao_Inicio, Id_Operacao_Final)"
VALUES_TRANSACTION = "(%s, %s, %s, %s)"
LIMIT_BLOCK_SIZE = 3
LIMIT_DELTA_TIME_MIN = 10
COLUMNS_HIST_METADADOS = "(Id_Bloco, Id_Transacao, Tempo_Transacao, Usuario)"
VALUES_HIST_METADADOS = "(%s, %s, %s, %s)"
COLUMNS_HIST_HASH = "(Id_Bloco, Tamanho_Bloco, Hash_Bloco_Anterior, Hash_Bloco_Digest)"
VALUES_HIST_HASH = "(%s, %s, %s, %s)"