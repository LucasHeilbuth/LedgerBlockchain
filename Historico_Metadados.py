from static import (DATABASE, LIMIT_BLOCK_SIZE, LIMIT_DELTA_TIME_MIN, COLUMNS_HIST_METADADOS, VALUES_HIST_METADADOS)
from datetime import datetime
import pandas as pd

class HistoricoMetadados:


    def __init__(self, ConnDB):
        self.ConnDB = ConnDB
        self.database = DATABASE

    def get_block_id(self):
        query = 'SELECT Id_Bloco FROM Report_Brain.Historico_Metadados ORDER BY Id_Bloco DESC LIMIT 1'
        id_bloco = self.ConnDB.select_query(query).values
        if id_bloco.size == 0:
            return 0
        return self.ConnDB.select_query(query).values[0][0]

    def get_size_block(self, block):
        query = f'SELECT DISTINCT COUNT(Id_Transacao) AS Tamanho_Bloco FROM Report_Brain.Historico_Metadados WHERE Id_Bloco = {block}'
        return self.ConnDB.select_query(query).values[0][0]

    def get_last_time(self, block):
        query = f'SELECT Tempo_Transacao FROM Report_Brain.Historico_Metadados WHERE Id_Bloco = {block} ORDER BY Id_Transacao ASC LIMIT 1'
        if self.ConnDB.select_query(query).empty:
            return 0
        return self.ConnDB.select_query(query).values[0][0]

    def get_time_transaction(self):
        time_now = datetime.now()
        return time_now.strftime('%Y-%m-%d %H:%M:%S')

    def delta_time(self, block_id):
        last_time = self.get_last_time(block_id)
        t1 = pd.Timestamp(last_time)
        time_now = datetime.now()
        delta_time = time_now - t1
        return delta_time.total_seconds()/60

    def get_user(self):
        query = 'SELECT current_user()'
        return self.ConnDB.select_query(query).values[0][0]

    def get_new_block_id(self):
        block_id = self.get_block_id()
        size_block = self.get_size_block(block_id)
        delta_time = self.delta_time(block_id)
        if block_id == 0:
            return 1
        if size_block >= LIMIT_BLOCK_SIZE or delta_time >= LIMIT_DELTA_TIME_MIN:
            return block_id + 1
        return block_id

    def insert_data(self, id_transaction):
        block_id = int(self.get_new_block_id())
        user = self.get_user()
        time_transaction = self.get_time_transaction()
        query = f'INSERT INTO {self.database}.Historico_Metadados {COLUMNS_HIST_METADADOS} VALUES {VALUES_HIST_METADADOS}'
        val = (block_id, id_transaction, time_transaction, user)
        self.ConnDB.alter_table(query, val)

    def get_last_valid_transaction(self, block_id):
        query = f'SELECT Id_Transacao FROM {self.database}.Historico_Metadados WHERE Id_Bloco = {block_id-1} ORDER BY Id_Transacao DESC LIMIT 1'
        return self.ConnDB.select_query(query).values[0][0]

    def remove_transaction_retrived(self, last_id_transaction_valid):
        query = f'DELETE FROM {self.database}.Historico_Metadados WHERE Id_Transacao > {last_id_transaction_valid}'
        self.ConnDB.select_query(query)

    def delete_table(self):
        query = f'DELETE FROM {self.database}.Historico_Metadados'
        self.ConnDB.select_query(query)