# Selecionar o maior ID_Transacao que seja menor que o Id_Transacao do Remove
from ConnDB import ConnectDB
from static import (HOST, DATABASE, USER, PASSWORD, COLUMNS_TRANSACTION, VALUES_TRANSACTION)
ConnDB = ConnectDB(USER, PASSWORD, HOST, DATABASE)

class Transactions:


    def __init__(self, ConnDB):
        self.ConnDB = ConnDB
        self.database = DATABASE

    def insert_transaction(self, id_req, id_transaction_end):
        query_start = (f'SELECT Id_Transacao, Id_Operacao from {self.database}.Operacao_Ledger '
                       f'where Id_Requisito = {id_req} AND Id_Transacao < {id_transaction_end} '
                       f'ORDER BY Id_Transacao DESC LIMIT 1')
        df_start = ConnDB.select_query(query_start)
        df_start_values = list(map(int, df_start.values[0]))
        val_transaction = tuple(df_start_values) + (id_transaction_end, 1)
        query_transaction = f'INSERT INTO Report_Brain.Transacoes {COLUMNS_TRANSACTION} VALUES {VALUES_TRANSACTION}'
        self.ConnDB.alter_table(query_transaction, val_transaction)

    def remove_transaction_retrived(self, last_id_transaction_valid):
        query = (f'DELETE FROM {self.database}.Transacoes WHERE Id_Transacao_Inicio > {last_id_transaction_valid} '
                 f'OR Id_Transacao_Final > {last_id_transaction_valid}')
        self.ConnDB.select_query(query)

    def delete_table(self):
        query = f'DELETE FROM {self.database}.Transacoes'
        self.ConnDB.select_query(query)