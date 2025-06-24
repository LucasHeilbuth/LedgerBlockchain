from Operacao_Ledger import OperacaoLedger
from Transactions import Transactions
from Requisito_Cobertura import RequisitoCobertura
from static import (HOST, DATABASE, USER, PASSWORD)
from ConnDB import ConnectDB
from Historico_Metadados import HistoricoMetadados
from Historico_Hash import HistoricoHash
import time

# Manage the classes to execute the history tables
class ManagerLedger:
    
    def __init__(self, ConnDB):
        self.operacao_ledger = OperacaoLedger(ConnDB)
        self.requisito_cobertura = RequisitoCobertura(ConnDB)
        self.transactions = Transactions(ConnDB)
        self.hist_metadados = HistoricoMetadados(ConnDB)
        self.hist_hash = HistoricoHash(ConnDB)

    def insert_data(self, requirement):
        id_req = self.requisito_cobertura.get_requirement_id() + 1
        id_transaction = self.operacao_ledger.get_id_transaction() + 1
        self.requisito_cobertura.insert_data(id_req, requirement)
        self.operacao_ledger.insert_data(id_req, requirement, id_transaction)
        self.hist_metadados.insert_data(id_transaction)
        self.hist_hash.update_data()

    def remove_data(self, req_id):
        id_transaction = self.operacao_ledger.get_id_transaction() + 1
        self.operacao_ledger.remove_data(req_id, id_transaction)
        self.requisito_cobertura.remove_requirement(req_id)
        self.transactions.insert_transaction(req_id, id_transaction)
        self.hist_metadados.insert_data(id_transaction)
        self.hist_hash.update_data()

    def update_data(self, req_id, requirement):
        id_transaction = self.operacao_ledger.get_id_transaction() + 1
        self.requisito_cobertura.update_requirement(req_id, requirement)
        self.operacao_ledger.update_data(req_id, id_transaction)
        self.transactions.insert_transaction(req_id, id_transaction)
        self.hist_metadados.insert_data(id_transaction)
        self.hist_hash.update_data()

    #Simulate tampering a historic transaction
    def tamper_transaction(self, id_transaction, requirement):
        self.operacao_ledger.tamper_transaction(id_transaction, requirement)

    def validate_hash(self, retrieve=False):
        block_id = self.hist_hash.validate_table()
        if block_id is None or not retrieve:
            return block_id
        last_id_transaction_valid = self.hist_metadados.get_last_valid_transaction(block_id)
        df_requirement_retrieve = self.operacao_ledger.retrieve_requirements(last_id_transaction_valid)
        # Update table Requisito_Cobertura
        df_requirement_retrieve.to_sql(name='Requisito_Cobertura', con=self.operacao_ledger.ConnDB.engine,
                                       if_exists='replace', index=False)
        self.operacao_ledger.remove_transaction_retrived(last_id_transaction_valid)
        self.hist_metadados.remove_transaction_retrived(last_id_transaction_valid)
        self.hist_hash.remove_transaction_retrived(block_id)
        self.transactions.remove_transaction_retrived(last_id_transaction_valid)

    def delete_tables(self):
        self.requisito_cobertura.delete_table()
        self.operacao_ledger.delete_table()
        self.transactions.delete_table()
        self.hist_metadados.delete_table()
        self.hist_hash.delete_table()

if __name__ == '__main__':
    ConnDB = ConnectDB(USER, PASSWORD, HOST, DATABASE)
    manager_ledger = ManagerLedger(ConnDB)
    for block_tamper in range(21,101):
        times_list = []
        for amostra in range(10):
            manager_ledger.delete_tables()
            for i in range (1,3*block_tamper+1):
                manager_ledger.insert_data('Novo Req')
            start_time = time.time()
            manager_ledger.tamper_transaction(block_tamper*3, 'Alterar Req')
            manager_ledger.validate_hash()
            times_list.append(time.time() - start_time)
        print(f'Tempo Bloco {block_tamper}', sum(times_list)/len(times_list))
    ConnDB.close_connection()


#Simulate inserting new data
"""
    manager_ledger.delete_tables()
    for i in range(1, 100):
        manager_ledger.insert_data(f'Req {i}')
    manager_ledger.remove_data(3)
    manager_ledger.remove_data(7)
    manager_ledger.update_data(1, 'Requisito 1')
    manager_ledger.update_data(5, 'Requisito 5')
    manager_ledger.insert_data(f'Novo Req')
    manager_ledger.insert_data(f'Novo Req1')
"""