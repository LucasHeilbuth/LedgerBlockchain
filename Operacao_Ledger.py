from static import (DATABASE, OPTIONAL_COLUMNS, COLUMNS_OPERATIONAL, COLUMNS_OP_LEDGER,
                    VALUES_LEDGER)

class OperacaoLedger:


    def __init__(self, ConnDB):
        self.ConnDB = ConnDB
        self.database = DATABASE

    def get_id_transaction(self):
        query = f'SELECT Id_Transacao from {self.database}.Operacao_Ledger ORDER BY Id_Transacao DESC LIMIT 1;'
        df = self.ConnDB.select_query(query)
        if df.empty:
            return 0
        return int(df['Id_Transacao'].values[0])

    def get_op_insert_ledger(self, column_uses):
        columns_requirement = "(Id_Requisito, Requisito"
        values_query = "(%s, %s, %s, %s, %s, %s"
        for column_name, column_use in zip(OPTIONAL_COLUMNS, column_uses):
            if column_use:
                # Add in query the column
                columns_requirement += ", " + column_name
                values_query += ", %s"
        columns_requirement += ", "
        values_query += ")"
        columns_query = columns_requirement + COLUMNS_OPERATIONAL
        return f'INSERT INTO Report_Brain.Operacao_Ledger {columns_query} VALUES {values_query}'

    def insert_data(self, req_id, requirement, id_transaction):
        columns_uses = [False, False, False, False, False, False, False]
        query_transaction = self.get_op_insert_ledger(columns_uses)
        val = (req_id, requirement, id_transaction, 1, 1, 'Insert')
        self.ConnDB.alter_table(query_transaction, val)

    def remove_data(self, req_id, id_transaction):
        query_line = f'SELECT * FROM {self.database}.Requisito_Cobertura WHERE Id_Requisito = {req_id}'
        df_line = self.ConnDB.select_query(query_line).iloc[0]
        columns_ledger = (id_transaction, 1, 2, 'Remove')
        df_line.values[0] = int(df_line.values[0])
        columns_req = tuple(df_line.values)
        val = columns_req + columns_ledger
        query_op_ledger = f'INSERT INTO {self.database}.Operacao_Ledger {COLUMNS_OP_LEDGER} VALUES {VALUES_LEDGER}'
        self.ConnDB.alter_table(query_op_ledger, val)

    def update_data(self, req_id, id_transaction):
        self.remove_data(req_id, id_transaction)
        query_line = f'SELECT * FROM {self.database}.Requisito_Cobertura WHERE Id_Requisito = {req_id}'
        df_line = self.ConnDB.select_query(query_line).iloc[0]
        columns_ledger = (id_transaction, 2, 1, 'Insert')
        df_line.values[0] = int(df_line.values[0])
        columns_req = tuple(df_line.values)
        val = columns_req + columns_ledger
        query_transaction = f'INSERT INTO {self.database}.Operacao_Ledger {COLUMNS_OP_LEDGER} VALUES {VALUES_LEDGER}'
        self.ConnDB.alter_table(query_transaction, val)

    # Simulacao de adulteracao
    def tamper_transaction(self, id_transaction, requirement):
        query = f'UPDATE {self.database}.Operacao_Ledger SET Requisito = %s WHERE Id_Transacao = %s'
        val = (requirement, id_transaction)
        self.ConnDB.alter_table(query, val)

    def retrieve_requirements(self, last_id_transaction_valid):
        query = f'''SELECT Retrieve.Id_Requisito, Requisito, Id_Titulo, Titulo, ECU, Funcao, DiagramaDeSequencia,
        CenarioDoUsuario, CasoDeUso FROM (SELECT DISTINCT ID_Requisito, MAX(Id_Transacao) AS Last_Id_Transacao 
        FROM {self.database}.Operacao_Ledger WHERE Id_Transacao <= {last_id_transaction_valid} GROUP BY Id_Requisito
        ORDER BY MAX(Id_Transacao) DESC, Id_Requisito) AS Retrieve INNER JOIN 
        (SELECT * FROM {self.database}.Operacao_Ledger WHERE Tipo_Operacao = 1) AS Last_Block
        ON Retrieve.Last_Id_Transacao = Last_Block.Id_Transacao ORDER BY Retrieve.Id_Requisito ASC'''
        return self.ConnDB.select_query(query)

    def remove_transaction_retrived(self, last_id_transaction_valid):
        query = f'DELETE FROM {self.database}.Operacao_Ledger WHERE Id_Transacao > {last_id_transaction_valid}'
        self.ConnDB.select_query(query)

    def delete_table(self):
        query = f'DELETE FROM {self.database}.Operacao_Ledger'
        self.ConnDB.select_query(query)