from static import DATABASE


class RequisitoCobertura:


    def __init__(self, ConnDB):
        self.ConnDB = ConnDB
        self.database = DATABASE

    # Criar classe
    def view_table(self, table='Requisito_Cobertura'):
        query = f'SELECT * from {self.database}.{table}'
        return self.ConnDB.select_query(query)

    def insert_data(self, req_id, requirement):
        query_insert = f'INSERT INTO {self.database}.Requisito_Cobertura (Id_Requisito, Requisito) VALUES (%s, %s)'
        val = (req_id, requirement)
        self.ConnDB.alter_table(query_insert, val)

    def remove_requirement(self, req_id):
        query = f'DELETE FROM {self.database}.Requisito_Cobertura WHERE Id_Requisito = %s'
        val = (req_id,)
        self.ConnDB.alter_table(query, val)

    def update_requirement(self, req_id, requirement):
        query = f'UPDATE {self.database}.Requisito_Cobertura SET Requisito = %s WHERE Id_Requisito = %s'
        val = (requirement, req_id)
        self.ConnDB.alter_table(query, val)

    def get_requirement_id(self):
        query = f'SELECT Id_Requisito from {self.database}.Requisito_Cobertura ORDER BY Id_Requisito DESC LIMIT 1;'
        df = self.ConnDB.select_query(query)
        if df.empty:
            return 0
        return int(df['Id_Requisito'].values[0])

    def delete_table(self):
        query = f'DELETE FROM {self.database}.Requisito_Cobertura'
        self.ConnDB.select_query(query)