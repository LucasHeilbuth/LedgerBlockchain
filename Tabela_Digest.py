from static import DATABASE


class TabelaDigest:


    def __init__(self, ConnDB):
        self.ConnDB = ConnDB
        self.database = DATABASE

    def get_op_ledger_hist_meta(self, id_bloco):
        query = f"""SELECT Op_Ledger.Id_Requisito, Op_Ledger.Requisito, Op_Ledger.Id_Titulo, Op_Ledger.Titulo,
        Op_Ledger.ECU, Op_Ledger.CasoDeUso, Op_Ledger.CenarioDoUsuario, Op_Ledger.DiagramaDeSequencia,
        Op_Ledger.Id_Transacao, Hist_Meta.Tempo_Transacao, Hist_Meta.Usuario, Op_Ledger.Id_Operacao, Op_Ledger.Tipo_Operacao
        FROM {DATABASE}.Historico_Metadados AS Hist_Meta LEFT JOIN {DATABASE}.Operacao_Ledger AS Op_Ledger
        ON Hist_Meta.Id_Transacao = Op_Ledger.Id_Transacao WHERE Hist_Meta.Id_Bloco = {id_bloco}"""
        return self.ConnDB.select_query(query)

    def get_hash_previous_block(self, id_bloco):
        query = f'SELECT Hash_Bloco_Digest AS Previous_Hash FROM {self.database}.Historico_Hash WHERE Id_Bloco = {id_bloco - 1}'
        df = self.ConnDB.select_query(query)
        if df.empty:
            return None
        return self.ConnDB.select_query(query).values[0][0]

    def get_table_digest(self, id_bloco):
        df_digest = self.get_op_ledger_hist_meta(id_bloco)
        df_digest['Hash_Bloco_Anterior'] = self.get_hash_previous_block(id_bloco)
        return df_digest