from static import (COLUMNS_HIST_HASH, VALUES_HIST_HASH, DATABASE, LIMIT_BLOCK_SIZE, LIMIT_DELTA_TIME_MIN)
from Tabela_Digest import TabelaDigest
from hashlib import sha256
from Historico_Metadados import HistoricoMetadados


class HistoricoHash:


    def __init__(self, ConnDB):
        self.ConnDB = ConnDB
        self.database = DATABASE
        self.tabela_digest = TabelaDigest(ConnDB)
        self.historico_metadados = HistoricoMetadados(ConnDB)

    def get_hash_bloco(self, id_bloco):
       df_digest = self.tabela_digest.get_table_digest(id_bloco)
       string_digest = df_digest.to_csv(index=False, header=False)
       hash_digest = sha256(string_digest.encode('utf-8')).hexdigest()
       return hash_digest

    def insert_data(self, id_bloco, tam_bloco):
        previous_hash = self.tabela_digest.get_hash_previous_block(id_bloco)
        hash_bloco = self.get_hash_bloco(id_bloco)
        query = f'INSERT INTO {self.database}.Historico_Hash {COLUMNS_HIST_HASH} VALUES {VALUES_HIST_HASH}'
        val = (int(id_bloco), int(tam_bloco), previous_hash, hash_bloco)
        self.ConnDB.alter_table(query, val)

    def update_hash(self, id_bloco, size_block):
        hash_bloco = self.get_hash_bloco(id_bloco)
        query = f'UPDATE {self.database}.Historico_Hash SET Tamanho_Bloco = {size_block}, Hash_Bloco_Digest = "{hash_bloco}" WHERE Id_Bloco = {id_bloco}'
        self.ConnDB.select_query(query)

    def validate_block(self, block_id):
        query = f'SELECT Hash_Bloco_Digest FROM {self.database}.Historico_Hash WHERE Id_Bloco = {block_id}'
        hash_bloco = self.ConnDB.select_query(query).values[0][0]
        new_hash = self.get_hash_bloco(block_id)
        return new_hash == hash_bloco

    def validate_table(self):
        query = f'SELECT Id_Bloco, Hash_Bloco_Anterior, Hash_Bloco_Digest FROM {self.database}.Historico_Hash'
        df = self.ConnDB.select_query(query)
        hash_bloco_anterior = None
        for block_id, hash_anterior, hash_digest in df.values:
            # Compare if the hash is linked with the previous hash block
            if hash_anterior != hash_bloco_anterior:
                return block_id
            # Check if the hash of the block stored is equal to the hash calculated
            if not self.validate_block(block_id):
                return block_id
            hash_bloco_anterior = hash_digest
        return None

    def remove_transaction_retrived(self, block_id_corrupted):
        query = f'DELETE FROM {self.database}.Historico_Hash WHERE Id_Bloco >= {block_id_corrupted}'
        self.ConnDB.select_query(query)

    def delete_table(self):
        query = f'DELETE FROM {self.database}.Historico_Hash'
        self.ConnDB.select_query(query)

    def update_data(self):
        block_id = self.historico_metadados.get_block_id()
        size_block = self.historico_metadados.get_size_block(block_id)
        delta_time = self.historico_metadados.delta_time(block_id)
        if size_block == 1:
            self.insert_data(block_id, size_block)
        if size_block < LIMIT_BLOCK_SIZE and delta_time < LIMIT_DELTA_TIME_MIN:
            self.update_hash(block_id, size_block)
        if size_block >= LIMIT_BLOCK_SIZE or delta_time >= LIMIT_DELTA_TIME_MIN:
            self.update_hash(block_id, size_block)