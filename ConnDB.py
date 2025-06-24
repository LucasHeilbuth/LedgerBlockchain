import mysql.connector
import pandas as pd
from sqlalchemy import create_engine


class ConnectDB:

    def __init__(self, user, password, host, database):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.conn = mysql.connector.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database
        )
        self.cursor = self.conn.cursor()
        #string_con = f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}'
        self.engine = create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}')

    def select_query(self, query):
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        self.conn.commit()
        return pd.DataFrame(results, columns=self.cursor.column_names)

    def alter_table(self, query, val):
        if isinstance(val, tuple):
            self.cursor.execute(query, val)
        elif isinstance(val, list):
            self.cursor.executemany(query, val)
        self.conn.commit()

    def close_connection(self):
        self.conn.close()
        self.cursor.close()