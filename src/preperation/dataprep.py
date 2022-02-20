from box import Box
import pandas as pd

import src.db.postgresql as psql
from src.utils.dataloader import DataLoader


class DataPrep:
    def __init__(self, config: Box):
        self.config = config

    def load_data(self) -> pd.DataFrame:
        if self.config.data.db_connection:
            return self.load_db_data()
        else:
            return self.load_csv_data()

    def load_db_data(self):
        return self.load_postgres_data()

    def load_csv_data(self) -> pd.DataFrame:
        dl = DataLoader(self.config)
        data_list = []
        for key in self.config.data.datasets.keys():
            data = dl.get_data(self.config.data.datasets[key])
            data.attrs["title"] = key
            data.attrs["source"] = self.config.data.datasets[key]
            data_list.append(data)

        return data_list

    def load_postgres_data(self) -> pd.DataFrame:
        postgresql = psql.Postgresql(config=self.config)

        if self.config.db.use_all_tables:
            table_names = postgresql.get_all_table_names()
        else:
            table_names = self.config.db.table_names

        data_list = []
        for table in table_names:
            data = postgresql.read_table(table_name=table)
            data.attrs["source"] = table
            data_list.append(data)

        return data_list
