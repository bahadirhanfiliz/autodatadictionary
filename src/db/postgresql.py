import pandas as pd
import logging
from sqlalchemy import create_engine
from box import Box

log = logging.getLogger(__name__)


class Postgresql:
    """
    This class handles database operations related to Postgresql
    """

    def __init__(self, config: Box):
        self.server_name = config.db.connection.server_name
        self.db_name = config.db.connection.db_name
        self.user_name = config.db.connection.user_name
        self.pass_word = config.db.connection.pass_word
        self.port = config.db.connection.port
        self.schema = config.db.connection.schema
        self.connection_string = "postgresql+psycopg2://" + self.user_name + ":" + self.pass_word + "@" + self.server_name + ":" + self.port + "/" + self.db_name
        self.limit = config.db.limit

    def read_table(self, table_name: str) -> pd.DataFrame:
        """
        Reads and returns the table from database
        @param table_name: database table name
        @return: whole table
        """
        log.info(f"Started querying data source - {table_name}")
        try:
            alchemy_engine = create_engine(self.connection_string)
            if isinstance(self.limit, int):
                data = pd.read_sql_query(f"SELECT * FROM {self.schema}.{table_name} LIMIT {self.limit}",
                                         alchemy_engine)
            else:
                data = pd.read_sql_query(f"SELECT * FROM {self.schema}.{table_name}", alchemy_engine)
            return data
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)

    def get_all_table_names(self) -> pd.DataFrame:
        """
        Reads and returns the table from database
        @return: whole table
        """
        try:
            alchemy_engine = create_engine(self.connection_string)
            data = pd.read_sql_query(
                "SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema='{self.schema}' AND table_type='BASE TABLE'",
                alchemy_engine)
            return data["table_name"].to_list()
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)

    def read_sql_query(self, sql_query: str) -> pd.DataFrame:
        """
        Return data based on custom sql query
        @return: dataframe
        """
        try:
            alchemy_engine = create_engine(self.connection_string)
            df = pd.read_sql_query(sql_query, alchemy_engine)
            return df
        except ValueError as vx:
            print(vx)
        except Exception as ex:
            print(ex)
