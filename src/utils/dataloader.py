import coloredlogs
import logging
from box import Box
import pandas as pd

log = logging.getLogger(__name__)


class DataLoader:
    def __init__(self, config: Box):
        self.separator = config.data.separator
        self.folder_path = config.data.folder_path

    def get_data(self, filename: str, dtype: dict = None):
        log.info(f"Started loading data source - {filename}")
        data = pd.read_csv(f'{self.folder_path}/{filename}', sep=self.separator, dtype=dtype).applymap(
            lambda x: x.strip() if isinstance(x, str) else x)
        return data.loc[:, ~data.columns.str.contains('^Unnamed')]

    def write_excel(self, results: [dict], filename: str):
        # Write outputs to excel file
        log.info(f"Started writing excel file - {filename}")
        with pd.ExcelWriter(f'{self.folder_path}/{filename}') as writer:
            for result in results:
                result["value"].to_excel(writer, sheet_name=result["sheet_name"], index=False)
