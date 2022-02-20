import coloredlogs
import logging
import yaml
import pandas as pd

from box import Box
from src.analysis.dictionarygenerator import DictionaryGenerator
from src.preperation.dataprep import DataPrep
from src.utils.dataloader import DataLoader

log = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG')


def main():
    log.info("Pipeline started")
    filename = "../config.yml"
    try:
        config = Box.from_yaml(filename=filename, Loader=yaml.FullLoader)
    except FileNotFoundError:
        log.error("file {} does not exist".format(filename))

    dp = DataPrep(config)
    data_list = dp.load_data()

    dg = DictionaryGenerator(config)
    result = dg.all_dictionary_handler(data_list)

    with pd.option_context('display.max_rows', 2, 'display.max_columns', None, 'precision', 2):
        print(result)

    DataLoader(config).write_excel(results=[{
        "sheet_name": "DataDictionary",
        "value": result
    }], filename=config.data.output_filename)


if __name__ == '__main__':
    main()
