import coloredlogs
import logging
import pandas as pd

import src.autodatadictionary.autodatadictionary as ad

log = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG')


def main():
    log.info("Pipeline started")
    titanic = pd.read_csv('https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv')
    df_result = ad.to_dictionary_from_dataframe([titanic])

    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'precision', 2):
        print(df_result)

    file_result = ad.to_dictionary_from_file(['/path/data1.csv', '/path/data2.csv', '/path/dataN.csv'])
    db_result = ad.to_dictionary_from_db(sql_alchemy_connection_string='postgresql://username:password@domain:5432/db')

    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'precision', 2):
        print(file_result)

    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'precision', 2):
        print(db_result)


if __name__ == '__main__':
    main()
