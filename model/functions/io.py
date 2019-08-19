import pandas_datareader


def write_symbols_to_csv():
    """
    The generate a csv file including companies' names, symbols and other information. Now we assume the file is
    'Nasdaq Company List'. Because of Nasdaq update the file everyday. So we should run this function everyday to follow
    the official updating.
    :return:
    """
    all_nasdaq = pandas_datareader.get_nasdaq_symbols()
    all_nasdaq.to_csv(path_or_buf='./data/nasdaq_symbols_all.csv')
    return True