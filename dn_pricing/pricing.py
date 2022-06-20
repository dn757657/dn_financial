import pandas_datareader as datar
from datetime import datetime
import pandas as pd

DATE_FORMAT = '%Y-%m-%d'


def get_price(symbol, date, source='yahoo'):
    if isinstance(date, datetime):
        date = date.__str__()
        date = date.split()[0]

    try:
        price = datar.DataReader(symbol, data_source=source, start=date, end=date)
        price = price.loc[date, 'Close']
        return price

    except ConnectionError:
        print("No price for " + symbol)
        return
    except KeyError:
        return None


def get_latest_price(symbol, source='yahoo', current=False, start=None, end=None):
    """ give a symobl -> get a price """
    try:
        price = datar.DataReader(symbol, source)

        price = price["Close"][price.last_valid_index()]
        return price

    except ConnectionError:
        print("No price for " + symbol)
        return


def get_all_yahoo(symbol):
    d = datar.get_data_yahoo(str(symbol))

    return d


def __main__():
    # print("latest: {}".format(get_latest_price('VFV.TO')))
    #
    # date = datetime.strptime('2020-02-20', DATE_FORMAT)
    # price = get_price(symbol='VFV.TO', date=date)
    # print("{}: {}".format(date, price))

    d = datar.get_data_yahoo('VFV.TO')
    print()


if __name__ == '__main__':
    __main__()