import qtrade
import pathlib
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from dn_date_util.utility import DATE_FORMAT, process_window, get_date_res, populate_dates, parse_prefix
from urllib.error import HTTPError
# internal
from sql_queries import Query
# TODO return share count as dictionary time series symbol as key, count as value


class QTAccount:

    def __init__(self, filepath=pathlib.Path(__file__).absolute().parent):
        self.qt_fp = filepath
        self.yaml_path = self.qt_fp.joinpath('access_token.yml')

        self.conn = self.qtrade_connect()

        self.accounts = self.conn.get_account_id()

        # self.txns = list()
        self.positions = list()

        for account in self.accounts:
            self.positions.append(self.conn.get_account_positions(account_id=int(account)))

    def qtrade_connect(self,):
        try:
            conn = qtrade.Questrade(token_yaml=self.yaml_path)
            conn.refresh_access_token(from_yaml=True)
        except:
            access_code = input("please input Questrade API Access token ")
            conn = qtrade.Questrade(access_code=access_code)

        return conn

    def date_2_str(self, date):
        date = date.__str__()
        date = date.split()[0]

        return date

    def get_txn_block(self, account, start, end, all_txns=None):
        """ can only get txns in blocks of one month so here is a dumb function for fetching blocks """
        try:
            block_txns = self.conn.get_account_activities(account_id=int(account),
                                                          start_date=self.date_2_str(date=start),
                                                          end_date=self.date_2_str(date=end))
        except HTTPError:
            block_txns = list()

        if all_txns:
            for txn in block_txns:
                all_txns.append(txn)

            return all_txns

        else:
            return block_txns

    def get_txns(self, date):
        """ dates can have max resolution of monthly dates - API will fail if larger request is made
        args:
            dates           list of tuples of string or datetime dates as format %Y-%m-%d denoting desired window

        returns:
            all account transactions linked to connection token
        """
        acc_creation_date = datetime.strptime('2020-11-12', DATE_FORMAT)
        all_txns = list()
        low_bound = acc_creation_date
        upper_bound = low_bound + relativedelta(months=1) - relativedelta(days=1)

        for account in self.accounts:
            # TODO add loading bar
            # QT Api will fail if future dates are requested
            while upper_bound <= date:
                all_txns = self.get_txn_block(account=account, start=low_bound, end=upper_bound, all_txns=all_txns)

                low_bound = low_bound + relativedelta(months=1)
                upper_bound = low_bound + relativedelta(months=1) - relativedelta(days=1)

            all_txns = self.get_txn_block(account=account, start=low_bound, end=date, all_txns=all_txns)

        return all_txns

    def get_asset_balances(self, dates):
        """
        dates can be one tuple or many for single points in time or timeseries of points in time

        takes in str dates as list of tuples indicating a window
        and returns share balances for each holding at the date specified

        returns balances at end date of each period of date tuples
        """
        final_date = None
        for date in dates:
            if date > datetime.now():
                continue
            else:
                final_date = date
        if not final_date:
            print("All dates are future")
            return

        txns = self.get_txns(final_date)
        balances = list()

        # get amount of each share at the first date, initial amounts
        for date in dates:
            period_balances = dict()
            sample_date = date

            for txn in txns:
                # parse questrade date into date format being used
                txn_date = parse_prefix(txn['settlementDate'], DATE_FORMAT)
                if txn_date <= sample_date:
                    if txn['action'].lower() == 'BUY'.lower() or txn['action'].lower() == 'SELL'.lower():
                        if not txn['symbol'] in period_balances:
                            period_balances[txn['symbol']] = txn['quantity']
                        else:
                            period_balances[txn['symbol']] += txn['quantity']

            # only append keys with non-zero entries
            balances.append({x: y for x, y in period_balances.items() if y != 0})

        all_date_balances = list()
        balances_df = pd.DataFrame()
        for i in range(0, len(balances)):
            date_balances = balances[i]
            date_balances['x'] = dates[i]
            date_balances_df = pd.DataFrame(date_balances, index=[i])

            if balances_df.empty:
                balances_df = date_balances_df
            else:
                balances_df = pd.concat([balances_df, date_balances_df])
        balances_df = balances_df.fillna(0)

        return balances_df


def __main__():
    tfsa = QTAccount()

    start_date = datetime.strptime('2021-01-01', DATE_FORMAT)
    end_date = datetime.strptime('2022-05-01', DATE_FORMAT)
    # sample_date = datetime.now()
    sample_dates = pd.date_range(start=start_date, end=end_date, freq='M')
    balances = tfsa.get_asset_balances(sample_dates)
    print()

if __name__ == '__main__':
    __main__()
