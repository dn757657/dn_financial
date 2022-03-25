import qtrade
import pathlib
from datetime import datetime, timedelta
from dn_date_util.utility import DATE_FORMAT, process_window, get_date_res, populate_dates, parse_prefix
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

    def get_txns(self, dates):
        """ dates can have max resolution of monthly dates - API will fail if larger request is made
        args:
            dates           list of tuples of string or datetime dates as format %Y-%m-%d denoting desired window

        returns:
            all account transactions linked to connection token
        """
        all_txns = list()
        for account in self.accounts:
            for date in dates:
                # TODO add loading bar
                # QT Api will fail if future dates are requested
                if date <= datetime.now():
                    date = date.__str__().split()
                    date = date[0]
                    txns = self.conn.get_account_activities(account_id=int(account),
                                                            start_date='2020-11-13',
                                                            end_date=date)
                else:
                    txns = list()

                for txn in txns:
                    all_txns.append(txn)

        return all_txns

    def get_share_balance(self, dates):
        """
        dates can be one tuple or many for single points in time or timeseries of points in time

        takes in str dates as list of tuples indicating a window
        and returns share balances for each holding at the date specified

        returns balances at end date of each period of date tuples
        """

        txns = self.get_txns(dates)
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
                    # elif txn['action'].lower() == 'SELL'.lower():
                    #     print('sell' + str(txn['quantity']) + 'of ' + txn['symbol'])
                    #     print('balance was' + str(period_balances[txn['symbol']]))
                    #     period_balances[txn['symbol']] = period_balances[txn['symbol']] - txn['quantity']
                    #     print('balance is now' + str(period_balances[txn['symbol']]))

            # only append keys with non-zero entries
            balances.append({x: y for x, y in period_balances.items() if y != 0})

        return balances

    # TODO
    # def get_fees(self, dates):
    # TODO
    # def get_dividends(self, dates):

    # def update_qpositions(self, account_id):
    #
    #     qtrade = self.qtrade_connect()
    #     positions = qtrade.get_account_positions(account_id=account_id)
    #     for pos in positions:
    #         update = {key: None for key in self.db.schema['qtrade']}
    #         for key in update.keys():
    #             # db table column names are same as default dictionary names for properties in position
    #             # need to eliminate params that are in positions dictionaries but not in db qtrade table
    #             if key in pos.keys():
    #                 update[key] = pos[key]
    #
    #         update.pop('date')
    #         update_cols = list(update.keys())
    #         update_vals = list(update.values())
    #
    #         query = Query(db=self.db, table=self.qtrade_table,
    #                       in_vals=update_vals, in_cols=update_cols, )
    #
    #         # query = db.insert(table='qtrade', columns=update_cols, values=update_vals)
    #         self.db.conn.cursor().execute(query.build_insert())
    #         self.db.conn.commit()


def __main__():
    tfsa = QTAccount()

    sample_date = datetime.strptime('2020-12-14', DATE_FORMAT)
    # sample_date = datetime.now()
    # TODO break up requests into monthly buckets or else requets fails
    balances = tfsa.get_share_balance([sample_date])
    print('done')
    # end_date = datetime.strptime('2022-02-01', DATE_FORMAT)
    # res = get_date_res(window_unit='week', window_size=1)
    #
    # data = populate_dates(resolution=res, x_max=end_date, x_min=start_date)
    # data_str = list()
    # for datepair in data:
    #     data_str.append((datepair[0].strftime(DATE_FORMAT), datepair[1].strftime(DATE_FORMAT)))
    #
    # txns = tfsa.get_txns(dates=data_str)
    #
    # balances = tfsa.get_share_balance(data_str)
    # for period in balances:
    #     print(period)


if __name__ == '__main__':
    __main__()
