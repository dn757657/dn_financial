import argparse
import json
import web3
import copy
import time
import datetime
from datetime import datetime, timedelta
from dn_date_util.utility import DATE_FORMAT, process_window, get_date_res, populate_dates, parse_prefix

from web3 import Web3
from hexbytes import HexBytes
import etherscan

from dn_pricing.pricing import get_latest_price

# Exports transactions to a JSON file where each line
# contains the data returned from the JSONRPC interface


class cryptoWallet():

    def __init__(self, addy):
        self.w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/80de2f7c4deb45d5a880a822f2bb1e5d'))
        self.addy = addy
        self.etherAPI = 'NC3VB6CBBUCPD7G3QHPRA5PGUZTJA8AYV4'
        self.etherscan = etherscan.Etherscan(self.etherAPI)
        self.eth_txns = self.get_eth_txn()
        # self.eth_balances = self.get_eth_balance()

    def tx_to_json(self, tx):
        result = {}
        for key, val in tx.items():
            if isinstance(val, HexBytes):
                result[key] = val.hex()
            else:
                result[key] = val

        return json.dumps(result)

    def get_eth_balances(self,
                         dates=None):
        """ create dictionary with tokenSymbol as key and token balance as value in token count

        args:
            dates           list of datetime objects where balances are sampled
            currency        string of currency symbol of desired output

        note that timestamps on the ethereum blockchain are UNIX timstamps

        returns dictionary

        """
        # TODO intialize dictionary with all dates, then sort txns into the buckets?
        # TODO theres gotta be a better way to sort this shit than looping each time
        # TODO same with questrade func
        balances = list()
        # convert datetime objects to UNIX time stamp for comparison with txn timestamps
        # if no dates passed use present date
        if not dates:
            dates = list()
            t_sample = datetime.datetime.now()
            t_sample = int(time.mktime(t_sample.timetuple()))
            dates.append(t_sample)

        for date in dates:
            period_balances = dict()
            t_sample = int(time.mktime(date.timetuple()))
            for txn in self.eth_txns:
                # skip transactions not in the date range
                if int(txn['timeStamp']) <= t_sample:
                    if txn['tokenSymbol'] in period_balances.keys():
                        if txn['to'] == self.addy.lower():
                            period_balances[txn['tokenSymbol']] += txn['value']
                        elif txn['from'] == self.addy.lower():
                            period_balances[txn['tokenSymbol']] -= txn['value']
                    else:
                        if txn['to'] == self.addy.lower():
                            period_balances[txn['tokenSymbol']] = txn['value']
                        elif txn['from'] == self.addy.lower():
                            period_balances[txn['tokenSymbol']] = txn['value']*-1

            balances.append(period_balances)

        return balances

    def get_eth_txn(self, blocks=None):
        # TODO potentially limit use of this function by diverting to get_eth_block_transactions
        # TODO store last interacted block of wallet and scan all newer up to latest??
        """ gets eth and erc20 transactions for passed address
        blocks will default to all known blocks if none passed """

        txns = list()
        # define blocks as all blocks if none passed

        if not blocks:
            # print(self.w3.eth.block_number)
            blocks = range(0, self.w3.eth.block_number)

        # list of dictionaries
        normal_txns = self.etherscan.get_normal_txs_by_address(self.addy, startblock=blocks[0], endblock=blocks[-1],
                                                               sort='asc')
        # returns list of dicitonaries
        erc20_txns = self.etherscan.get_erc20_token_transfer_events_by_address(self.addy, startblock=blocks[0], endblock=blocks[-1],
                                                                               sort='asc')

        for item in normal_txns:
            # add token symbol for later functionality
            item['tokenSymbol'] = 'ETH'
            # convert wei to eth
            item['value'] = float(self.w3.fromWei(int(item['value']), 'ether'))
            txns.append(item)
        for item in erc20_txns:
            # convert to number of tokens using coins native decimal
            item['value'] = int(item['value'])/(10 ** int(item['tokenDecimal']))
            txns.append(item)
        # print(txns)
        return txns

    def get_eth_block_transactions(self, blocks=None):
        if not blocks:
            # print(self.w3.eth.block_number)
            blocks = range(0, self.w3.eth.block_number)

        # address_lowercase = args.addr.lower()
        address_lowercase = self.addy.lower()
        transactions = list()
        interacted_blocks = list()

        end_block = blocks[-1]
        start_block = blocks[0]

        for idx in blocks:
            print('Fetching block %d, remaining: %d, progress: %d%%'%(
                idx, (end_block-idx), 100*(idx-start_block)/(end_block-start_block)))

            block = self.w3.eth.getBlock(idx, full_transactions=True)

            for tx in block.transactions:
                if tx['to']:
                    to_matches = tx['to'].lower() == address_lowercase
                else:
                    to_matches = False

                if tx['from']:
                    from_matches = tx['from'].lower() == address_lowercase
                else:
                    from_matches = False

                if to_matches or from_matches:
                    print('Found transaction with hash %s'%tx['hash'].hex())
                    transactions.append(tx)
                    interacted_blocks.append(idx)

        return transactions, interacted_blocks


def __main__():
    # wallet = cryptoWallet(addy='0x68e6D61EDB3fb8b564E9Ae196cFa59FBda01d5A4')
    wallet = cryptoWallet(addy='0x68e6D61EDB3fb8b564E9Ae196cFa59FBda01d5A4')
    start_date = datetime.strptime('2021-11-01', DATE_FORMAT)
    end_date = datetime.strptime('2022-04-01', DATE_FORMAT)
    res = get_date_res(window_unit='month', window_size=1)

    data = populate_dates(resolution=res, x_max=end_date, x_min=start_date)
    data_str = list()
    for datepair in data:
        data_str.append(datepair[1])

    balances = wallet.get_eth_balances(dates=data_str)
    for balance in balances:
        print(balance)
    print(wallet.eth_txns)


if __name__ == '__main__':
    __main__()