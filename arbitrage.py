__author__ = 'rohan'

import pandas as pd
import exchanges as ex
import database as db


class arbitrage():

    def __init__(self):
        self.minimum_trade_percentage = 0
        self.minimum_trade_size = 0.01 # minimum 0.01
        self.maximum_trade_size = 10.0
        self.exchange_fees = ex.fees
        self.max_trades = 2 # number of maximum arb (1 trade => 1 buy, 1 sell) trades to take per cycle
        self.trading_pair = 'BTCUSD'

    def _get_orderbooks(self):

        kraken_ask, kraken_bid = ex.kraken_object.get_orderbook(self.trading_pair)
        cryptsy_ask, cryptsy_bid = ex.cryptsy_object.get_orderbook(self.trading_pair)
        bitfinex_ask, bitfinex_bid = ex.bitfinex_object.get_orderbook(self.trading_pair)
        vircurex_ask, vircurex_bid = ex.vircurex_object.get_orderbook(self.trading_pair)
        btce_ask, btce_bid = ex.btce_object.get_orderbook(self.trading_pair)

        cum_asks = pd.concat([kraken_ask, cryptsy_ask, bitfinex_ask, vircurex_ask, btce_ask]).sort(['Price'], ascending = True, kind = 'quicksort')
        cum_bids = pd.concat([kraken_bid, cryptsy_bid, bitfinex_bid, vircurex_bid, btce_bid]).sort(['Price'], ascending = False, kind = 'quicksort')

        print "Cumulative asks (sellers): \n", cum_asks, "\n"
        print "Cumulative bids (buyers): \n",cum_bids

        # print cum_bids.loc[0, 'Price'] - cum_asks.loc[0, 'Price'] # Best buyers active in markets
        # print cum_asks.loc[0, 'Price'] # Best sellers active in markets

        best_bids = cum_bids.loc[0] # Index 0 holds best bids from each individual exchange
        best_bids = best_bids.reset_index(drop = True) # Rebuild Index for referencing
        print "\nBest bids: \n", best_bids, "\n"

        best_asks = cum_asks.loc[0] # Index 0 holds best asks from each individual exchange
        best_asks = best_asks.reset_index(drop = True)
        print "Best asks: \n", best_asks, "\n"

        return best_asks, best_bids

    def _place_orders(self, arb_list = []):
        exchange_objects = {
        'btc-e': ex.btce_object,
        'Bitfinex': ex.bitfinex_object,
        'Cryptsy': ex.cryptsy_object,
        'Vircurex': ex.vircurex_object,
        'Kraken': ex.kraken_object
        }
        #print arb_list
        if len(arb_list) == 0:
            return 'No arbitrage opportunities parsed'
        else:
            x = 0
            executed_trades = 0
            while (x < (len(arb_list)-5)):
                if executed_trades < self.max_trades:
                    print exchange_objects[arb_list[x]].add_order(self.trading_pair, 'buy', arb_list[x + 4], arb_list[x + 1])
                    buy_execution_data = arb_list[x], self.trading_pair, 'buy', arb_list[x + 4], arb_list[x + 1]
                    print "Order executed at", arb_list[x], "to buy", arb_list[x + 4], self.trading_pair, "at", arb_list[x + 1]

                    print exchange_objects[arb_list[x + 2]].add_order(self.trading_pair, 'sell', arb_list[x + 4], arb_list[x + 3])
                    sell_execution_data = arb_list[x + 2], self.trading_pair, 'sell', arb_list[x + 4], arb_list[x + 3]
                    print "Order executed at", arb_list[x + 2], "to sell", arb_list[x + 4], self.trading_pair, "at", arb_list[x + 3]

                    db.database_object.insert_trades(buy_execution_data, sell_execution_data)

                    x += 7
                    executed_trades += 1

                return 'Maximum number of trades have been executed'

    def deterministic_arb(self):
        best_asks, best_bids = self._get_orderbooks()

        arb_ops = 0
        arb_list = []

        # build list with arb trades
        for i in range (0, 5):
            for j in range (0, 5):
                if (best_bids.loc[i, 'Price'] - best_asks.loc[j, 'Price']) > 0:
                    # print "Sell at: ", best_bids.loc[i, 'Exchange'], " Buy at: "
                    # print "\nSell at: ", (best_bids.loc[i, 'Exchange'], " Buy at: ", best_asks.loc[j, 'Exchange'])

                    if best_bids.loc[i, 'Size'] > best_asks.loc[j, 'Size']:
                        size = best_asks.loc[j, 'Size']
                    else:
                        size = best_bids.loc[i, 'Size']

                    arb_list.append((best_asks.loc[j, 'Exchange'], best_asks.loc[j, 'Price'], best_bids.loc[i, 'Exchange'], best_bids.loc[i, 'Price'], size,
                    best_bids.loc[i, 'Price'] - best_asks.loc[j, 'Price'], ((best_bids.loc[i, 'Price'] - best_asks.loc[j, 'Price']) / best_asks.loc[j, 'Price']) * 100))
                    arb_ops += 1

        dfArb = pd.DataFrame(arb_list).rename(columns = {0: 'Buy exchange', 1: 'Buy price', 2: 'Sell exchange',
                                                         3: 'Sell price', 4: 'Trade size', 5: 'Difference', 6: 'Percentage difference'}).sort(['Difference'], ascending = False, kind = 'quicksort').reset_index(drop = True)

        dfArb = dfArb[(dfArb['Trade size'] >= self.minimum_trade_size) & (dfArb['Trade size'] <= self.maximum_trade_size) & (dfArb['Percentage difference'] >= self.minimum_trade_percentage)]
        print dfArb
        dfArb_list = list(dfArb.values.flatten()) # sorted list (arb_list => unsorted)

        if arb_ops > 0:
            return self._place_orders(dfArb_list)
        else:
            return 'No arbitrage opportunities found'

arbitrage_object = arbitrage()
print arbitrage_object.deterministic_arb()
