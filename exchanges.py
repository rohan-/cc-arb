__author__ = 'rohan'
import time
import hashlib
import requests
import hmac
import urllib
import json
import base64
import collections
import formatdata as fd
import logging

import pandas as pd
logging.basicConfig(level=logging.DEBUG)

# buy / sell fees
fees = {
        'Kraken': 0.35,
        'Cryptsy': 0.33,
        'Bitfinex': 0.20,
        'btc-e': 0.20,
        'Vircurex': 0.00,
        }

class Kraken():
    url = 'https://api.kraken.com'
    version = '0/'
    public = 'public/'
    private = 'private/'
    fees = fees['Kraken']
    symbols = {'BTCUSD': 'XXBTZUSD', 'LTCBTC': 'XXBTXLTC'}

    def __init__(self, public_key, private_key):
        self.public_key = str(public_key)
        self.private_key = str(private_key)
        self.name = 'Kraken'

    def _public_query(self, method, parameters = {}):
        methods = {'get_orderbook': 'Depth'}
        header = {'User-Agent': 'FYP'}
        uri = '/' + self.version + self.public + methods[method]
        url = self.url + uri
        post_data = urllib.urlencode(parameters)
        try:
            r = requests.post(url, data = post_data, headers = header)
        except requests.exceptions.RequestException as e:
            print e
            exit(1)
        return fd.make_dataframe(r.json()['result'][parameters['pair']], {'ask_array': 'asks', 'bids_array': 'bids',
                                 'price_column': 0, 'quantity_column': 1, 'name': self.name, 'pair': parameters['pair']})

    def _private_query(self, method, parameters = {}):
        methods = {'add_order': 'AddOrder', 'cancel_order': 'CancelOrder'}
        parameters['nonce'] = int(1000 * time.time())
        post_data = urllib.urlencode(parameters)
        uri = '/' + self.version + self.private + methods[method]
        url = self.url + uri
        hash_object = uri + hashlib.sha256(str(parameters['nonce']) + post_data).digest()
        signature = hmac.new(base64.b64decode(self.private_key), hash_object, hashlib.sha512)
        header = {'API-Key': self.public_key, 'API-Sign': base64.b64encode(signature.digest()), 'User-Agent': 'FYP'}
        try:
            r = requests.post(url, data = post_data , headers = header)
        except requests.exceptions.RequestException as e:
            print e
            exit(1)
        transaction_id = r.json()
        return transaction_id

    def get_orderbook(self, pair):
        return self._public_query('get_orderbook', {'pair': self.symbols[pair]})

    def add_order(self, pair, direction, size, price):
        return self._private_query('add_order', {'pair': self.symbols[pair], 'type': direction, 'ordertype': 'market', 'price': price, 'volume': size})

    def cancel_order(self, id):
        return self._private_query('cancel_order', {'txid': id})


class Cryptsy():
    url = 'https://api.cryptsy.com/api'
    symbols = {'BTCUSD': '2', 'BTCLTC': '1', 'LTCBTC' : '3'}
    fees = fees['Cryptsy']

    def __init__(self, public_key, private_key):
        self.public_key = str(public_key)
        self.private_key = str(private_key)
        self.name = 'Cryptsy'

    def _private_query(self, method, parameters = {}):
        methods = {'get_orderbook': 'marketorders', 'add_order': 'createorder', 'cancel_order': 'cancelorder'}
        parameters['nonce'] = int(1000 * time.time())
        if method == 'get_orderbook' or method == 'add_order':
            parameters['marketid'] = self.symbols[parameters['marketid']]
        parameters['method'] = methods[method]
        post_data = urllib.urlencode(parameters)
        signature = hmac.new(self.private_key, post_data, hashlib.sha512).hexdigest()
        header = {'Key': self.public_key, 'Sign': signature}
        try:
            r = requests.post(self.url, data = post_data, headers = header)
        except requests.exceptions.RequestException as e:
            print e
            exit(1)
        if method == 'get_orderbook':
            return fd.make_dataframe(r.json()['return'], {'ask_array': 'sellorders', 'bids_array': 'buyorders',
                                                          'price_column': 'sellprice', 'quantity_column': 'quantity', 'name': self.name})
        else:
            return r.json()

    def get_orderbook(self, pair):
        return self._private_query('get_orderbook', {'marketid': pair})

    def add_order(self, pair, direction, size, price):
        return self._private_query('add_order', {'marketid': pair, 'ordertype': direction.title(), 'quantity': size, 'price': price})

    def cancel_order(self, id):
        return self._private_query('cancel_order', {'orderid': id})


class Bitfinex():
    url = 'https://api.bitfinex.com/'
    version = 'v1/'
    fees = fees['Bitfinex']

    def __init__(self, public_key, private_key):
        self.public_key = str(public_key)
        self.private_key = str(private_key)
        self.name = 'Bitfinex'

    def _public_query(self, method, parameters = {}):
        methods = {'get_orderbook': 'book'}
        url = self.url + self.version + methods[method] + '/'
        if method == 'get_orderbook':
            url = url + parameters['pair']
        parameters['nonce'] = int(1000 * time.time())
        get_data = urllib.urlencode(parameters)
        try:
            r = requests.get(url, data = get_data)
        except requests.exceptions.RequestException as e:
            print e
            exit(1)
        return fd.make_dataframe(r.json(), {'ask_array': 'asks', 'bids_array': 'bids',
                                                          'price_column': 'price', 'quantity_column': 'amount', 'name': self.name})

    def _private_query(self, method, parameters = {}):
        methods = {'add_order': 'order/new', 'cancel_order': 'order/cancel'}
        url = self.url + self.version + methods[method]
        parameters['request'] = '/' + self.version + methods[method]
        parameters['nonce'] = str(int(1000 * time.time()))
        payload_data = base64.b64encode(json.dumps(parameters), "utf-8")
        signature = hmac.new(self.private_key, payload_data, hashlib.sha384).hexdigest()
        header = {'X-BFX-APIKEY': self.public_key, 'X-BFX-PAYLOAD': payload_data, 'X-BFX-SIGNATURE': signature}
        try:
            r = requests.post(url, data = parameters, headers = header)
        except requests.exceptions.RequestException as e:
            print e
            exit(1)
        return r.json()

    def get_orderbook(self, pair):
        return self._public_query('get_orderbook', {'pair': pair})

    def add_order(self, pair, direction, size, price):
        return self._private_query('add_order', {'symbol': pair, 'amount': str(size), 'price': str(price), 'exchange': 'bitfinex', 'side': direction, 'type': 'market'})

    def cancel_order(self, id):
        return self._private_query('cancel_order', {'order_id': int(id)})


class Vircurex():
    url = 'https://api.vircurex.com/api/'
    security_word = 'finalyearproject'
    username = 'rohanm'
    fees = fees['Vircurex']

    def __init__(self, username, security_word):
        self.name = 'Vircurex'
        self.username = str(username)
        self.security_word = str(security_word)

    def _public_query(self, method, parameters = {}):
        methods = {'get_orderbook': 'orderbook.json'}
        url = self.url + methods[method]
        get_data = urllib.urlencode(parameters)
        try:
            r = requests.get(url, params = get_data)
        except requests.exceptions.RequestException as e:
            print e
            exit(1)
        return fd.make_dataframe(r.json(), {'ask_array': 'asks', 'bids_array': 'bids',
                                                          'price_column': 0, 'quantity_column': 1, 'name': self.name})

    def _private_query(self, method, parameters = collections.OrderedDict()):
        methods = {'add_order': 'create_released_order.json', 'cancel_order': 'delete_order.json'}
        url = self.url + methods[method]
        nonce = int(1000 * time.time())
        timestamp = str(time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()))
        input_token = str(self.security_word + ';' + self.username + ';' + timestamp + ';' + str(nonce))
        input_token = input_token + ';' + str(''.join(['%s;' % value for value in parameters.values()])[:-1])
        hash_object = hashlib.sha256(input_token).hexdigest()
        parameters['account'] = self.username
        parameters['id'] = nonce
        parameters['token'] = hash_object
        parameters['timestamp'] = timestamp
        get_data = urllib.urlencode(parameters)
        try:
            r = requests.get(url, params = get_data, data = get_data)
        except requests.exceptions.RequestException as e:
            print e
            exit(1)
        return r.json()

    def get_orderbook(self, pair):
        base, alt = self._get_pair(pair)
        return self._public_query('get_orderbook', {'base': base, 'alt': alt})

    def add_order(self, pair, direction, size, price):
        base, alt = self._get_pair(pair)
        ordered_dict = collections.OrderedDict([('type', 'create_order'), ('ordertype', str(direction).upper()), ('amount', size), ('currency1', base), ('unitprice', price), ('currency2', alt)])
        return self._private_query('add_order', ordered_dict)

    def cancel_order(self, id):
        ordered_dict = collections.OrderedDict([('type', 'delete_order'), ('orderid', id), ('otype', '1')])
        return self._private_query('cancel_order', ordered_dict)

    def _get_pair(self, pair):
        if pair[0] == 'd'.capitalize():
            base = pair[:4]
            alt = pair[4:]

        else:
            base = pair[:3]
            alt = pair[3:]

        return base, alt


class BtcE():
    url = 'https://btc-e.com/api/'
    trade_url = 'https://btc-e.com/tapi'
    version = '3/'
    fees = fees['btc-e']


    def __init__(self, public_key, private_key):
        self.name = 'btc-e'
        self.public_key = public_key
        self.private_key = private_key

    def _public_query(self, method, parameters = {}):
        methods = {'get_orderbook': 'depth'}
        url = self.url + self.version + methods[method] + '/'
        try:
            r = requests.get(url + parameters['pair'])
        except requests.exceptions.RequestException as e:
            print e
            exit(1)
        return fd.make_dataframe(r.json()[parameters['pair']], {'ask_array': 'asks', 'bids_array': 'bids',
                                                          'price_column': 0, 'quantity_column': 1, 'name': self.name})

    def _private_query(self, method, parameters = {}):
        methods = {'add_order': 'Trade', 'cancel_order': 'CancelOrder'}
        parameters['method'] = methods[method]
        parameters['nonce'] = int(100 * time.time())/100
        post_data = urllib.urlencode(parameters)
        signature = hmac.new(self.private_key, post_data, hashlib.sha512).hexdigest()
        headers = {"Content-type": "application/x-www-form-urlencoded", "Key": self.public_key, "Sign": signature}
        try:
            r = requests.post(self.trade_url, params = post_data, data = post_data, headers = headers)
        except requests.exceptions.RequestException as e:
            print e
            exit(1)
        return r.json()

    def get_orderbook(self, pair):
        pair = self._get_pair(pair)
        return self._public_query('get_orderbook', {'pair': pair})

    def add_order(self, pair, direction, size, price):
        pair = self._get_pair(pair)
        return self._private_query('add_order', {'pair': pair, 'type': direction, 'amount': size, 'rate': price})

    def cancel_order(self, id):
        return self._private_query('cancel_order', {'order_id': id})

    def _get_pair(self, pair):
        if pair[0] == 'd'.capitalize():
            pair = pair[:4] + '_' + pair[4:]
        else:
            pair = pair[:3] + '_' + pair[3:]
        return str(pair).lower()

kraken_object = Kraken('', '')
cryptsy_object = Cryptsy('', '')
bitfinex_object = Bitfinex('', '')
vircurex_object = Vircurex('', '')
btce_object = BtcE('', '')

