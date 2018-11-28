import signal
import requests
import time
from math import floor

# TODO clear imports
import json
import traceback

shutdown = False

MAIN_TAKER = 0.0065
MAIN_MAKER = 0.002
ALT_TAKER = 0.005
ALT_MAKER = 0.0035
TAKER = (MAIN_TAKER + ALT_TAKER)*2
MAKER = MAIN_MAKER + ALT_MAKER
TAKEMAIN = MAIN_TAKER - ALT_MAKER
TAKEALT = ALT_TAKER - MAIN_MAKER
BUFFER = 0.01

run_count = 37


class ApiException(Exception):
    pass


class Book(object):
    def __init__(self, sym, json):
        self.sym = sym
        self.json = json
        # could be cached
        self.bids = json['bids']
        self.asks = json['asks']
        self.ask_price = None
        self.bid_price = None
        if self.bids:
            self.bid_price = self.bids[0]['price']
        if self.asks:
            self.ask_price = self.asks[0]['price']

    def bids_room(self):
        if self.bids:
            quantity = sum([b['quantity']
                            for b in self.bids if b['price'] == self.bid_price])
            filled = sum([b['quantity_filled']
                          for b in self.bids if b['price'] == self.bid_price])
            return quantity - filled
        else:
            return 0

    def asks_room(self):
        if self.asks:
            quantity = sum([b['quantity']
                            for b in self.asks if b['price'] == self.ask_price])
            filled = sum([b['quantity_filled']
                          for b in self.asks if b['price'] == self.ask_price])
            return quantity - filled
        else:
            return 0


class Limits(dict):
    def __init__(self, json):
        self.update(json)
        self.gross_limit = int(json['gross_limit'])
        self.net_limit = int(json['net_limit'])
        self.gross = int(json['gross'])
        self.net = int(json['net'])


class OHLC(dict):
    def __init__(self, sym, json):
        self.sym = sym
        self.update(json)
        self.tick = json['tick']
        self.open = json['open']
        self.high = json['high']
        self.low = json['low']
        self.close = json['close']


class Shock(dict):
    def __init__(self, news, currtick):
        self.ticker = news['ticker']
        self.elapsed = currtick - news['tick']
        headline = news['headline']
        try:
            self.amount = float(headline[-6:].replace('$', ''))
        except:
            self.amount = 0


class Session(object):
    def __init__(self, url, key):
        self.url = url
        self.key = key
        self.tick = -1

    def __enter__(self):
        self.session = requests.Session()
        self.session.headers.update({'X-API-Key': self.key})
        return self

    def __exit__(self, type, value, traceback):
        self.session.close()

    def get_tick(self):
        while True:
            resp = self.session.get(self.url + '/v1/case', params=None)
            if not resp.ok:
                raise ApiException('could not get tick: ' + str(resp))
            json = resp.json()
            if json['status'] == 'STOPPED' or shutdown:
                return False
            if json['tick'] != self.tick:
                self.tick = json['tick']
                print('.', self.tick)
                return True
                # this timer is unnecessary, network latency should be enough
            time.sleep(0.25)

    def get_book(self, sym):
        resp = self.session.get(
            self.url + '/v1/securities/book', params={'ticker': sym})
        if not resp.ok:
            raise ApiException('could not get book: ' + str(resp))
        return Book(sym, resp.json())

    def send_order(self, sym, side, price, size):
        resp = self.session.post(self.url + '/v1/orders', params={
                                 'ticker': sym, 'type': 'LIMIT', 'action': side, 'quantity': size, 'price': price})
        if resp.ok:
            print('sent order', side, sym, size, '@', price)
        else:
            print('failed to send order', side, sym,
                  size, '@', price, ':', resp.text)

    def getLimit(self):
        resp = self.session.get(self.url+'/v1/limits')

        if not resp.ok:
            raise ApiException('could not get limit: '+str(resp))
        return Limits(resp.json()[0])

    def getSecurities(self, sym=None):
        if sym is None:
            resp = self.session.get(self.url+'/v1/securities')
        else:
            resp = self.session.get(
                self.url+'/v1/securities', params={'ticker': sym})
        if not resp.ok:
            raise ApiException('could not get position: '+str(resp))
        json = resp.json()
        return {sec['ticker']: {k: sec[k] for k in [
            "position",
            "vwap",
            "nlv",
            "last",
            "bid",
            "bid_size",
            "ask",
            "ask_size",
            "unrealized",
            "realized"
        ]} for sec in json}

    def get_OHLC(self, sym):
        resp = self.session.get(
            self.url + '/v1/securities/history', params={'ticker': sym})
        if not resp.ok:
            raise ApiException('could not get OHLC: ' + str(resp))
        return [OHLC(sym, ohlc) for ohlc in resp.json()]

    def buy(self, sym, price, size):
        self.send_order(sym, 'BUY', price, size)

    def sell(self, sym, price, size):
        self.send_order(sym, 'SELL', price, size)

    def send_market(self, sym, side, size):
        resp = self.session.post(self.url + '/v1/orders', params={
                                 'ticker': sym, 'type': 'MARKET', 'action': side, 'quantity': size})
        if resp.ok:
            json = resp.json()
            print('market order', side, sym, size, '@', json['vwap'])
            return json['vwap']
        else:
            print('failed to send order', side, sym,
                  size, '@Market:', resp.text)
            return 0

    def buyM(self, sym, size):
        return self.send_market(sym, 'BUY', size)

    def sellM(self, sym, size):
        return self.send_market(sym, 'SELL', size)

    def getNews(self):
        resp = self.session.get(self.url + '/v1/news', params={'limit': 10})

        if not resp.ok:
            raise ApiException('failed to get news', resp.text)
        else:
            json = resp.json()
            # only care about recent news
            return [Shock(news, self.tick) for news in json if news['tick'] > self.tick-4]

    def getTrader(self):
        resp = self.session.get(self.url + '/v1/trader')
        if not resp.ok:
            raise ApiException('failed to get trader info', resp.text)
        else:
            json = resp.json()
            return json


def getHistory(session):
    # observe certain metrics in the market
    ETF = session.get_OHLC('ETF')
    WMTM = session.get_OHLC('WMT-M')
    MMMM = session.get_OHLC('MMM-M')
    CATM = session.get_OHLC('CAT-M')
    WMTA = session.get_OHLC('WMT-A')
    MMMA = session.get_OHLC('MMM-A')
    CATA = session.get_OHLC('CAT-A')
    ES = session.get_OHLC('ES')

    return {
        'ES': ES,
        'ETF': ETF,
        'WMTM': WMTM,
        'MMMM': MMMM,
        'CATM': CATM,
        'WMTA': WMTA,
        'MMMA': MMMA,
        'CATA': CATA
    }


prices = {
    "ETF": [],
    "WMT-M": [],
    "MMM-M": [],
    "CAT-M": [],
    "WMT-A": [],
    "MMM-A": [],
    "CAT-A": []}
imbalances = {
    "ETF": [],
    "WMT-M": [],
    "MMM-M": [],
    "CAT-M": [],
    "WMT-A": [],
    "MMM-A": [],
    "CAT-A": []}


def main():
    # price does change in every tick

    # check position

    # plain arbitradge
    # index arbitrage
    # shock handling
    # wave riding

    global prices, imbalance, run_count

    with Session('http://localhost:9998', 'VHK3DEDE') as session:

        while session.get_tick():
            try:
                shock_runner(session)
                exchange_arbitrage(session, "WMT-M", "WMT-A")
                exchange_arbitrage(session, "CAT-M", "CAT-A")
                exchange_arbitrage(session, "MMM-M", "MMM-A")
                index_arbitrage(session, ['WMT', 'MMM', 'CAT'])
                # for ticker in prices:
                    # front_runner(session, ticker)
            except Exception as ex:
                print("error", str(ex))
                with open("./error.txt", 'a') as logfile:
                    traceback.print_exc(file=logfile)
                continue
            # print((end - start)*1000, 'ms')
        try:
            # data = getHistory(session)
            # with open('./data'+str(run_count)+'.json', 'w') as outfile:
                # json.dump(data, outfile)
            trader = session.getTrader()
            with open('./trader'+str(run_count)+'.json', 'w') as outfile:
                json.dump(trader, outfile)
        except:
            print()
        #     print("collecting historical data failed")
        # with open("./prices"+str(run_count)+".json", 'w') as outfile:
        #     json.dump(prices, outfile)
        # with open("./imbalances"+str(run_count)+".json", 'w') as outfile:
        #     json.dump(imbalances, outfile)
        # while not session.get_tick():
        #     time.sleep(1)
        run_count += 1
    return main()


# TODO: position cleaner: try to reduce gross position loss-free
pairTickers = [('WMT-M', 'WMT-A'), ('CAT-M', 'CAT-A'), ('MMM-M', 'MMM-A')]

# TODO: implement range runner for the last x ticks

# TODO: implement delta runner per one send, two sec etc


def front_runner(session, ticker):
    # if enough room on both ends, place a pair order
    global prices, imbalance
    book = session.get_book(ticker)
    bids_room = book.bids_room()
    asks_room = book.asks_room()
    if None in [bids_room, asks_room, book.bid_price, book.ask_price]:
        return
    imbalance = floor(bids_room/(asks_room+1))
    imbalances[ticker].append(imbalance)
    curr_price = (book.bid_price+book.ask_price)/2
    prices[ticker].append(curr_price)


def shock_runner(session):
    shocks = session.getNews()
    quantity = 50000
    for shock in sorted(shocks, key=lambda s: s.elapsed):
        if shock.elapsed < 1:
            Mticker = shock.ticker+"-M"
            Aticker = shock.ticker+"-A"
            if shock.amount > MAIN_TAKER + BUFFER*2:
                Mprice = session.buyM(Mticker, quantity)
                Aprice = session.buyM(Aticker, quantity)
                session.sell(Mticker, Mprice+shock.amount-BUFFER, quantity)
                session.sell(Aticker, Aprice+shock.amount-BUFFER, quantity)
            elif - shock.amount > MAIN_TAKER + BUFFER*2:
                Mprice = session.sellM(Mticker, quantity)
                Aprice = session.sellM(Aticker, quantity)
                session.buy(Mticker, Mprice+shock.amount+BUFFER, quantity)
                session.buy(Aticker, Aprice+shock.amount+BUFFER, quantity)
            print('shock', shock.ticker, shock.amount)


TAKER4 = MAIN_TAKER * 5


def index_arbitrage(session, tickers):
    secs = session.getSecurities()
    ETF = secs['ETF']
    etfBid = ETF['bid']
    etfAsk = ETF['ask']
    bestBids = {}
    bestBidsQ = {}
    bestAsks = {}
    bestAsksQ = {}
    for ticker in tickers:
        tickerM = ticker+"-M"
        tickerA = ticker+"-A"
        Mticker = secs[tickerM]
        Aticker = secs[tickerA]
        Mbid = Mticker['bid']
        Abid = Aticker['bid']
        Mask = Mticker['ask']
        Aask = Aticker['ask']
        if Mbid >= Abid:
            bestBids[tickerM] = Mbid
            bestBidsQ[tickerM] = Mticker['bid_size']
        else:
            bestBids[tickerA] = Abid
            bestBidsQ[tickerA] = Aticker['bid_size']
        if Mask <= Aask:
            bestAsks[tickerM] = Mask
            bestAsksQ[tickerM] = Mticker['ask_size']
        else:
            bestAsks[tickerA] = Aask
            bestAsksQ[tickerA] = Aticker['ask_size']
    compositBid = sum(bestBids.values())
    compositBidQ = min(bestBidsQ.values())
    compositAsk = sum(bestAsks.values())
    compositAskQ = min(bestAsksQ.values())
    boughtprice = 0
    soldprice = 0
    if etfBid - compositAsk > TAKER4+BUFFER:
        quantity = ETF['bid_size'] if ETF['bid_size'] < compositAskQ else compositAskQ
        if quantity == 0:
            return
        quantity = min([quantity, 50000])
        soldprice = session.sellM('ETF', quantity)
        for ticker in bestAsks:
            boughtprice += session.buyM(ticker, quantity)
        print('Plan   ETF', etfBid, 'Stocks', compositAsk)
        print('Actual ETF', soldprice, 'Stocks', boughtprice)
    elif compositBid - etfAsk > TAKER4+BUFFER:
        quantity = ETF['ask_size'] if ETF['ask_size'] < compositBidQ else compositBidQ
        if quantity == 0:
            return
        quantity = min([quantity, 50000])
        for ticker in bestBids:
            soldprice += session.sellM(ticker, quantity)
        boughtprice = session.buyM('ETF', quantity)
        print('Plan   Stocks', compositBid, 'ETF', etfAsk)
        print('Actual Stocks', soldprice, 'ETF', boughtprice)

# TODO: send limit orders and use market to cover unfilled ones after

def exchange_arbitrage(session, mticker, aticker):
    mbook = session.get_book(mticker)
    masks_room = mbook.asks_room()
    mbids_room = mbook.bids_room()
    abook = session.get_book(aticker)
    aasks_room = abook.asks_room()
    abids_room = abook.bids_room()
    # TODO: could set a good price here
    if None in [mbook.bid_price, mbook.ask_price, abook.ask_price, abook.bid_price]:
        return
    if mbook.bid_price - abook.ask_price > TAKER+BUFFER:
        quantity = aasks_room if aasks_room < mbids_room else mbids_room
        quantity = min([quantity, 50000])
        session.sellM(mbook.sym, quantity)
        session.buyM(abook.sym, quantity)
    elif abook.bid_price - mbook.ask_price > TAKER+BUFFER:
        quantity = aasks_room if aasks_room < mbids_room else mbids_room
        quantity = min([quantity, 50000])
        session.sellM(abook.sym, quantity)
        session.buyM(mbook.sym, quantity)


def sigint(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint)
    main()
