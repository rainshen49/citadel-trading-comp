import signal
import requests
import time
from math import floor

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

NaN = float('nan')

class ApiException(Exception):
    pass


class Book(object):
    def __init__(self, sym, json):
        global NaN
        self.sym = sym
        self.json = json
        # could be cached
        self.bids = self.json['bids']
        self.asks = self.json['asks']
        self.ask_price = 1
        self.asks_quantity_left = 0
        self.bid_price = 1
        self.bids_quantity_left = 0
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
            time.sleep(0.1)

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

    def get_OHLC(self, sym, ticks=50):
        resp = self.session.get(
            self.url + '/v1/securities/history', params={'ticker': sym,'limit':ticks})
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


def main():
    # price does change in every tick

    # check position

    # plain arbitradge
    # index arbitrage
    # shock handling
    # wave riding

    # pairTickers = [('WMT-M', 'WMT-A'), ('CAT-M', 'CAT-A'), ('MMM-M', 'MMM-A')]
    with Session('http://localhost:9998', 'VHK3DEDE') as session:

        while session.get_tick():
            try:
                shock_runner(session)
                exchange_arbitrage(session, "WMT-M", "WMT-A")
                exchange_arbitrage(session, "CAT-M", "CAT-A")
                exchange_arbitrage(session, "MMM-M", "MMM-A")
                index_arbitrage(session, ['WMT', 'MMM', 'CAT'])
            except Exception as ex:
                print("error", str(ex))
        # trader = session.getTrader()
        # print(trader['nlv'])

# TODO: position cleaner: try to reduce gross position loss-free

# TODO: implement range runner for the last x ticks

def avg(arr):
    return sum(arr)/float(len(arr))

def window_trend(left,right):
    leftavg = avg(left)
    rightavg = avg(right)
    if rightavg > leftavg:
        return 1
    elif rightavg < leftavg:
        return -1
    else:
        return 0

def splitarr(arr):
    n = len(arr)
    left = arr[:n//2]
    right = arr[n//2:]
    return left,right

def wwindow_trend(prices):
    left, right = splitarr(prices)
    trend = window_trend(left,right)
    lleft, lright = splitarr(left)
    rleft, rright = splitarr(right)
    trendl = window_trend(lleft,lright)
    trendr = window_trend(rleft,rright)
    return trend + trendl + trendr

def trend_runner(session, ticker):
    if session.tick<20:
        return
    # short term trend
    prices = session.get_OHLC(ticker, 20)
    highs = [price.high for price in prices]
    lows = [price.low for price in prices]
    highTrend = wwindow_trend(highs)
    lowTrend = wwindow_trend(lows)
    if highTrend+lowTrend < -4:
        # volatile, but no trend
        session.buyM(ticker,1000)
    if highTrend+lowTrend > 4:
        session.sellM(ticker,1000)
    
    print(ticker,"short hightrend",highTrend,"lowtrend",lowTrend)

    if session.tick<100:
        return
    prices = session.get_OHLC(ticker, 100)
    highs = [price.high for price in prices]
    lows = [price.low for price in prices]
    highTrend = wwindow_trend(highs)
    lowTrend = wwindow_trend(lows)
    # grown too much
    if highTrend+lowTrend < -4:
        # volatile, but no trend
        session.sellM(ticker,1000)
    # dropped too much
    if highTrend+lowTrend > 4:
        session.buyM(ticker,1000)
    
    print(ticker,"long hightrend",highTrend,"lowtrend",lowTrend)

def shock_runner(session):
    shocks = session.getNews()
    quantity = 50000
    for shock in sorted(shocks, key=lambda s: s.elapsed):
        Mticker = shock.ticker+"-M"
        Aticker = shock.ticker+"-A"
        if shock.elapsed < 2:
            if shock.amount > MAIN_TAKER + BUFFER*2:
                session.buyM(Mticker, quantity)
                session.buyM(Aticker, quantity)
            elif - shock.amount > MAIN_TAKER + BUFFER*2:
                session.sellM(Mticker, quantity)
                session.sellM(Aticker, quantity)
            print('shock', shock.ticker, shock.amount)
        if shock.elapsed == 2:
            if shock.amount > MAIN_TAKER + BUFFER*2:
                session.sellM(Mticker, quantity)
                session.sellM(Aticker, quantity)
            elif - shock.amount > MAIN_TAKER + BUFFER*2:
                session.buyM(Mticker, quantity)
                session.buyM(Aticker, quantity)
            print('post shock', shock.ticker, shock.amount)



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
    global NaN
    mbook = session.get_book(mticker)
    masks_room = mbook.asks_room()
    mbids_room = mbook.bids_room()
    abook = session.get_book(aticker)
    aasks_room = abook.asks_room()
    abids_room = abook.bids_room()
    # a lot of room, make market orders
    if mbook.bid_price - abook.ask_price > TAKER+BUFFER*2:
        quantity = aasks_room if aasks_room < mbids_room else mbids_room
        quantity = min([quantity, 50000])
        session.sellM(mbook.sym, quantity)
        session.buyM(abook.sym, quantity)
    elif abook.bid_price - mbook.ask_price > TAKER+BUFFER*2:
        quantity = aasks_room if aasks_room < mbids_room else mbids_room
        quantity = min([quantity, 50000])
        session.sellM(abook.sym, quantity)
        session.buyM(mbook.sym, quantity)
    # only a little room, make limit orders
    if mbook.bid_price - abook.ask_price > BUFFER:
        quantity = aasks_room if aasks_room < mbids_room else mbids_room
        quantity = min([quantity, 50000])
        session.sell(mbook.sym, mbook.bid_price, quantity)
        session.buy(abook.sym, abook.ask_price, quantity)
    elif abook.bid_price - mbook.ask_price > BUFFER:
        quantity = aasks_room if aasks_room < mbids_room else mbids_room
        quantity = min([quantity, 50000])
        session.sell(abook.sym, abook.bid_price, quantity)
        session.buy(mbook.sym, mbook.ask_price, quantity)



def sigint(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint)
    main()
