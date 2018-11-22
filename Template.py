import signal
import requests
import time

shutdown = False


class ApiException(Exception):
    pass


class Book(object):
    def __init__(self, sym, json):
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
            bid = self.bids[0]
            self.bid_price = bid['price']
            self.bids_quantity_left = bid['quantity'] - bid['quantity_filled']
        if self.asks:
            ask = self.asks[0]
            self.ask_price = ask['price']
            self.asks_quantity_left = ask['quantity'] - ask['quantity_filled']


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
                print('.', self.tick, end='')
                return True
                # this timer is unnecessary, network latency should be enough
            time.sleep(0.05)

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


def main():
    commission = 0.02
    buffer = 0.01
    ratios = []
    with Session('http://localhost:9999', 'VHK3DEDE') as session:
        while session.get_tick():
            book1 = session.get_book('WMT-M')
            book2 = session.get_book('WMT-A')
            spread_cover(session, book1, book2, commission, buffer)
            book1 = session.get_book('MMM-M')
            book2 = session.get_book('MMM-A')
            spread_cover(session, book1, book2, commission, buffer)
            book1 = session.get_book('CAT-M')
            book2 = session.get_book('CAT-A')
            spread_cover(session, book1, book2, commission, buffer)
            ES = session.get_book('ES')
            ETF = session.get_book('ETF')
            ESPrice = (ES.ask_price + ES.bid_price)/2
            ETFPrice = (ETF.ask_price + ETF.bid_price)/2
            ratios.append(ESPrice/ETFPrice)
            # if book1.bid_price - book2.ask_price > commission + buffer*2:
            #     session.send_order(
            #         'WMT-M', 'SELL', book1.bid_price-buffer, 1000)
            #     session.send_order(
            #         'WMT-A', 'BUY', book2.ask_price+buffer,  1000)
            # elif book2.bid_price - book1.ask_price > commission + buffer*2:
            #     session.send_order(
            #         'WMT-M', 'BUY', book1.bid_price+buffer, 1000)
            #     session.send_order(
            #         'WMT-A', 'SELL', book2.ask_price-buffer,  1000)
    print(sorted(ratios))
    print(ratios)


def spread_cover(session, book1, book2, commission, buffer):
    buy = None
    buy_price = None
    buy_volume = None
    sell = None
    sell_price = None
    sell_volume = None
    if book1.bid_price - book2.ask_price > commission + buffer*2:
        buy = book2.sym
        buy_price = book2.ask_price+buffer
        buy_volume = book2.asks_quantity_left//2
        sell = book1.sym
        sell_price = book1.bid_price-buffer
        sell_volume = book1.bids_quantity_left//2
    elif book2.bid_price - book1.ask_price > commission + buffer*2:
        buy = book1.sym
        buy_price = book1.ask_price+buffer
        buy_volume = book1.asks_quantity_left//2
        sell = book2.sym
        sell_price = book2.bid_price-buffer
        sell_volume = book2.bids_quantity_left//2
    else:
        # no bid ask spread detected
        return
    print()
    volume = min([sell_volume, buy_volume])
    session.send_order(buy, 'BUY', buy_price, volume)
    session.send_order(sell, 'SELL', sell_price,  volume)


def spread_ratio_cover(initialRatio):
    ratioguess = initialRatio

    def coverer(session, book1, book2, commission, buffer):
        buy = None
        buy_price = None
        sell = None
        sell_price = None
        if book1.bid_price - book2.ask_price > commission + buffer*2:
            buy = book2.sym
            buy_price = book2.ask_price+buffer
            sell = book1.sym
            sell_price = book1.bid_price-buffer
        elif book2.bid_price - book1.ask_price > commission + buffer*2:
            buy = book1.sym
            buy_price = book1.ask_price+buffer
            sell = book2.sym
            sell_price = book2.bid_price-buffer
        else:
            # no bid ask spread detected
            return
        print()
        session.send_order(buy, 'BUY', buy_price, 10000)
        session.send_order(sell, 'SELL', sell_price,  10000)
    return coverer


def sigint(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True


if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint)
    main()
