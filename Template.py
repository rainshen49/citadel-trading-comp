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
        self.bid_price = self._bid_price()
        self.ask_price = self._ask_price()
    def _bid_price(self):
        if self.json['bids']:
            return self.json['bids'][0]['price']
        else:
            return float('nan')
    def _ask_price(self):
        if self.json['asks']:
            return self.json['asks'][0]['price']
        else:
            return float('nan')
    
class Session(object):
    def __init__(self, url, key):
        self.url = url
        self.key = key
        self.tick = -1
    def __enter__(self):
        self.session = requests.Session()
        self.session.headers.update({'X-API-Key': self.key })
        return self
    def __exit__(self, type, value, traceback):
        self.session.close()
    def get_tick(self):
        while True:
            resp = self.session.get(self.url + '/v1/case', params = None)
            if not resp.ok:
                raise ApiException('could not get tick: ' + str(resp))
            json = resp.json()
            if json['status'] == 'STOPPED' or shutdown:
                return False
            if json['tick'] != self.tick:
                self.tick = json['tick']
                print('got tick', self.tick)
                return True
            time.sleep(0.25)
    def get_book(self, sym):
        resp = self.session.get(self.url + '/v1/securities/book', params = { 'ticker': sym })
        if not resp.ok:
            raise ApiException('could not get book: ' + str(resp))
        return Book(sym, resp.json())
    def send_order(self, sym, side, price, size):
        resp = self.session.post(self.url + '/v1/orders', params = { 'ticker': sym, 'type': 'LIMIT', 'action': side, 'quantity': size, 'price': price })
        if resp.ok:
            print('sent order', side, sym, size, '@', price)
        else:
            print('failed to send order', side, sym, size, '@', price, ':', resp.text)
            
def main():
    with Session('http://localhost:9998', 'OAIU1GJ7') as session:
        while session.get_tick():
            book1 = session.get_book('WMT-M')
            book2 = session.get_book('WMT-A')
            if book1.bid_price > book2.ask_price:
                session.send_order('WMT-M', 'SELL', book1.bid_price, 1000)
                session.send_order('WMT-A', 'BUY', book2.ask_price,  1000)

def sigint(signum, frame):
    global shutdown
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    shutdown = True
    
if __name__ == '__main__':
    signal.signal(signal.SIGINT, sigint)
    main()
