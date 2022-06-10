
from datetime import datetime
from numpy import isin
import pandas as pd
import time
import requests
import threading
import asyncio
from binance import AsyncClient, BinanceSocketManager
import websocket
import sys
import _thread
import json
from websocket import create_connection



class Bin_man():
    def __init__(self, ticker_list) -> None:

        self.ticker_list = self.get_ticker(ticker_list)
        self.name = "binance"
        self.books  = {}

    def get_ticker(self,tl):
        ticklist = []
        for ticker in tl :
            if ticker == 'XBT/USDT':
                ticklist.append('BTCUSDT')
            else : 
                s = ticker
                s = s.replace('/','')
                ticklist.append(s)
        return ticklist

    async def get_ob(self, ticker):
        try:
            client = await AsyncClient.create()
            bm = BinanceSocketManager(client)
        except Exception as e:
            print(e)
            input()
        # start any sockets here, i.e a trade socket

        ds = bm.depth_socket(ticker, depth=BinanceSocketManager.WEBSOCKET_DEPTH_20)
        # then start receiving messages
        async with ds as tscm:
            while True:
                res = await tscm.recv()
                st = time.time()
                lsbp = []
                lsbq = []
                lsap = []
                lsaq = []
                for item in res["bids"]:
                    lsbp.append(item[0])
                    lsbq.append(item[1])
                for item in res["asks"]:
                    lsap.append(item[0])
                    lsaq.append(item[1])
                bidsdf = pd.DataFrame({"price": lsbp, "qtt": lsbq}).astype(float)
                asksdf = pd.DataFrame({"price": lsap, "qtt": lsaq}).astype(float)
                # print(bidsdf)
                # print(asksdf)
                self.books[ticker] = {"buy": bidsdf, "sell": asksdf}
                nd = time.time()
                # print(f"timing is {nd-st}")
                # print(self.ob)

    def loop_ob(self, ticker):
        
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.get_ob(ticker))
            loop.close() 

class Krak_man():
    def __init__(self, ticker_list) -> None:
        self.wsb = create_connection("wss://ws.kraken.com/")
        self.wst = create_connection("wss://ws.kraken.com/")
        self.trade_hist = {}
        self.books = {}
        self.ticker_list = ticker_list
        self.name = 'kraken'
    
    def gather_data(self, ticker_list):
        # print(ticker_list)
        #format for the api call
        formating = '", "'
        string = ''
        for pair in ticker_list:
            if ticker_list[-1] == pair:
                string += pair
            else:
                string += pair+formating

            #ticker_list[0]+'", "'+ticker_list[1] le truc qui marche
        pr = '["{}"]'.format(string)
        # print(pr)
        # pr = "''"
        # print(pr)
        # nm = '"name":"trade", "name":"book"'
        self.wst.send('{"event":"subscribe", "subscription":{"name":"trade"}, "pair":%s}' %(pr))
        self.wsb.send('{"event":"subscribe", "subscription":{"name":"book"}, "pair":%s}' %(pr))
        
        def get_trades(callback):
            # print(callback)
            if isinstance(callback, dict):
                    pass
            elif isinstance(callback, list):
                # print('got a callback') 
                # print(callback)
                del callback[0]
                pair = callback[-1]
                for item in callback[0]:
                    #make some shit here to get a dict of trade hist

                    # print(f'price {item[0]}, qtt {item[1]}, unix {item[2]}')
                    ls = [float(item[0]), float(item[1]), float(item[2]), item[3]]
                    try:
                        # print('bao')
                        self.trade_hist[pair]
                        df = self.trade_hist[pair]
                    except KeyError:
                        # print('no hist yet')
                        self.trade_hist[pair] = pd.DataFrame(columns=['price', 'qtt', 'unix','side'])
                        df = self.trade_hist[pair]
                        # print(df)
                        

                    df.loc[len(df)] = ls
                    self.trade_hist[pair] = df 
                    # print(self.trade_hist)
        def get_book(ticker):
            api_symbol = ticker
            api_depth = 10

            # Define order book variables
            api_book = {'bid':{}, 'ask':{}}

            # Define order book update functions
            def dicttofloat(data):
                return float(data[0])

            def api_book_update(api_book_side, api_book_data):
                for data in api_book_data:
                    price_level = data[0]
                    volume = data[1]
                    if float(volume) > 0.0:
                        api_book[api_book_side][price_level] = volume
                    else:
                        api_book[api_book_side].pop(price_level)
                    if api_book_side == 'bid':
                        api_book['bid'] = dict(sorted(api_book['bid'].items(), key=dicttofloat, reverse=True)[:api_depth])
                    elif api_book_side == 'ask':
                        api_book['ask'] = dict(sorted(api_book['ask'].items(), key=dicttofloat)[:api_depth])

            # Define WebSocket callback functions
            def ws_thread(*args):

                ws = websocket.WebSocketApp('wss://ws.kraken.com/', on_open=ws_open, on_message=ws_message)
                ws.run_forever()

            def ws_open(ws):
                ws.send('{"event":"subscribe", "subscription":{"name":"book", "depth":%(api_depth)d}, "pair":["%(api_symbol)s"]}' % {'api_depth':api_depth, 'api_symbol':api_symbol})

            def ws_message(ws, ws_data):
                api_data = json.loads(ws_data)
                if 'event' in api_data:
                    return
                else:
                    if 'as' in api_data[1]:
                        api_book_update('ask', api_data[1]['as'])
                        api_book_update('bid', api_data[1]['bs'])
                    else:
                        for data in api_data[1:len(api_data)-2]:
                            if 'a' in data:
                                api_book_update('ask', data['a'])
                            elif 'b' in data:
                                api_book_update('bid', data['b'])

            # Start new thread for WebSocket interface
            _thread.start_new_thread(ws_thread, ())

            # Output order book (once per second) in main thread
            try:
                while True:
                    if len(api_book['bid']) < api_depth or len(api_book['ask']) < api_depth:
                        time.sleep(1)
                    else:
                        lsbp = []
                        lsbq = []
                        lsap =[]
                        lsaq = []
                        try:
                            for key, value in api_book['bid'].items():
                                lsbp.append(key)
                                lsbq.append(value)
                            for key, value in api_book['ask'].items():
                                lsap.append(key)
                                lsaq.append(value)
                        except RuntimeError as e:
                            print(f'problem :{e}')
                        bidsdf = pd.DataFrame({'price':lsbp, 'qtt':lsbq})
                        asksdf = pd.DataFrame({'price':lsap, 'qtt':lsaq})
                        bidsdf = bidsdf.astype(float)
                        asksdf = asksdf.astype(float)
                        self.books[ticker] = {'buy':bidsdf, 'sell':asksdf}
                        # print(self.books)
                        time.sleep(0.1)
            except KeyboardInterrupt:
                sys.exit(0)
        

        n=0
        for ticker in ticker_list:
            t = threading.Thread(target=get_book, args = [ticker])
            # print(ticker)
            # get_book(ticker)
            t.start()
            time.sleep(0.02)
        while True:
            # print(n)
            rest = self.wst.recv()
            rest = json.loads(rest)
            get_trades(rest)
            resb = self.wsb.recv()
            resb = json.loads(resb)
            # print(resb)
            # print(self.books['BTC/USDT'])
            n+=1

class calc():
    def __init__(self) -> None:
        pass

    def get_fees(self, amount, order_type="limit"):
        if order_type == "limit":
            rate = 0.0002
        else:
            rate = 0.0004
        am = amount * rate
        rest = amount - am
        return {"rest": rest, "fees": am}

    def execute_market_trade(self, ob, side, qtt):
        """returns a dict {price, qtt, value, fee}"""
        if side == "buy":
            side = "sell"
        elif side == "sell":
            side = "buy"
        rest = qtt
        lsp = []
        lsq = []
        n = 0
        while rest > 0:
            price = float(ob[side].iloc[n]["price"])
            ob_qtt = float(ob[side].iloc[n]["qtt"])
            if rest <= ob_qtt:
                lsp.append(price)
                lsq.append(rest)
                rest = 0
            if rest > ob_qtt:
                lsp.append(price)
                lsq.append(ob_qtt)
                rest -= ob_qtt
            n += 1
        df = pd.DataFrame({"price": lsp, "qtt": lsq})
        df["total"] = df["price"] * df["qtt"]
        ###added a )
        avg = sum(df["total"]) / sum(df["qtt"])
        qtt = sum(df["qtt"])
        fee = self.get_fees(avg * sum(df["qtt"]), "market")
        return {
            "price": round(avg, 4),
            "qtt": round(qtt, 6),
            "value": round(avg * qtt, 4),
            "fee": round(fee["fees"], 4),
        }

class Datamanager():
    def __init__(self) -> None:
        pass

    def get_data(self, df):
        

        if len(df)>1:
            # print(df)
            lsv = df["value"].tolist()
            lss = df["side"].tolist()
            lsf = df["fee"].tolist()
            lsp = []
            lsa = []
            n = 0
            while n < len(lsv):
                if n % 2 == 0:
                    lsp.append(None)
                    lsa.append(None)
                else:
                    if lss[n] == "sell":
                        lsp.append(lsv[n] - lsv[n - 1])
                    elif lss[n] == "buy":
                        lsp.append(lsv[n - 1] - lsv[n])
                    prof = lsp[n] - (lsf[n] + lsf[n - 1])
                    lsa.append(round(prof, 4))
                n += 1
            # print(lsp)
            df["profit"] = lsp
            # print(df)
            df["date"] = pd.to_datetime(df["date"])

            df["adjusted"] = lsa
            profit = round(df["profit"].sum(), 4)
            fees = round(df["fee"].sum(), 4)
            net = round(df["adjusted"].sum(), 4)
            trade_amount = int(len(df) / 2)
            time_span = df["date"].iloc[-1] - df["date"].iloc[0]

            print(
                "PROFIT : {}\n"
                "FEES : {}\n"
                "NET PROFIT : {}\n"
                "TOTAL TRADES : {}\n"
                "TIME SPAN :{}".format(profit, fees, net, trade_amount, time_span)
            )
            # print(df)
            df= df[['date','exchange', 'side', 'price', 'qtt', 'value', 'fee', 'profit', 'adjusted']]
            return {
                "df": df,
                "profit": profit,
                "fees": fees,
                "net": net,
                "time_span": time_span,
                "total_trades": trade_amount,
            }

    def compare_data(self, botname_list):

        ls = botname_list
        path = "csv/"
        globdf = pd.DataFrame(
            columns=[
                "time_span",
                "bot_name",
                "trade_qtt",
                "profit",
                "fees",
                "net_profit",
            ]
        )
        for botname in botname_list:
            df = pd.read_csv(f"{path}trade_hist_{botname}.csv")
            if len(df)>1:
                print(botname)
                anal = self.get_data(df)
                ls = [
                    anal["time_span"],
                    botname,
                    anal["total_trades"],
                    anal["profit"],
                    anal["fees"],
                    anal["net"],
                ]
                globdf.loc[len(globdf)] = ls
        print(globdf)
        return globdf

    def transpose_ticker(self, ticker):
        '''transposes the ticker from kraken format to binance fomat'''

        if ticker == 'XBT/USDT':
            ticker = 'BTCUSDT'
            return ticker
        else :              
            s = ticker
            s = s.replace('/','')
            return s

class Matrix():
    def __init__(self, botlist=[]) -> None:


        self.botlist = botlist
        self.tickerlist = self.get_tickerlist()
        self.new_trades = {}
        self.obs = {}
        self.managers = {}

    def get_tickerlist(self):
        ls = []
        for bot in self.botlist:
            if bot.ticker not in ls:
                ls.append(bot.ticker)
        return ls

    def get_new_trades_data(self, bot):
        """returns the df if new trades, else returns false"""
        last_trade_bot = bot.last_trade
        # print(last_trade_bot)

    def run_main(self):
        # print(self.tickerlist)
        n = 0
        self.managers['trade']=Krak_man(self.tickerlist)
        self.managers['hedge']=Bin_man(self.tickerlist)
        self.managers['hedge'].ticker_list = self.managers['hedge'].get_ticker(self.tickerlist)

        # demarrer la collecte de data
        # print(self.managers['hedge'].ticker_list)
        # input()
        for ticker in self.managers['hedge'].ticker_list:
            tbin = threading.Thread(target=self.managers['hedge'].loop_ob, args = [ticker]) #VOIR POUR FAIR UN FOR LOOP AVEC TICKERLIST
            tbin.start()
        tkrak = threading.Thread(target=self.managers['trade'].gather_data, args = [self.tickerlist])
        tkrak.start()
        print('attributin managers to bots')
        for bot in self.botlist:
            bot.trade_manager = self.managers['trade']
            bot.hedge_manager = self.managers['hedge']
            bot.init_wallet()
            bot.init_transac_hist()
            print(f'{bot.name} got its mnanagers')
        # print(n)    
        run=True
        while run:
            # print(n)
            verifk = []
            verifb = []
            for pair in self.managers['hedge'].ticker_list:
                while pair not in self.managers['hedge'].books:
                    pass
                    # print(f'{pair} not in books yet')
            for pair in self.managers['trade'].ticker_list:
                while pair not in self.managers['trade'].books:
                    pass
                    # print(f'{pair} not in books yet')
            print('MANAGERS AND BOTS INITIALISED')
            print('LETS FUCKING GOOOOOOOOOO')
            run = False
            # print(self.managers['hedge'].books)
            # print(self.managers['trade'].books)
            # for bot in self.botlist:
            #     bot.run()

        run = True
        n = 0
        while run:
            if n%1000 == 0:
                st = time.time()
            th = self.managers['trade'].trade_hist
            # print(f'from main {th}')
            ls = []
            for bot in self.botlist:
                # print(bot.orders)
                t = threading.Thread(target=bot.run)
                t.start()
                ls.append(t)
            for thread in ls:
                thread.join()
            if n%1000 == 0:
                nd = time.time()
                print(f'last 1000 cycles, average cycle time : {nd-st}')
                print(f'cycle {n}')
            n+=1

