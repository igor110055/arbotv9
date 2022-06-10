from datetime import datetime
from numpy import blackman, linspace
import pandas as pd
import names as nm
from wonderwords import RandomWord
from cstr import Bin_man, Krak_man
import threading
from os import path
import os
import time


r = RandomWord()


class constructor:
    def __init__(self) -> None:
        pass

    def transpose_ticker(self, ticker):
        """transposes the ticker from kraken format to binance fomat"""

        if ticker == "XBT/USDT":
            ticker = "BTCUSDT"
            return ticker
        else:
            s = ticker
            s = s.replace("/", "")
            return s

    def get_fees(self, value, order_type="limit"):
        if order_type == "limit":
            rate = 0.0002
        else:
            rate = 0.0004
        am = value * rate
        rest = value - am
        return {"rest": rest, "fees": am}


c = constructor()


class Botmaker:

    """creates bots with specified params
    ticker list : list of tiokers in format 'COIN/USDT'
    crit_spread :  the crit spread applied to bot
    capital : the capital applied to bot"""

    def __init__(self, ticker_list, crit_spread_list, capital, load=False) -> None:
        self.load = load
        self.botlist = False
        self.ticker_list = ticker_list
        self.crit_spread_list = crit_spread_list
        self.crit_spread = False
        self.capital = capital

    def generate_name(self):
        fname = nm.get_first_name(gender="female")
        adj = r.word(include_parts_of_speech=["adjectives"])
        name = f"{fname}_the_{adj}"
        return name

    def generate_bot_list(self):
        ls = []
        lsname = []
        for ticker in self.ticker_list:
            for crit_spread in self.crit_spread_list:
                run=True
                while run:
                    name = self.generate_name()
                    if name not in lsname:
                        lsname.append(name)
                        run = False
                ls.append(Bot(ticker, name, self.capital, crit_spread))
                # print(ls)
        return ls

    def store_bot_list(self, botlist):
        pass


class Bot:
    """Creates the bot, the args are passed by botmanager,
    trade manager, hedge manger and wallet needs to be initalised in matrix main loop"""

    def __init__(self, ticker, name, capital, crit_spread):
        self.ticker = ticker
        self.name = name
        self.capital = float(capital)
        self.crit_spread = float(crit_spread)
        self.trade_manager = False
        self.hedge_manager = False
        self.wallet = False
        self.transac_hist = False
        self.last_trade = False
        self.decimals = False
        self.orders = {"buy": False, "sell": False}
        self.restriction = {"buy": False, "sell": False}

    def get_delta(self):
        """Returns a dict {buy, sell}
        if spread is < threshold, returns False"""
        hfirst_sell = self.hedge_manager.books[c.transpose_ticker(self.ticker)][
            "sell"
        ].iloc[0]["price"]
        hfirst_buy = self.hedge_manager.books[c.transpose_ticker(self.ticker)][
            "buy"
        ].iloc[0]["price"]
        # last_price = float(self.hedge_manager.get_trades()['price'].iloc[-1])
        first_sell = self.trade_manager.books[self.ticker]["sell"].iloc[0]["price"]
        first_buy = self.trade_manager.books[self.ticker]["buy"].iloc[0]["price"]
        # print(hfirst_sell)
        # print(type(hfirst_sell))
        # print(type(first_buy))
        buy_delta = hfirst_sell - first_buy
        sell_delta = first_sell - hfirst_buy
        buy_spread = buy_delta / first_sell * 100
        sell_spread = sell_delta / first_buy * 100
        if buy_spread < self.crit_spread:
            buy_spread = False
        if sell_spread < self.crit_spread:
            sell_spread = False
        return {"buy": buy_spread, "sell": sell_spread}

    def init_wallet(self):
        """calculate the crypto amount, makes the csv file for the wallet, returnd the DF"""
        if path.exists(f"csv/wallet_{self.name}.csv"):
            print(f"{self.name}wallet found")
            wallet = pd.read_csv(f"csv/wallet_{self.name}.csv", index_col=False)
            self.wallet = wallet
            #printwallet)
        else:
            crypto_amount = (self.capital / 4) / self.trade_manager.books[self.ticker][
                "sell"
            ].iloc[0]["price"]
            fiat_amount = self.capital / 4
            df = pd.DataFrame(
                {
                    "date": datetime.now(),
                    f"{self.trade_manager.name}_crypto": crypto_amount,
                    f"{self.trade_manager.name}_fiat": fiat_amount,
                    f"{self.hedge_manager.name}_crypto": crypto_amount,
                    f"{self.hedge_manager.name}_fiat": fiat_amount,
                },
                index=[0],
            )
            df.to_csv(f"csv/wallet_{self.name}.csv", index=False)
            self.wallet = df

    def get_decimals(self, num):
        """to see how many decimals to adjust the trade amount for being first"""
        string = str(num)
        dec = len(string.split(".")[1])
        #print(dec)
        diff = "0."
        for n in range(dec):
            if n == dec - 1:
                diff += "1"
            else:
                diff += "0"
        #print(f"diff is {diff}")
        self.decimals = float(diff)

    def init_transac_hist(self):
        """creates or loads the DF"""
        if path.exists("csv/trade_hist_{}.csv".format(self.name)):
            #print(f"{self.name}trade_hist found")
            t_hist = pd.read_csv(
                "csv/trade_hist_{}.csv".format(self.name), index_col=False
            )
        else:
            #print(f"{self.name}creating trade hist")
            t_hist = pd.DataFrame(
                columns=["date", "exchange", "side", "price", "qtt", "value", "fee"]
            )
            t_hist.to_csv("csv/trade_hist_{}.csv".format(self.name), index=False)
        self.transac_hist = t_hist
        return t_hist

    def set_order(self, side, target="fiat", multiplicator=0.2, fake=False):
        """'Sets the order according to the config of the bot, returns a dict {price, qtt, value}
        target valid params are 'crypto' or 'fiat', how the profit is made
        fake to get the price to restrict"""

        if side == "buy":
            if not fake:
                if not self.restriction["buy"]:
                    price = (
                        self.trade_manager.books[self.ticker][side].iloc[0]["price"]
                        + self.decimals
                    )
                else:
                    return False
            else:
                price = (
                    self.trade_manager.books[self.ticker][side].iloc[0]["price"]
                    + self.decimals
                )
        elif side == "sell":
            if not fake:
                if not self.restriction["sell"]:
                    price = (
                        self.trade_manager.books[self.ticker][side].iloc[0]["price"]
                        - self.decimals
                    )
                else:
                    return False
            else:
                price = (
                    self.trade_manager.books[self.ticker][side].iloc[0]["price"]
                    - self.decimals
                )

        if target == "fiat":
            value = (
                self.wallet[f"{self.trade_manager.name}_fiat"].iloc[-1] * multiplicator
            )
            qtt = float(value / price)
        elif target == "crypto":
            qtt = (
                self.wallet[f"{self.trade_manager.name}_crypto"].iloc[-1]
                * multiplicator
            )
            value = float(qtt * price)
        order = {"side": side, "price": price, "qtt": qtt, "value": value}
        if not fake:
            self.orders[side] = order
        return order

    def check_if_order_is_first(self, order):
        """returns bool if order is or not longer at the top"""
        dct = {}
        first = self.trade_manager.books[self.ticker][order["side"]].iloc[0]["price"]
        if order["side"] == "buy":
            price = first + self.decimals

        elif order["side"] == "sell":
            price = first - self.decimals
        if price == order["price"]:
            return True
        else:
            #print("Not first")
            return False

    def execute_order(self, order):
        """checks if the order got filled
        returns a dict with the amount liquidated or False, updates order"""
        # check if filled
        last_trades = self.get_last_trades()
        #print(last_trades)
        if isinstance(last_trades, pd.DataFrame):
            df = last_trades
            if order["side"] == "buy":
                df = df[df["price"] <= order["price"]]
                if df.empty:
                    #print('the df is empty')
                    return False
                #print("this is the valid trades")
                #print(df)
                #print("some got filled")
                if df["qtt"].sum() >= order["qtt"]:
                    #print("ORDER FULLY FILLED")
                    self.orders[order["side"]] = False
                    #print(order)
                    return order
                else:
                    order["qtt"] = float(df["qtt"].sum())
                    order["value"] = float(df["qtt"].sum() * order["price"])
                    # update the active order, for now cancel it
                    self.orders[order["side"]] = False
                    # #print(self.orders)
                    # self.orders[order['side']]['qtt'] -= df['qtt'].sum()
                    # self.orders[order['side']]['value'] = self.orders[order['side']]['qtt']*self.orders[order['side']['price']]
                    #print(order)
                    return order



            elif order["side"] == "sell":
                df = df[df["price"] >= order["price"]]
                if df.empty:
                    #print('the df is empty')
                    return False
                #print("this is the valid trades")
                #print(df)
                #print("some got filled")
                if df["qtt"].sum() >= order["qtt"]:
                    #print("ORDER FULLY FILLED")
                    self.orders[order["side"]] = False
                    #print(order)
                    return order
                else:
                    order["qtt"] = float(df["qtt"].sum())
                    order["value"] = float(df["qtt"].sum() * order["price"])
                    # update the active order, for now cancel it
                    self.orders[order["side"]] = False
                    # #print(self.orders)
                    # self.orders[order['side']]['qtt'] -= df['qtt'].sum()
                    # self.orders[order['side']]['value'] = self.orders[order['side']]['qtt']*self.orders[order['side']['price']]
                    #print(order)
                    return order
        else:
            return False

    def hedge_market(self, exec_order):
        #print("hedging")
        qtt = exec_order["qtt"]
        # #print(self.hedge_manager.books[c.transpose_ticker(self.ticker)]['sell'])
        if exec_order["side"] == "buy":
            side = "sell"
        elif exec_order["side"] == "sell":
            side = "buy"
        rest = qtt
        ob = self.hedge_manager.books[c.transpose_ticker(self.ticker)][side]
        lsprice = []
        lsqtt = []
        n = 0
        while rest > 0:
            if ob.iloc[n]["qtt"] >= rest:
                lsprice.append(ob.iloc[n]["price"])
                lsqtt.append(rest)
                rest = 0
            else:
                lsprice.append(ob.iloc[n]["price"])
                lsqtt.append(ob.iloc[n]["qtt"])
                rest -= ob.iloc[n]["qtt"]
                n += 1

        df = pd.DataFrame({"price": lsprice, "qtt": lsqtt})
        df["value"] = df["price"] * df["qtt"]
        # #print('HEDGE DF')
        # #print(df)
        avg = df["value"].sum() / df["qtt"].sum()
        exec = {
            "side": side,
            "price": avg,
            "qtt": df["qtt"].sum(),
            "value": df["value"].sum(),
        }
        #print(f"hedge is : {exec}")
        #print(exec)
        return exec

    def get_last_trades(self):
        #print("last trade is :")
        #print(self.last_trade)
        try:
            df = self.trade_manager.trade_hist[self.ticker]
            last_trades = df
        except KeyError:
            #print("no trades yet")
            return False

        if isinstance(self.last_trade, pd.Series):
            if self.last_trade.equals(df.iloc[-1]):
                #print("no new trade :")
                #print(self.last_trade)
                #print(df.iloc[-1])
                return False
            else:
                last_trades = df[df["unix"] > self.last_trade["unix"]]
                self.last_trade = df.iloc[-1]
                return last_trades
        else:

            self.last_trade = df.iloc[-1]

            return last_trades

    def restrict(self, margin=1):

        available_amounts = {
            "fiat": self.wallet[f"{self.hedge_manager.name}_fiat"].iloc[-1],
            "crypto": self.wallet[f"{self.hedge_manager.name}_crypto"].iloc[-1],
        }
        # #print(available_amounts)
        ls = ["buy", "sell"]
        for side in ls:
            if side == "buy":
                order = self.set_order(side, fake=True)
                # #print(order)
                qtt = order["qtt"]
                # #print(order['qtt'])
                # #print(available_amounts['crypto'])
                #print(f"min qtt to hedge : {qtt}")
                if available_amounts["crypto"] < qtt * margin:
                    # print("NOT ENOUGH TO HEDGE")
                    self.restriction["buy"] = True
                else:
                    self.restriction["buy"] = False

            elif side == "sell":
                order = self.set_order(side, fake=True)
                # print(order)
                value = order["value"]
                #print(f"min value to hedge : {value}")
                if available_amounts["fiat"] < value * margin:
                    # print("NOT ENOUGH TO HEDGE")
                    self.restriction["sell"] = True
                else:
                    self.restriction["sell"] = False

    def run(self):
        def loop(side):
            #print(f"START LOOP {self.ticker}")
            # see and execute if rders filled
            #print("checking for orders")
            if isinstance(self.orders[side], dict):
                #print(f"orders are {self.orders}")
                exec = self.execute_order(self.orders[side])
                if isinstance(exec, dict):
                    hedge = self.hedge_market(exec)
                    self.store_trades(exec, hedge)
            else:
                pass
                #print("no orders")
            # make permissions
            self.restrict()
            #print("restrictions are :")
            #print(self.restriction)
            # set orders
            #print(f"CHECKING {side} DELTA")
            delta = self.get_delta()
            #print(delta)
            for side, value in delta.items():
                #print(side, value)
                if isinstance(value, float):
                    #print(f"{side} DELTA IS VALID")
                    if isinstance(self.orders[side], dict):
                        first = self.check_if_order_is_first(self.orders[side])
                        if first:
                            #print("order is first")
                            return
                        else:
                            #print("order is not first, cancelling")
                            self.orders[side] = False
                    self.set_order(side)
                    #print(self.orders)
                elif not value:
                    #print("NO GOOD DELTA")
                    if isinstance(self.orders[side], dict):
                        #print(f"Cancelling order {self.orders[side]}")
                        self.orders[side] = False

        lsside = ["buy", "sell"]
        lst = []
        for side in lsside:
            loop(side)
            # t = threading.Thread(target=loop, args = [side])
            # t.start()
            # lst.append(t)
        # for th in lst:
        # th.join()

    def store_trades(self, exec, hedge):
        df = self.transac_hist
        wallet = self.wallet
        # for trade :
        trade_balance_fiat = wallet[f"{self.trade_manager.name}_fiat"].iloc[-1]
        trade_balance_crypto = wallet[f"{self.trade_manager.name}_crypto"].iloc[-1]
        fee = c.get_fees(exec["value"])["fees"]
        # get data in the trade hist
        df.loc[len(df)] = [
            datetime.now(),
            self.trade_manager.name,
            exec["side"],
            exec["price"],
            exec["qtt"],
            exec["value"],
            fee,
        ]
        # gather data for wallet
        if exec["side"] == "buy":
            tfiat_balance = trade_balance_fiat - exec["value"] - fee
            tcrypto_balance = trade_balance_crypto + exec["qtt"]
        elif exec["side"] == "sell":
            tfiat_balance = trade_balance_fiat + exec["value"] - fee
            tcrypto_balance = trade_balance_crypto - exec["qtt"]

        # for hedge
        fee = c.get_fees(hedge["value"], "market")["fees"]
        hedge_balance_fiat = wallet[f"{self.hedge_manager.name}_fiat"].iloc[-1]
        hedge_balance_crypto = wallet[f"{self.hedge_manager.name}_crypto"].iloc[-1]
        df.loc[len(df)] = [
            datetime.now(),
            self.hedge_manager.name,
            hedge["side"],
            hedge["price"],
            hedge["qtt"],
            hedge["value"],
            fee,
        ]
        # gather data for wallet
        if hedge["side"] == "buy":
            hfiat_balance = hedge_balance_fiat - hedge["value"] - fee
            hcrypto_balance = hedge_balance_crypto + hedge["qtt"]
        elif hedge["side"] == "sell":
            hfiat_balance = hedge_balance_fiat + hedge["value"] - fee
            hcrypto_balance = hedge_balance_crypto - hedge["qtt"]

        # save trade_hist:
        df.to_csv(f"csv/trade_hist_{self.name}.csv", index=False)
        self.transac_hist = df

        # add row and save wallet
        wallet.loc[len(wallet)] = [
            datetime.now(),
            tcrypto_balance,
            tfiat_balance,
            hcrypto_balance,
            hfiat_balance,
        ]
        self.wallet = wallet
        wallet.to_csv(f"csv/wallet_{self.name}.csv", index=False)

        # #print the trade
        df = df.iloc[-2:]
        if df.iloc[-1]["side"] == "buy":
            profit = df.iloc[-2]["value"] - df.iloc[-1]["value"]
        elif df.iloc[-1]["side"] == "sell":
            profit = df.iloc[-1]["value"] - df.iloc[-2]["value"]
        fees = df["fee"].sum()
        net = profit - fees
        #print("TRADE STORED :")
        #print(df)
        #print(f"PROFIT : {profit}, FEE : {fee}, NET PROFIT : {net}")
        #print(wallet)


class Main_prog:
    """for test pupposes
    load from file : put path if load"""

    def __init__(self, botlist, load_from_file=False):
        if not load_from_file:
            self.raw_botlist = botlist
            self.botlist = self.get_botlist()
            self.ticker_list = self.get_tickerlist()

        else:
            self.botlist = []
            df = pd.read_csv(load_from_file, index_col=False)
            for n in range(len(df)):
                self.botlist.append(
                    Bot(
                        df.iloc[n]["ticker"],
                        df.iloc[n]["name"],
                        1,
                        df.iloc[n]["crit_spread"],
                    )
                )
            self.ticker_list = df["ticker"].drop_duplicates().to_list()
        self.new_trades = {}
        self.obs = {}
        self.managers = {}

    def load_from_file(self):
        """reads the bots.csv file and generates the botlist"""
        pass

    def get_tickerlist(self):
        ls = []
        for bot in self.botlist:
            if bot.ticker not in ls:
                ls.append(bot.ticker)
        return ls

    def get_botlist(self):
        if any(isinstance(i, list) for i in self.raw_botlist):
            #print("nest detected")
            res = [item for sublist in self.raw_botlist for item in sublist]
            return res
        else:
            return self.raw_botlist

    def initialise(self):

        # Start managers
        self.managers["trade"] = Krak_man(self.ticker_list)
        self.managers["hedge"] = Bin_man(self.ticker_list)
        self.managers["hedge"].ticker_list = self.managers["hedge"].get_ticker(
            self.ticker_list
        )
        for ticker in self.managers["hedge"].ticker_list:
            tbin = threading.Thread(
                target=self.managers["hedge"].loop_ob, args=[ticker]
            )  # VOIR POUR FAIR UN FOR LOOP AVEC TICKERLIST
            tbin.start()
        tkrak = threading.Thread(
            target=self.managers["trade"].gather_data, args=[self.ticker_list]
        )
        tkrak.start()
        for pair in self.managers["hedge"].ticker_list:
            while pair not in self.managers["hedge"].books:
                pass
                # #print(f'{pair} not in books yet')
        for pair in self.managers["trade"].ticker_list:
            while pair not in self.managers["trade"].books:
                pass
                # #print(f'{pair} not in books yet')
        #print("all books gathered")
        # initialise bots
        for bot in self.botlist:
            bot.trade_manager = self.managers["trade"]
            bot.hedge_manager = self.managers["hedge"]
            bot.init_wallet()
            bot.get_decimals(
                bot.trade_manager.books[bot.ticker]["buy"].iloc[0]["price"]
            )
            bot.init_transac_hist()
        # store botlist if not present
        if os.path.isfile("csv/bots.csv"):
            #print("botlist detected")
            pass
        else:
            df = pd.DataFrame(columns=["name", "ticker", "crit_spread", "decimals"])
            n = 0
            for bot in self.botlist:
                df.loc[n] = [bot.name, bot.ticker, bot.crit_spread, bot.decimals]
                n += 1
            df.to_csv("csv/bots.csv", index=False)
        botnum = len(self.botlist)
        print(f'{botnum} bots loaded')
    
    def run_bots(self):
        n=0
        ls = []
        while True:
            if n%100 == 0:
                print(f"LOOP {n}")
                print(datetime.now())
                print("***---***\n")
            for bot in self.botlist:
                t= threading.Thread(target=bot.run)
                t.start
                bot.run()
            for th in ls :
                th.stop()
            n+=1
            