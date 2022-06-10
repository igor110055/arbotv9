from datetime import datetime
from hashlib import new
from tracemalloc import start
from cstr import Krak_man, Bin_man, calc, Datamanager
from os import path
import time
import pandas as pd
import threading



c = calc()
d=Datamanager()
class Bot():
    def __init__(self, ticker, name, threshold, start_capital, delta = 0.10) -> None:
        #pas besoin de call les managers, on le fera dans matrix.
        """Params = 
        trade_manager :  the exchange where the bot sets orders(the illiquid one)
        hedge manager : the exchange where the bot will hedge (the liquid one)
        ticker = format kraken ex : XBT/USDT ou ETH/USDT
        name : the bot name
        threshold : % of spread for which the bot will set orders
        start_capital : must be a dict {crypto:amount, fiat:amount}
        delta : delta between price and order
        self.last_trade is triggered by the check new trades fct in Matrix"""
        self.trade_manager = None   
        self.hedge_manager = None
        self.name = name
        self.threshold = float(threshold)
        self.start_capital = start_capital
        self.orders = {'buy':False, 'sell':False}
        self.wallet= None
        self.transac_hist = None
        self.ticker = ticker
        self.delta = delta
        self.last_trade = False
        
    def get_delta(self, trade_ob, hedge_ob):

        """Returns a dict {buy, sell}
        if spread is < threshold, returns False"""
        hfirst_sell =float(hedge_ob['sell'].iloc[0]['price'])
        hfirst_buy = float(hedge_ob['buy'].iloc[0]['price'])
        # last_price = float(self.hedge_manager.get_trades()['price'].iloc[-1])
        first_sell = float(trade_ob['sell'].iloc[0]['price'])
        first_buy = float(trade_ob['buy'].iloc[0]['price'])
        buy_delta = round(hfirst_sell-first_buy,6)
        sell_delta = round(first_sell-hfirst_buy,6)
        buy_spread = round(buy_delta/first_sell*100, 3)
        sell_spread = round(sell_delta/first_buy*100, 3)
        if buy_spread < self.threshold:
            buy_spread = False
        if sell_spread < self.threshold:
            sell_spread = False
        return {'buy':buy_spread, 'sell':sell_spread}

    def run(self):
        newtrades = self.get_new_trades()
        #check if there are new trades:
        # st = time.time()
        # new_trades = self.trade_manager.check_new_trades()

        # print(obs)
        # print(f'new trades for {self.name} : {new_trades}')
        lsside = ['buy', 'sell']
        threadlist = []
        nd = time.time()
        # print(f'{self.name} fetching new trades done in {round(nd-st,2)}')
        # st = time.time()
        def do_the_stuff(side):
            if isinstance(newtrades, pd.DataFrame):
                # print(new_trades)
                # print(f'{side} order : {self.orders[side]}')
                if self.orders[side]:               
                    self.execute_order(side, self.orders[side], newtrades)
                    # print('WE TRADED')
                    # time.sleep()
            delta = self.get_delta(self.trade_manager.books[self.ticker],self.hedge_manager.books[d.transpose_ticker(self.ticker)])
            ###PASS THE ORDERS
            if delta[side]:
                if isinstance(self.orders[side], dict):
                    # print(f'{side} active orders detected')
                    first = self.check_if_order_is_first(self.orders[side], side, self.trade_manager.books[self.ticker])
                    if first:
                        # print('all good we are first')
                        pass
                    elif not first:
                        # print('We have been frontrunned ! Canceling order')
                        self.orders[side]=False
                        # print(f'Setting new {side} orders')
                        self.set_order(side,self.trade_manager.books[self.ticker])
                else:
                    # print(f'No {side} active orders, setting new')
                    self.set_order(side, self.trade_manager.books[self.ticker])
            elif not delta[side]:
                # print(f'Not enough spread to {side}')
                if isinstance(self.orders[side], dict):
                    # print(f'cancelling {side} orders')
                    self.orders[side]=False
                else:
                    # print(f'no {side} orders to cancel')
                    pass
        for side in lsside:
            t = threading.Thread(target=do_the_stuff, args=[side])
            t.start()
            threadlist.append(t)
        for thread in threadlist:
            thread.join()

    def init_wallet(self):
        """initialise the csv file for the wallet, returns the DF"""
        if path.exists(f'csv/wallet_{self.name}.csv'):
            print(f'{self.name}wallet found')
            wallet = pd.read_csv(f'csv/wallet_{self.name}.csv')
            self.wallet=wallet
        else:
            print(f'creating wallet for {self.name}')
            crypto = self.start_capital['crypto']/2
            fiat = self.start_capital['fiat']/2

            wallet = pd.DataFrame({'date':datetime.now(), f'{self.trade_manager.name}_crypto':crypto,
            f'{self.trade_manager.name}_fiat':fiat,
            f'{self.hedge_manager.name}_crypto':crypto, 
            f'{self.hedge_manager.name}_fiat':fiat}, index=[0])
            wallet.to_csv(f'csv/wallet_{self.name}.csv', index=False)
            self.wallet = wallet
        # print(wallet)
        return wallet

    def init_transac_hist(self):
            if path.exists('csv/trade_hist_{}.csv'.format(self.name)):
                print(f'{self.name}trade_hist found')
                t_hist = pd.read_csv('csv/trade_hist_{}.csv'.format(self.name))
            else :
                print(f'{self.name}creating trade hist')
                t_hist = pd.DataFrame(columns=['date', 'exchange', 'side', 'price','qtt', 'value', 'fee'])
                t_hist.to_csv('csv/trade_hist_{}.csv'.format(self.name), index=False)
            return t_hist

    def set_order(self, side, ob):
        st = time.time()
        """sets the order (fake)
        returns a dict {price, qtt, value}"""
        first = float(ob[side].iloc[0]['price'])
        if side =='buy':
            price = first + self.delta
        elif side =='sell':
            price = first - self.delta
        value = float(self.wallet[f'{self.trade_manager.name}_fiat'].iloc[-1])*0.15
        qtt = value/price
        # print(first)
        # print(price)
        order = {'price':round(price,4), 'qtt':round(qtt,6), 'value':round(value,4)}
        self.orders[side] = order
        nd = time.time()
        # print(f'{self.name} order setting done in {round(nd-st,2)}')
        return order

    def check_if_order_is_first(self, order, side, ob_trade):
        st = time.time()
        price = order['price']
        if side == 'buy':
            ###REMPLACER ICI PAR LE DATA GATHERING
            if price == float(ob_trade[side].iloc[0]['price']) + self.delta:
                nd = time.time()
                # print(f'{self.name} chekcing order first done in {round(nd-st,2)}')
                return True
            else:
                nd = time.time()
                # print(f'{self.name} chekcing order first done in {round(nd-st,2)}')
                return False
        elif side == 'sell':
            if price == float(ob_trade[side].iloc[0]['price']) - self.delta:
                nd = time.time()
                # print(f'{self.name} chekcing order first done in {round(nd-st,2)}')
                return True
            else:
                # print(f'{side} order is no good')
                nd = time.time()
                # print(f'{self.name} chekcing order first done in {round(nd-st,2)}')
                return False

    def execute_order(self, side, order, last_trades):
        '''execute the trade and the hedge
        store the data'''
        if side == 'buy':

            df = last_trades[last_trades['side'] == 's']
            df['price'] = df['price'].astype(float)
            df = df[(df['price']) <= float(order['price'])]
            # print(df)
            if not df.empty:
                print('last trades are :')
                print(df)
                # print('SELL TRADES COMPAT')
                # print(df)
                # input()
                # print(type(df['qtt'].sum()))
                # print(f'We are going to execute this {side} order{self.orders[side]}')
                if df['qtt'].sum() >= float(self.orders[side]['qtt']):
                    print('order fully filled')
                    
                elif df['qtt'].sum() < float(self.orders[side]['qtt']):
                    qtt = self.orders[side]['qtt']
                    self.orders[side]['qtt'] = df['qtt'].sum()
                    #store the trade:
                fee = c.get_fees(self.orders[side]['qtt']*self.orders[side]['price'])
                trade = {
                'price':round(self.orders[side]['price'],6), 
                'qtt':round(self.orders[side]['qtt'],6),
                'value': round(self.orders[side]['qtt']*self.orders[side]['price'],6),
                'fee':round(fee['fees'],6)         
                }
                #execute hedge
                hedge = c.execute_market_trade(self.hedge_manager.books[d.transpose_ticker(self.ticker)], side, self.orders[side]['qtt'])
                # print(f'the trade is {trade}')
                # print(f'the hedge is {hedge}')
                self.store_trade(trade, hedge, side)
                # input()        
            elif df.empty:
                # print(f'no good trades detected')
                pass



        else:
            df = last_trades[last_trades['side'] == 'b']
            df['price'] = df['price'].astype(float)
            df = df[df['price'] >= order['price']]
            # print(df)   
            if not df.empty:
                print('df is ')
                # print('SELL TRADES COMPAT')
                print(df)
                if df['qtt'].sum() >= self.orders[side]['qtt']:
                    print('order fully filled')
                elif df['qtt'].sum() < self.orders[side]['qtt']:
                    qtt = self.orders[side]['qtt']
                    self.orders[side]['qtt'] = df['qtt'].sum()
                #store the trade:
                fee = c.get_fees(self.orders[side]['qtt']*self.orders[side]['price'])
                trade = {
                'price':round(self.orders[side]['price'],6), 
                'qtt':round(self.orders[side]['qtt'],6),
                'value': round(self.orders[side]['qtt']*self.orders[side]['price'],6),
                'fee':round(fee['fees'],6)         
                }
                #execute hedge
                hedge = c.execute_market_trade(self.hedge_manager.books[d.transpose_ticker(self.ticker)], side, self.orders[side]['qtt'])
                print(f'the trade is {trade}')
                print(f'the hedge is {hedge}')

                self.store_trade(trade, hedge, side)
                # input()
            elif df.empty:
                # print(f'no good trades detected')
                pass
        
    def store_trade(self, trade, hedge, side):
        '''stores in trade history and does the wallets transaction
        trade must be a dict {'qtt', 'price'}'''
                #STORE IN HIST AND UPDATE WALLET
        if side == 'buy':
            hside ='sell'
        elif side =='sell':
            hside = 'buy'
        trade_df = pd.DataFrame({
            "date":datetime.now(),
            'exchange': self.trade_manager.name,
            'side' : side,
            'price':trade['price'],
            'qtt':trade['qtt'],
            'value':trade['value'],
            'fee':trade['fee']            
            }, index=[0])
        hedge_df = pd.DataFrame({
            "date":datetime.now(),
            'exchange': self.hedge_manager.name,
            'side' : hside,
            'price':hedge['price'],
            'qtt':hedge['qtt'],
            'value':hedge['value'],
            'fee':hedge['fee']            
            }, index=[0])
        df = pd.concat([self.transac_hist, trade_df, hedge_df], ignore_index=True)
        self.transac_hist = df
        self.transac_hist.to_csv(f'csv/trade_hist_{self.name}.csv')
        # print('transac hist updated')
        #UPDATE WALLETS
        if side == 'buy':
            wallet_df = pd.DataFrame({
                'date':datetime.now(),
                f'{self.trade_manager.name}_crypto': self.wallet[f'{self.trade_manager.name}_crypto'].iloc[-1] + trade['qtt'],
                f'{self.trade_manager.name}_fiat': self.wallet[f'{self.trade_manager.name}_fiat'].iloc[-1] - trade['value']-trade['fee'],
                f'{self.hedge_manager.name}_crypto': self.wallet[f'{self.hedge_manager.name}_crypto'].iloc[-1] - hedge['qtt'],
                f'{self.hedge_manager.name}_fiat': self.wallet[f'{self.hedge_manager.name}_fiat'].iloc[-1] + hedge['value']-hedge['fee']
            }, index = [0])
        elif side == 'sell':
            wallet_df = pd.DataFrame({
                'date':datetime.now(),
                f'{self.trade_manager.name}_crypto': self.wallet[f'{self.trade_manager.name}_crypto'].iloc[-1] - trade['qtt'],
                f'{self.trade_manager.name}_fiat': self.wallet[f'{self.trade_manager.name}_fiat'].iloc[-1] + trade['value']-trade['fee'],
                f'{self.hedge_manager.name}_crypto': self.wallet[f'{self.hedge_manager.name}_crypto'].iloc[-1] + hedge['qtt'],
                f'{self.hedge_manager.name}_fiat': self.wallet[f'{self.hedge_manager.name}_fiat'].iloc[-1] - hedge['value']-hedge['fee']
            }, index=[0])
        df = pd.concat([self.wallet, wallet_df], ignore_index=True)
        self.wallet = df
        self.wallet.to_csv(f'csv/wallet_{self.name}.csv', index=False)
        # print('wallet updated')
        print(f"{self.name} traded at {datetime.now()}")
        print(f"trade {side} :{trade['qtt']} @ {trade['price']}$, hedge {hside}: {trade['qtt']} @ {hedge['price']}$")
        print('\n')

    def get_total_balance(self):
        '''returns a dict {fiat, crypto}'''
        wallet = self.wallet
        fiat = round(wallet[f'{self.trade_manager.name}_fiat'].iloc[-1]+wallet[f'{self.hedge_manager.name}_fiat'].iloc[-1],4)      
        crypto = round(wallet[f'{self.trade_manager.name}_crypto'].iloc[-1]+wallet[f'{self.hedge_manager.name}_crypto'].iloc[-1],6)
        fees = round(sum(self.transac_hist['fee']),4)
        return {'fiat':fiat, 'crypto':crypto, 'fee':fees}

    def get_new_trades(self):
        try:
            self.last_trade['unix']
        except TypeError:
            # print('no las trade yet')
            try:
                self.last_trade = self.trade_manager.trade_hist[self.ticker].iloc[-1]
                
                return self.trade_manager.trade_hist[self.ticker].iloc[1:]
            except KeyError:
                # print(f'no new trades for {self.name}')
                return False
        # print(self.last_trade)
        if self.last_trade.equals(self.trade_manager.trade_hist[self.ticker].iloc[-1]):
            # print('equal, no new trades')
            return False
        
        
        elif not self.last_trade.equals(self.trade_manager.trade_hist[self.ticker].iloc[-1]):
            # print('not equal, new trades')
            manager_df = self.trade_manager.trade_hist[self.ticker]
            last_unix = self.last_trade['unix']
            df = manager_df[manager_df['unix']> last_unix]
            # print('these are the last trades')
            # print(df)
            self.last_trade = manager_df.iloc[-1]
            # print('this is the last trade')
            # print(self.last_trade)
            return df
        else :
            print('wtf this should not appear')
        
            ############WORKING HERE, KEU ERROR AND STUFF
        


