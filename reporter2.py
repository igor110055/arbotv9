from sys import path
from numpy import NaN
import pandas as pd
import os
import shutil
import datetime
import time

class Reporter():
    '''makes reports based on the bots.csv file'''
    def __init__(self, path_to_folder='csv/') -> None:
        self.path_to_folder= path_to_folder

    def get_bots(self):
        '''reads the bots.csv and creates a DF for the results'''
        df = pd.read_csv(f'{self.path_to_folder}bots.csv', index_col=False)
        return df

    def get_bot_params(self, botname):
        df = self.get_bots()
        ticker = df[df['name']==botname]['ticker'].iloc[0]
        crit_spread = df[df['name']==botname]['crit_spread'].iloc[0]
        return {'ticker':ticker, 'crit_spread':crit_spread}

    def get_individual_stats(self, botname):
        '''returns a dict and a DF to append to the general report'''
        wallet = pd.read_csv(f'{self.path_to_folder}wallet_{botname}.csv')
        wallet['date'] = pd.to_datetime(wallet['date'])

        def get_profits_lists(side_ls, value_ls, fee_ls):
            profit = []
            fee = []
            net = []
            n=0
            while n<len(side_ls):
                if n%2 ==0:
                    profit.append(NaN)
                    fee.append(NaN)
                    net.append(NaN)
                    
                else:
                    if side_ls[n] ==  'buy':
                        profit.append(value_ls[n-1]-value_ls[n])
                        fee.append(fee_ls[n]+fee_ls[n-1])
                        net.append(profit[-1]-fee[-1])
                    elif side_ls[n] ==  'sell':
                        profit.append(value_ls[n]-value_ls[n-1])
                        fee.append(fee_ls[n]+fee_ls[n-1])
                        net.append(profit[-1]-fee[-1])
                n+=1
            return {'profit' : profit, 'fee': fee, 'net':net}


        df = pd.read_csv(f'{self.path_to_folder}/trade_hist_{botname}.csv', index_col=False)
        if df.empty :
            #make some shit here to make it appear on report
            return
        else:
            df['date'] = pd.to_datetime(df['date'])
            lists = get_profits_lists(df['side'].tolist(), df['value'].tolist(), df['fee'].tolist())
            df['profit'] = lists['profit']
            df['fee'] = lists['fee']
            df['net_profit'] = lists['net']
        params = self.get_bot_params(botname)
        ticker = params['ticker']
        crit_spread = params['crit_spread']
        time_span = datetime.datetime.now()-wallet['date'].iloc[0]
        profit = df['profit'].sum()
        fee = df['fee'].sum()
        net = df['net_profit'].sum()
        trade_count = len(df)/2
        avg = net/trade_count
        # print(df)

        return{
            'name':botname,
            'ticker':ticker,
            'crit_spread':crit_spread,
            'time_span':time_span,
            'trade_count':trade_count,
            'profit':profit,
            'fee':fee,
            'net_profit':net,
            'average_profit':avg
        }
        
        
    def run(self, archive=False, store=False):
        df = pd.DataFrame(
            columns=[
                'name',
                'ticker',
                'crit_spread',
                'time_span',
                'trade_count',
                'profit', 
                'fee',
                'net_profit',
                'average_profit'
            ]
        )
        ls = self.get_bots()['name'].tolist()
        #print(ls)
        n=0

        for botname in ls :
            try:
                stats = list(self.get_individual_stats(botname).values())
                df.loc[n] = stats
                n+=1
            except AttributeError:
                #print(f'no stats yet for {botname}')
                pass
        df['minutes'] = df['time_span'].dt.total_seconds()/60
        df['prof_per_m'] = df['net_profit']/df['minutes']
        df['projected profit 1W'] = df['prof_per_m']*10080
        df['projected profit 1M'] = df['prof_per_m']*43800
        df['projected profit 1Y'] = df['prof_per_m']*525600
        # df['projected profit 1M'] = 
        printdf = df.sort_values(by=['net_profit'], ascending=False)
        printdf = printdf[printdf['net_profit']>0]
        printdf = printdf[['time_span', 'ticker', 'crit_spread', 'trade_count', 'profit', 'fee', 'net_profit', 'average_profit', 'projected profit 1W', 'projected profit 1M', 'projected profit 1Y']]
        printdf = printdf.round(
                {
                'trade_count':1,
                'profit':2,
                'fee':2,
                'net_profit':2,
                'average_profit':2,
                'projected profit 1W':2, 
                'projected profit 1M':2, 
                'projected profit 1Y':2
                 })
        print(printdf)
        name = datetime.datetime.now().strftime("%Y-%m-%d_%Hh%M")
        if store :
            df.to_csv(f'{self.path_to_folder}{name}_results.csv')
        if archive:
            os.makedirs(f'archives/{name}')
            for file in os.listdir(self.path_to_folder):
                shutil.copy(f'{self.path_to_folder}{file}', f'archives/{name}')
                os.remove(f'{self.path_to_folder}{file}')
            print('done')
        return(df)
            


