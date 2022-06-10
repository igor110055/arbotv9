from importlib.resources import path
import pandas as pd
import os
from datetime import datetime
import shutil


class Reporter():
    '''A class to make reporting on the data gathered'''

    def get_bot_result(self, bot, path_to_folder = 'csv/', df = False):
        #make sure the bot is str
        if not df:
            if not isinstance(bot, str):
                bot=str(bot)
            df = pd.read_csv(f'{path_to_folder}trade_hist_{bot}.csv', index_col=False)
            wallet = pd.read_csv(f'{path_to_folder}wallet_{bot}.csv', index_col=False)
        #Reformat in case of bad layout
        df = df[['date', 'exchange', 'side', 'price', 'qtt', 'value', 'fee']]
        df["date"] = pd.to_datetime(df["date"])
        df.astype({'price':float, 'fee':float, 'qtt':float, 'value':float})

        #calculate values and add the columns
        time_span=df['date'].iloc[-1] - wallet['date'].iloc[0]    
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
                lsa.append(round(prof, 2))
            n += 1
        df["profit"] = lsp
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
        df = df[['date','exchange', 'side', 'price', 'qtt', 'value', 'fee', 'profit', 'adjusted']]
        return {
            "df": df,
            "profit": profit,
            "fees": fees,
            "net": net,
            "time_span": time_span,
            "total_trades": trade_amount
            }

    def get_session_results(self, path_to_folder = 'csv/', store=False, archive=False):
        '''cycles the folder and if a bot gets a results, gathers the data in a df
        if store set to true, will create a .csv in that same folder, 
        if archive = True, will clear the specififed folder and move to archives/'''
        ls = os.listdir(path_to_folder)
        # print(ls)
        result_df = pd.DataFrame(columns=['name','ticker', 'time_span', 'trade_amount', 'profit', 'fees', 'net_profit'])
        n=0
        for item in ls :
            if 'trade_hist_' in item:
                name = item.replace('trade_hist_', '')
                name = name.replace('.csv', '')
                print(name)
                if len(pd.read_csv(f'{path_to_folder}{item}'))>1:
                    bot_result = self.get_bot_result(name, path_to_folder)
                    result_df.loc[n] = [name, bot_result['time_span'], bot_result['total_trades'], bot_result['profit'], bot_result['fees'], bot_result['net']]
                    n+=1
                else :
                    print(f'no data for {name}')
        result_df['avg_profit'] = result_df['net_profit']/result_df['trade_amount']
        print(result_df)
        name = datetime.now().strftime("%Y-%m-%d_%Hh%M")
        if store :
            name = datetime.now().strftime("%Y-%m-%d_%Hh%M")
            result_df.to_csv(f'{path_to_folder}{name}_report.csv', index=False)
        if archive:
            os.makedirs(f'archives/{name}')
            for file in os.listdir(path_to_folder):
                shutil.copy(f'{path_to_folder}{file}', f'archives/{name}')
                os.remove(f'{path_to_folder}{file}')
            print('done')

r=Reporter()

bot = 'ltcbot'


r.get_session_results(store=True, archive=True)
