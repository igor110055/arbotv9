from datetime import datetime
from bot2 import Botmaker, constructor, Main_prog, Bot
import time
import numpy as np

tick = ["XBT/USDT", "ETH/USDT", "LTC/USDT", 'DOT/USDT', 'LINK/USDT', 'BCH/USDT', 'EOS/USDT', 'ADA/USDT']
spread = [round(x,2) for x in np.arange(0.01, 2.02, 0.03)]

c = constructor()
# b = Botmaker(tick, spread, 20000)
# bl = b.generate_bot_list()
# m = Main_prog(bl)
m = Main_prog(None, "csv/bots.csv")
m.initialise()
# bot = m.botlist[0]
bnuber = len(m.botlist)
# print(f'{bnuber} bots loaded')
m.run_bots()
