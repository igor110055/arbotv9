import pandas as pd

import time
# class Bao():
#     def __init__(self, load=False) -> None:
#         if load :
#             self.name = 'new'
#         else : self.name = 'false'

# b = Bao(load=True)
# print(b.name)

# num1 = 1564.25
# num2 = 5.56489874

# def count_decimals(num):
#     string = str(num)
#     dec = len(string.split(".")[1])
#     diff = '0.'
#     for n in range(dec):
#         if n == dec-1:
#             diff += "1"
#         else:
#             diff += "0"
#     print(diff)
#     print(float(diff))
#     return diff
# count_decimals(num1)
# count_decimals(num2)

# df = pd.read_csv('csv/bots.csv', index_col=False)
# print(df)
# print(len(df))
# tkls = df['ticker'].drop_duplicates().to_list()
# print(tkls)

# bao = 6

# if bao:
#     print('bao')

# d={'a':'b'}
# if bool(d):
#     print('yes')
# n = 1
# while n <= 1000:
#     print(n)
#     time.sleep(1)
#     if n%5 == 0:
#         break
#     n+=1


# df = pd.DataFrame({'cola':['a', 'c'], 'colb':['b', 'd']})
# print(df)
# print(len(df))
# df.loc[len(df)] = ['bao', 'kk']
# print(df)
# print(type(df.iloc[-1]))
# if isinstance(df.iloc[-1], pd.Series):
#     print('its a serie')

s = pd.Series({'s':3})
print(s)
df = pd.DataFrame({'a': [1,3], 'b':[2,4]})
df2 = df[df['b']>s['s']]
print(df2)

df = pd.DataFrame(columns=['b', 'c'])
print(df)
if df.empty:
    print('its empty')
print(len(df))
ls = [x for x in range(10) ]
print(ls)
print(len(ls))