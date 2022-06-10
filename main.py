from cstr import Matrix
from bot import Bot


btcbot = Bot("XBT/USDT", "btcbot", 0.1, {"crypto": 0.35, "fiat": 10000})
ethbot = Bot("ETH/USDT", "ethbot", 0.1, {"crypto": 2, "fiat": 10000}, 0.01)
ltcbot = Bot("LTC/USDT", "ltcbot", 0.1, {"crypto": 145, "fiat": 10000}, 0.01)
bchbot = Bot("BCH/USDT", "bchbot", 0.1, {"crypto": 50, "fiat": 10000}, 0.01)
adabot = Bot("ADA/USDT", "adabot", 0.1, {"crypto": 16700, "fiat": 10000}, 0.00001)
eosbot = Bot("EOS/USDT", "eosbot", 0.1, {"crypto": 7300, "fiat": 10000}, 0.0001)
linkbot = Bot("LINK/USDT", "linkbot", 0.1, {"crypto": 1300, "fiat": 10000}, 0.00001)
dotbot = Bot("DOT/USDT", "dotbot", 0.1, {"crypto": 100, "fiat": 10000}, 0.001)

botlist = [btcbot, ethbot, ltcbot, bchbot, adabot, eosbot, linkbot, dotbot]

m = Matrix(botlist)

m.run_main()
