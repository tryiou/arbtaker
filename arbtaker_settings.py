# RPC TO BLOCKNET CORE
rpc_user = "rpc_user"
rpc_password = "rpc_pass"
rpc_port = 41414

ccxt_exchange_name = "bittrex"
ccxt_exchange_hostname = "global.bittrex.com"  # SET TO None TO USE CCXT DEFAULT
# BOT LOGIC:
# BOT GATHER XB/CEX BALANCES AND SCAN ANY POSSIBLE XB ORDERBOOKS THEN SEARCH CEX EQUIVALENCE FOR PROFITABLE TRADES,
# EXECUTE TRADES ON EACH PLATFORM IF ENOUGH BALANCES OF EACH INVOLVED COINS IS SUFFICIENT

dry_mode = True  # TRUE WONT EXECUTE TRADE FOR REAL, JUST CONSOLE/LOG, FALSE FOR LIVE ACTION!
debug = 0  # DEBUGLEVEL
dex_coins_disabled = ['RVN']  # THOSE COIN WILL BE EXCLUDED FROM TRADING
min_profit = 1.075  # 1=100% # MINIMUM % PROFIT TO TRIGGER
time_per_loop = 20 # SECONDS SLEEPING BETWEEN EACH BOT LOOP
error_rate_mod = 0.0001

# MAX SIZE IN COIN ASSET PER ORDER
max_size = {}
max_size['BLOCK'] = 20
max_size['BTC'] = 0.01
max_size['LTC'] = 5

# MIN SIZE in COIN ASSET PER ORDER, BTC VALUE APPLY TO ALL, AS CEX HAVE MINSIZE PER ORDER TOO
min_size = {}
min_size['BTC'] = 0.0005  # NEEDED, CEX MINSIZE

# BOT WILL REQUEST NEW ADDRESS IF MISSING AND ADD HERE, DONT CHANGE SYNTAX FROM HERE
dx_addresses = {}
