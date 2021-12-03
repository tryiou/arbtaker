# arbtaker  
arbtaker trading bot for blocknet xbridge / ccxt supported exchanges

logic:  
bot will scan xbridge tokens & balance and possible orderbooks,  
then calculate CEX tokens path and orderbooks,  
compare to find profitable trades,  
execute on each platform if sufficient balances on each side.  

installation:  
`git clone https://github.com/tryiou/arbtaker.git`  
`cd arbtaker`  
open `arbtaker_settings.py` with text editor  
-Set correct rpc_user, rpc_password, rpc_port, corresponding to your blocknet core wallet.  
-dry_mode to True won't execute trade in real, just execute logic and console/logging.  
-dry_mode to False will execute trade in real.  
-Bot will gather new xbridge addresses at first run.  
-open `utils/keys.local.json` with text editor
-Set your active exchange name / api_key / api_secret  

start:  
python3 main.py  
