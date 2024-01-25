# pip install dydx-v3-python
# 支持python3.11，python3.12也可以运行dydx有几处需要修改
from dydx3 import Client
from dydx3.constants import MARKET_BTC_USD, ORDER_STATUS_OPEN
from dydx3 import private_key_to_public_key_pair_hex
from web3 import Web3
from vnpy.trader.setting import dydx_account
# app.infura.io申请的key
INFURA_API_KEY = "d6fa094c7f8b49099d8612eccebd7bc6"
WEB_PROVIDER_URL = f'https://mainnet.infura.io/v3/{INFURA_API_KEY}'
# 钱包地址(wallet_address)和eth私钥(eth_private_key)去钱包获取
client = Client(
    network_id=1,
    host="https://api.dydx.exchange",
    # 以太坊私钥
    eth_private_key=dydx_account["eth_private_key"],
    web3=Web3(Web3.HTTPProvider(WEB_PROVIDER_URL)),
)
# 获取api_key
api_key = client.onboarding.recover_default_api_key_credentials()
# 获取stark_key
stark_key = client.onboarding.derive_stark_key()
print(f"dydx参数：\nkey: {api_key['key']}\nsecret：{api_key['secret']}\npassphrase：{api_key['passphrase']}\nstark_private_key：{stark_key['private_key']}")