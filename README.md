# vnpy_dydx
* dydx每月成交额10万刀以内免手续费
* 注意stark_private_key,stark_public_key浏览器获取到的都是0开头的要改成0x
* 进入https://trade.dydx.exchange/portfolio/overview，鼠标右键点击【检查，右上角>>找到【应用】，左侧【本地存储空间】/https://trade.dydx.exchange/找到【STARK_KEY_PAIRS】字典和【API_KEY_PAIRS】，需要用到STARK_KEY_PAIRS的privateKey(对应vn.py里面的stark_private_key)和API_KEY_PAIRS里面的key，secret，passphrase，walletAddress其他参数不用改
* 链接到dydx的钱包资产必须走erc20链
