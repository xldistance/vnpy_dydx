import hashlib
import hmac
from time import time
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Sequence
from decimal import Decimal
import pytz
import base64
import json
from peewee import chunked
from requests.exceptions import SSLError

from vnpy.trader.database import database_manager
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval,
    Offset
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    PositionData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.event import Event, EventEngine

from vnpy.api.rest import Request, RestClient, Response
from vnpy.api.websocket import WebsocketClient
from.dydx_tool import order_to_sign, generate_hash_number
from vnpy.trader.setting import dydx_account
from vnpy.trader.utility import(TZ_INFO,get_local_datetime,GetFilePath,extract_vt_symbol)
# 实盘REST API地址
REST_HOST: str = "https://api.dydx.exchange"

# 实盘Websocket API地址
WEBSOCKET_HOST: str = "wss://api.dydx.exchange/v3/ws"

# 模拟盘REST API地址
TESTNET_REST_HOST: str = "https://api.stage.dydx.exchange"

# 模拟盘Websocket API地址
TESTNET_WEBSOCKET_HOST: str = "wss://api.stage.dydx.exchange/v3/ws"


# 委托状态映射
STATUS_DYDX2VT: Dict[str, Status] = {
    "PENDING": Status.NOTTRADED,
    "OPEN": Status.NOTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
    "UNTRIGGERED":Status.NOTTRADED,
}

# 委托类型映射
ORDERTYPE_VT2DYDX: Dict[OrderType, str] = {
    OrderType.LIMIT: "LIMIT",
    OrderType.MARKET: "MARKET"
}
ORDERTYPE_DYDX2VT: Dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2DYDX.items()}

# 买卖方向映射
DIRECTION_VT2DYDX: Dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_DYDX2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2DYDX.items()}

# 数据频率映射
INTERVAL_VT2DYDX: Dict[Interval, str] = {
    Interval.MINUTE: "1MIN",
    Interval.HOUR: "1HOUR",
    Interval.DAILY: "1DAY",
}

# 时间间隔映射
TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}


class Security(Enum):
    """鉴权类型"""
    PUBLIC: int = 0
    PRIVATE: int = 1


# 账户信息全局缓存字典
api_key_credentials_map: Dict[str, str] = {}

#------------------------------------------------------------------------------------------------- 
class DydxGateway(BaseGateway):
    """
    * vn.py用于对接dYdX交易所的交易接口
    * 进入https://trade.dydx.exchange/portfolio/overview，鼠标右键点击【检查，右上角>>找到【应用】，左侧【本地存储空间】/https://trade.dydx.exchange/找到【STARK_KEY_PAIRS】字典和【API_KEY_PAIRS】，需要用到STARK_KEY_PAIRS的privateKey(对应vn.py里面的stark_private_key)和API_KEY_PAIRS里面的key，secret，passphrase，其他参数不用改
    * 链接到dydx的钱包资产必须走erc20链
    """
    default_setting: Dict[str, Any] = {
        "key": "",
        "secret": "",
        "passphrase": "",
        "stark_private_key": "",
        "服务器": ["REAL", "TESTNET"],
        "代理地址": "",
        "代理端口": 0,
        "limitFee": 0.0,
        "accountNumber": "0"
    }

    exchanges: Exchange = [Exchange.DYDX]
    #所有合约列表
    recording_list = GetFilePath.recording_list
    #------------------------------------------------------------------------------------------------- 
    def __init__(self, event_engine: EventEngine, gateway_name: str = "DYDX") -> None:
        """
        构造函数
        """
        super().__init__(event_engine, gateway_name)

        self.rest_api: "DydxRestApi" = DydxRestApi(self)
        self.ws_api: "DydxWebsocketApi" = DydxWebsocketApi(self)

        self.pos_id: str = ""
        self.id: str = ""
        self.sys_local_map: Dict[str, str] = {}
        self.local_sys_map: Dict[str, str] = {}

        self.orders: Dict[str, OrderData] = {}
        self.recording_list = [vt_symbol for vt_symbol in self.recording_list if extract_vt_symbol(vt_symbol)[2] == self.gateway_name and not extract_vt_symbol(vt_symbol)[0].endswith("99")]
        #历史数据合约列表
        self.history_contracts = copy(self.recording_list)
        #rest查询合约列表
        self.query_contracts = [vt_symbol for vt_symbol in GetFilePath.all_trading_vt_symbols if extract_vt_symbol(vt_symbol)[2] == self.gateway_name and not extract_vt_symbol(vt_symbol)[0].endswith("99")]
    #------------------------------------------------------------------------------------------------- 
    def connect(self, log_account: dict) -> None:
        """
        连接交易接口
        """
        if not log_account:
            log_account = dydx_account
        api_key_credentials_map["key"] = log_account["key"]
        api_key_credentials_map["secret"] = log_account["secret"]
        api_key_credentials_map["passphrase"] = log_account["passphrase"]
        api_key_credentials_map["stark_private_key"] = log_account["stark_private_key"]
        server: str = log_account["服务器"]
        proxy_host: str = log_account["代理地址"]
        proxy_port: int = log_account["代理端口"]
        limitFee: float = log_account["limitFee"]
        accountNumber: str = log_account["accountNumber"]

        self.rest_api.connect(server, proxy_host, proxy_port, limitFee,self.gateway_name)
        self.ws_api.connect(proxy_host, proxy_port, server, accountNumber,self.gateway_name)
    #------------------------------------------------------------------------------------------------- 
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        self.ws_api.subscribe(req)
    #------------------------------------------------------------------------------------------------- 
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单
        """
        return self.rest_api.send_order(req)
    #------------------------------------------------------------------------------------------------- 
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        """
        self.rest_api.cancel_order(req)
    #------------------------------------------------------------------------------------------------- 
    def query_account(self) -> None:
        """
        查询资金
        """
        self.rest_api.query_account()
    #------------------------------------------------------------------------------------------------- 
    def query_position(self) -> None:
        pass
    #------------------------------------------------------------------------------------------------- 
    def query_active_orders(self) -> None:
        """
        查询活动委托单
        """
        self.rest_api.query_active_orders()
    #-------------------------------------------------------------------------------------------------   
    def query_history(self,event):
        """
        查询合约历史数据
        """
        if self.history_contracts:
            symbol,exchange,gateway_name = extract_vt_symbol(self.history_contracts.pop(0))
            req = HistoryRequest(
                symbol = symbol,
                exchange = Exchange(exchange),
                interval = Interval.MINUTE,
                start = datetime.now(TZ_INFO) - timedelta(days = 1),
                gateway_name = self.gateway_name
            )
            self.rest_api.query_history(req)
    #-------------------------------------------------------------------------------------------------   
    def process_timer_event(self,event):
        """
        处理定时任务
        """
        self.query_account()
        if self.query_contracts:
            vt_symbol = self.query_contracts.pop(0)
            symbol,exchange,gateway_name = extract_vt_symbol(vt_symbol)
            self.query_active_orders(symbol)
            self.query_contracts.append(vt_symbol)
    #------------------------------------------------------------------------------------------------- 
    def close(self) -> None:
        """
        关闭连接
        """
        self.rest_api.stop()
        self.ws_api.stop()
    #------------------------------------------------------------------------------------------------- 
    def on_order(self, order: OrderData) -> None:
        """
        推送委托数据
        """
        self.orders[order.orderid] = copy(order)
        super().on_order(order)
    #------------------------------------------------------------------------------------------------- 
    def get_order(self, orderid: str) -> OrderData:
        """
        查询委托数据
        """
        return self.orders.get(orderid, None)
    #------------------------------------------------------------------------------------------------- 
    def init_query(self) -> None:
        """
        初始化查询任务
        """
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_TIMER, self.query_history)
#------------------------------------------------------------------------------------------------- 
class DydxRestApi(RestClient):
    """
    dYdX的REST API
    """

    def __init__(self, gateway: DydxGateway) -> None:
        """
        构造函数
        """
        super().__init__()

        self.gateway: DydxGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.order_count: int = 0
#------------------------------------------------------------------------------------------------- 
    def sign(self, request: Request) -> Request:
        """
        生成dYdX签名
        """
        security: Security = request.data["security"]
        now_iso_string = generate_now_iso()
        if security == Security.PUBLIC:
            request.data = None
            return request
        else:
            request.data.pop("security")
            signature: str = sign(
                request_path=request.path,
                method=request.method,
                iso_timestamp=now_iso_string,
                data=request.data
            )
            request.data = json.dumps(request.data)

        headers = {
            "DYDX-SIGNATURE": signature,
            "DYDX-API-KEY": api_key_credentials_map["key"],
            "DYDX-TIMESTAMP": now_iso_string,
            "DYDX-PASSPHRASE": api_key_credentials_map["passphrase"],
            "Accept": 'application/json',
            "Content-Type": 'application/json'
        }
        request.headers = headers

        return request
#------------------------------------------------------------------------------------------------- 
    def connect(
        self,
        server: str,
        proxy_host: str,
        proxy_port: int,
        limitFee: float,
        gateway_name:str
    ) -> None:
        """
        连接REST服务器
        """
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server
        self.limitFee = limitFee
        self.gateway_name = gateway_name
        if self.server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port,gateway_name= self.gateway_name)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port,gateway_name= self.gateway_name)

        self.start()
        self.query_contract()

        self.gateway.write_log(f"交易接口：{self.gateway_name}，REST API启动成功")
    #------------------------------------------------------------------------------------------------- 
    def query_contract(self) -> None:
        """
        查询合约信息
        """
        data: dict = {
            "security": Security.PUBLIC
        }

        self.add_request(
            method="GET",
            path="/v3/markets",
            callback=self.on_query_contract,
            data=data
        )
    #------------------------------------------------------------------------------------------------- 
    def query_account(self) -> None:
        """
        查询资金
        """
        data: dict = {
            "security": Security.PRIVATE
        }

        self.add_request(
            method="GET",
            path=f"/v3/accounts/{self.gateway.id}",
            callback=self.on_query_account,
            data=data
        )
    #------------------------------------------------------------------------------------------------- 
    def query_active_orders(self,symbol:str) -> None:
        data: dict = {
            "security": Security.PRIVATE
        }
        params = {
            "market":symbol,
            "status":["PENDING","OPEN","UNTRIGGERED"]
        }
        self.add_request(
            method="GET",
            path="/v3/orders",
            callback=self.on_active_orders,
            data=data,
            params=params
        )
    #------------------------------------------------------------------------------------------------- 
    def on_active_orders(self,data: dict, request: Request) -> None:
        """
        收到活动委托单回报
        """
        for order_data in data:
            order: OrderData = OrderData(
                symbol=order_data["market"],
                exchange=Exchange.DYDX,
                orderid=order_data["clientId"],
                type=ORDERTYPE_DYDX2VT[order_data["type"]],
                direction=DIRECTION_DYDX2VT[order_data["side"]],
                price=float(order_data["price"]),
                volume=float(order_data["size"]),
                traded=float(order_data["size"]) - float(order_data["remainingSize"]),
                status=STATUS_DYDX2VT.get(order_data["status"]),
                datetime=get_local_datetime(order_data["createdAt"]),
                gateway_name=self.gateway_name
            )
            if 0 < order.traded < order.volume:
                order.status = Status.PARTTRADED
            self.gateway.on_order(order)
    #------------------------------------------------------------------------------------------------- 
    def new_orderid(self) -> str:
        """
        生成本地委托号
        """
        prefix: str = datetime.now().strftime("%Y%m%d%H%M%S")

        self.order_count += 1
        suffix: str = str(self.order_count).rjust(8, "0")

        orderid: str = prefix + suffix
        return orderid
    #------------------------------------------------------------------------------------------------- 
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单
        """
        # 生成本地委托号
        orderid: str = req.symbol + "-" + self.new_orderid()

        # 推送提交中事件
        order: OrderData = req.create_order_data(
            orderid,
            self.gateway_name
        )
        self.gateway.on_order(order)

        expiration_epoch_seconds: int = int(time() + 86400)

        hash_number: int = generate_hash_number(
            server=self.server,
            position_id=self.gateway.pos_id,
            client_id=orderid,
            market=req.symbol,
            side=DIRECTION_VT2DYDX[req.direction],
            human_size=str(req.volume),
            human_price=str(req.price),
            limit_fee=str(self.limitFee),
            expiration_epoch_seconds=expiration_epoch_seconds
        )

        signature: str = order_to_sign(hash_number, api_key_credentials_map["stark_private_key"])

        # 生成委托请求
        data: dict = {
            "security": Security.PRIVATE,
            "market": req.symbol,
            "side": DIRECTION_VT2DYDX[req.direction],
            "type": ORDERTYPE_VT2DYDX[req.type],
            "timeInForce": "GTT",
            "size": str(req.volume),
            "price": str(req.price),
            "limitFee": str(self.limitFee),
            "expiration": epoch_seconds_to_iso(expiration_epoch_seconds),
            "postOnly": False,
            "clientId": orderid,
            "signature": signature
        }
        # reduceOnly不支持GTT
        #if order.offset == Offset.CLOSE:
            #data["reduceOnly"] = True
        self.add_request(
            method="POST",
            path="/v3/orders",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_orderid
    #------------------------------------------------------------------------------------------------- 
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        """
        order_no: str = self.gateway.local_sys_map.get(req.orderid, "")
        if not order_no:
            self.gateway.write_log(f"撤单失败，找不到{req.orderid}对应的系统委托号")
            return

        data: dict = {
            "security": Security.PRIVATE
        }

        order: OrderData = self.gateway.get_order(req.orderid)

        self.add_request(
            method="DELETE",
            path=f"/v3/orders/{order_no}",
            callback=self.on_cancel_order,
            data=data,
            on_failed=self.on_cancel_failed,
            extra=order
        )
    #------------------------------------------------------------------------------------------------- 
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        查询历史数据
        """
        history: List[BarData] = []
        data: dict = {
            "security": Security.PUBLIC
        }

        params: dict = {
            "resolution": INTERVAL_VT2DYDX[req.interval],
            "limit":100,
        }

        resp: Response = self.request(
            method="GET",
            path=f"/v3/candles/{req.symbol}",
            data=data,
            params=params
        )
        time_consuming_start = time()
        if resp.status_code // 100 != 2:
            msg: str = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
            self.gateway.write_log(msg)
        else:
            data: dict = resp.json()
            if not data:
                self.gateway.write_log("获取历史数据为空")

            for data in data["candles"]:

                bar: BarData = BarData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    datetime=get_local_datetime(data["startedAt"]),
                    interval=req.interval,
                    volume=float(data["baseTokenVolume"]),
                    open_price=float(data["open"]),
                    high_price=float(data["high"]),
                    low_price=float(data["low"]),
                    close_price=float(data["close"]),
                    open_interest=float(data["startingOpenInterest"]),
                    gateway_name=self.gateway_name
                )
                history.append(bar)
        if not history:
            msg = f"未获取到合约：{req.vt_symbol}历史数据"
            self.gateway.write_log(msg)
            return
        for bar_data in chunked(history, 10000):               #分批保存数据
            try:
                database_manager.save_bar_data(bar_data,False)      #保存数据到数据库  
            except Exception as err:
                self.gateway.write_log(f"{err}")
                return
        time_consuming_end =time()
        query_time = round(time_consuming_end - time_consuming_start,3)
        msg = f"载入{req.vt_symbol}:bar数据，开始时间：{history[-1].datetime} ，结束时间： {history[0].datetime}，数据量：{len(history)}，耗时:{query_time}秒"
        self.gateway.write_log(msg)
        return history
    #------------------------------------------------------------------------------------------------- 
    def on_query_contract(self, data: dict, request: Request) -> None:
        """
        合约信息查询回报
        """
        for raw_data in data["markets"]:
            contract: ContractData = ContractData(
                symbol=raw_data,
                exchange=Exchange.DYDX,
                name=raw_data,
                price_tick=data["markets"][raw_data]["tickSize"],
                size=data["markets"][raw_data]["stepSize"],
                min_volume=data["markets"][raw_data]["minOrderSize"],
                max_volume= data["markets"][raw_data]["maxPositionSize"],
                product=Product.FUTURES,
                gateway_name=self.gateway_name
            )
            self.gateway.on_contract(contract)

        self.gateway.write_log(f"交易接口：{self.gateway_name}，合约信息查询成功")
    #------------------------------------------------------------------------------------------------- 
    def on_query_account(self, data: dict, request: Request) -> None:
        """
        资金查询回报
        """
        data: dict = data["account"]
        balance: float = float(data["equity"])
        available: float = float(data["freeCollateral"])
        account: AccountData = AccountData(
            accountid=f"USDC_{self.gateway_name}",
            balance=balance,
            available = available,
            frozen=balance - available,
            gateway_name=self.gateway_name
        )

        self.gateway.on_account(account)

        for keys in data["openPositions"]:
            if data["openPositions"][keys]["size"] == "SHORT":
                direction = Direction.SHORT
                position.volume = -position.volume
            elif data["openPositions"][keys]["size"] == "LONG":
                direction = Direction.LONG
            position: PositionData = PositionData(
                symbol=keys,
                exchange=Exchange.DYDX,
                direction=direction,
                volume=float(data["openPositions"][keys]["size"]),
                price=float(data["openPositions"][keys]["entryPrice"]),
                pnl=float(data["openPositions"][keys]["unrealizedPnl"]),
                gateway_name=self.gateway_name
            )
            self.gateway.on_position(position)
    #------------------------------------------------------------------------------------------------- 
    def on_send_order(self, data: dict, request: Request) -> None:
        """
        委托下单回报
        """
        pass
    #------------------------------------------------------------------------------------------------- 
    def on_send_order_error(
        self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """
        委托下单回报函数报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if not issubclass(exception_type, (ConnectionError, SSLError)):
            self.on_error(exception_type, exception_value, tb, request)
    #------------------------------------------------------------------------------------------------- 
    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """
        委托下单失败服务器报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg: str = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    #------------------------------------------------------------------------------------------------- 
    def on_cancel_order(self, data: dict, request: Request) -> None:
        """
        委托撤单回报
        """
        pass
    #------------------------------------------------------------------------------------------------- 
    def on_cancel_failed(self, status_code: str, request: Request) -> None:
        """
        撤单回报函数报错回报
        """
        if request.extra:
            order: OrderData = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)

        msg: str = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
#------------------------------------------------------------------------------------------------- 
class DydxWebsocketApi(WebsocketClient):
    """
    dYdX的Websocket API
    """

    def __init__(self, gateway: DydxGateway) -> None:
        """
        构造函数
        """
        super().__init__()

        self.gateway: DydxGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.subscribed: Dict[str, SubscribeRequest] = {}
        self.order_books: Dict[str, "OrderBook"] = {}
    #------------------------------------------------------------------------------------------------- 
    def connect(
        self,
        proxy_host: str,
        proxy_port: int,
        server: str,
        accountNumber: str,
        gateway_name:str
    ) -> None:
        """
        连接Websocket行情频道
        """
        self.accountNumber = accountNumber
        self.gateway_name = gateway_name
        if server == "REAL":
            self.init(WEBSOCKET_HOST, proxy_host, proxy_port,gateway_name= self.gateway_name)
        else:
            self.init(TESTNET_WEBSOCKET_HOST, proxy_host, proxy_port,gateway_name= self.gateway_name)
        self.start()
    #------------------------------------------------------------------------------------------------- 
    def on_connected(self) -> None:
        """
        连接成功回报
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket API连接成功")
        self.subscribe_topic()

        for req in list(self.subscribed.values()):
            self.subscribe(req)
    #------------------------------------------------------------------------------------------------- 
    def on_disconnected(self) -> None:
        """
        连接断开回报
        """
        self.gateway.write_log("Websocket API连接断开")
    #------------------------------------------------------------------------------------------------- 
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """

        # 缓存订阅记录
        self.subscribed[req.vt_symbol] = req
        symbol = req.symbol

        orderbook = OrderBook(symbol, req.exchange, self.gateway)
        self.order_books[symbol] = orderbook

        req: dict = {
            "type": "subscribe",
            "channel": "v3_orderbook",
            "id": symbol
        }
        self.send_packet(req)

        req: dict = {
            "type": "subscribe",
            "channel": "v3_trades",
            "id": symbol
        }
        self.send_packet(req)
    #------------------------------------------------------------------------------------------------- 
    def subscribe_topic(self) -> None:
        """
        订阅委托、资金和持仓推送
        """
        now_iso_string = generate_now_iso()
        signature: str = sign(
            request_path="/ws/accounts",
            method="GET",
            iso_timestamp=now_iso_string,
            data={},
        )
        req: dict = {
            "type": "subscribe",
            "channel": "v3_accounts",
            "accountNumber": self.accountNumber,
            "apiKey": api_key_credentials_map["key"],
            "signature": signature,
            "timestamp": now_iso_string,
            "passphrase": api_key_credentials_map["passphrase"]
        }
        self.send_packet(req)
    #------------------------------------------------------------------------------------------------- 
    def on_packet(self, packet: dict) -> None:
        """
        推送数据回报
        """
        type = packet.get("type", None)
        if type == "error":
            msg: str = packet["message"]
            # 过滤重复订阅错误
            if "already subscribed" not in msg:
                self.gateway.write_log(f"交易接口websocket收到错误回报：{msg}")
            return

        channel: str = packet.get("channel", None)

        if channel:
            if packet["channel"] == "v3_orderbook" or packet["channel"] == "v3_trades":
                self.on_orderbook(packet)
            elif packet["channel"] == "v3_accounts":
                self.on_message(packet)
    #------------------------------------------------------------------------------------------------- 
    def on_orderbook(self, packet: dict) -> None:
        """
        订单簿更新推送
        """
        orderbook = self.order_books[packet["id"]]
        orderbook.on_message(packet)
    #------------------------------------------------------------------------------------------------- 
    def on_message(self, packet: dict) -> None:
        """
        Websocket账户更新推送
        """
        for order_data in packet["contents"]["orders"]:
            # 绑定本地和系统委托号映射
            self.gateway.local_sys_map[order_data["clientId"]] = order_data["id"]
            self.gateway.sys_local_map[order_data["id"]] = order_data["clientId"]
            order: OrderData = OrderData(
                symbol=order_data["market"],
                exchange=Exchange.DYDX,
                orderid=order_data["clientId"],
                type=ORDERTYPE_DYDX2VT[order_data["type"]],
                direction=DIRECTION_DYDX2VT[order_data["side"]],
                offset=Offset.NONE,
                price=float(order_data["price"]),
                volume=float(order_data["size"]),
                traded=float(order_data["size"]) - float(order_data["remainingSize"]),
                status=STATUS_DYDX2VT.get(order_data["status"], Status.SUBMITTING),
                datetime=get_local_datetime(order_data["createdAt"]),
                gateway_name=self.gateway_name
            )
            if 0 < order.traded < order.volume:
                order.status = Status.PARTTRADED
            self.gateway.on_order(order)

        if packet["type"] == "subscribed":
            self.gateway.pos_id = packet["contents"]["account"]["positionId"]
            self.gateway.id = packet["id"]
            self.gateway.init_query()
            self.gateway.write_log(f"交易接口：{self.gateway_name}，账户资金查询成功")
        else:
            fills = packet["contents"].get("fills", None)
            if not fills:
                return
            for fill_data in packet["contents"]["fills"]:
                orderid: str = self.gateway.sys_local_map[fill_data["orderId"]]

                trade: TradeData = TradeData(
                    symbol=fill_data["market"],
                    exchange=Exchange.DYDX,
                    orderid=orderid,
                    tradeid=fill_data["id"],
                    direction=DIRECTION_DYDX2VT[fill_data["side"]],
                    price=float(fill_data["price"]),
                    volume=float(fill_data["size"]),
                    datetime=get_local_datetime(fill_data["createdAt"]),
                    gateway_name=self.gateway_name
                )
                self.gateway.on_trade(trade)
#------------------------------------------------------------------------------------------------- 
class OrderBook():
    """
    储存dYdX订单簿数据
    """
    def __init__(self, symbol: str, exchange: Exchange, gateway: BaseGateway) -> None:
        """
        构造函数
        """

        self.asks: Dict[Decimal, Decimal] = dict()
        self.bids: Dict[Decimal, Decimal] = dict()
        self.gateway: DydxGateway = gateway

        # 创建TICK对象
        self.tick: TickData = TickData(
            symbol=symbol,
            exchange=exchange,
            name=symbol,
            datetime=datetime.now(TZ_INFO),
            gateway_name=gateway.gateway_name,
        )
    #------------------------------------------------------------------------------------------------- 
    def on_message(self, data: dict) -> None:
        """
        Websocket订单簿更新推送
        """
        type_: str = data["type"]
        channel: str = data["channel"]
        dt: datetime = datetime.now(TZ_INFO)
        if type_ == "subscribed" and channel == "v3_orderbook":
            self.on_snapshot(data["contents"]["asks"][:5], data["contents"]["bids"][:5], dt)
        elif type_ == "channel_data" and channel == "v3_orderbook":
            self.on_update(data["contents"], dt)
        elif channel == "v3_trades":
            self.on_trades(data["contents"]["trades"], dt)
    #------------------------------------------------------------------------------------------------- 
    def on_trades(self, data: list, dt) -> None:
        """
        成交更新推送
        """
        tick: TickData = self.tick
        tick.last_price = float(data[0]["price"])
        tick.datetime = get_local_datetime(data[0]["createdAt"])
    #------------------------------------------------------------------------------------------------- 
    def on_update(self, data: dict, dt:datetime) -> None:
        """
        盘口更新推送
        """
        for price, ask_volume in data["asks"]:
            price: float = float(price)
            ask_volume: float = float(ask_volume)
            if ask_volume:
                self.asks[price] = ask_volume
            else:
                if price in self.asks:
                    del self.asks[price]

        for price, bid_volume in data["bids"]:
            price: float = float(price)
            bid_volume: float = float(bid_volume)
            if bid_volume:
                self.bids[price] = bid_volume
            else:
                if price in self.bids:
                    del self.bids[price]
        self.generate_tick(dt)
    #------------------------------------------------------------------------------------------------- 
    def on_snapshot(self, asks: Sequence[List], bids: Sequence[List], dt: datetime) -> None:
        """
        盘口推送回报
        """
        self.bids.clear()
        self.asks.clear()
        for n in range(len(asks)):
            price = asks[n]["price"]
            volume = asks[n]["size"]
            self.asks[float(price)] = float(volume)

        for n in range(len(bids)):
            price = bids[n]["price"]
            volume = bids[n]["size"]
            self.bids[float(price)] = float(volume)

        self.generate_tick(dt)
    #------------------------------------------------------------------------------------------------- 
    def generate_tick(self, dt: datetime) -> None:
        """
        合成tick
        """
        tick: TickData = self.tick
        # bids和asks删除错误价格
        if not tick.last_price:
            return
        for price,volume in list(self.bids.items()):
            if tick.last_price < price:
                self.bids.pop(price)
        for price,volume in list(self.asks.items()):
            if tick.last_price > price:
                self.asks.pop(price)

        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:5]
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0], reverse=False)[:5]
        # 重置bids,asks防止字典长度一直递增
        self.bids = {}
        self.asks = {}
        for index,value in enumerate(sorted_bids):
            setattr(tick, f"bid_price_{index + 1}", value[0])
            setattr(tick, f"bid_volume_{index + 1}", value[1])
            self.bids[value[0]] = value[1]

        for index,value in enumerate(sorted_asks):
            setattr(tick, f"ask_price_{index + 1}", value[0])
            setattr(tick, f"ask_volume_{index + 1}", value[1])
            self.asks[value[0]] = value[1]

        tick.datetime = dt
        self.gateway.on_tick(copy(tick))
#------------------------------------------------------------------------------------------------- 
def generate_now_iso() -> str:
    """生成ISO时间"""
    return datetime.utcnow().strftime(
        '%Y-%m-%dT%H:%M:%S.%f',
    )[:-3] + 'Z'
#------------------------------------------------------------------------------------------------- 
def epoch_seconds_to_iso(epoch: float) -> str:
    """时间格式转换"""
    return datetime.utcfromtimestamp(epoch).strftime(
        '%Y-%m-%dT%H:%M:%S.%f',
    )[:-3] + 'Z'
#------------------------------------------------------------------------------------------------- 
def sign(
    request_path: str,
    method: str,
    iso_timestamp: str,
    data: dict,
) -> str:
    """
    生成签名
    """
    body: str = ""
    if data:
        body = json.dumps(data, separators=(',', ':'))

    message_string = "".join([
        iso_timestamp,
        method,
        request_path,
        body
    ])

    hashed = hmac.new(
        base64.urlsafe_b64decode(
            (api_key_credentials_map["secret"]).encode('utf-8'),
        ),
        msg=message_string.encode('utf-8'),
        digestmod=hashlib.sha256,
    )
    return base64.urlsafe_b64encode(hashed.digest()).decode()
