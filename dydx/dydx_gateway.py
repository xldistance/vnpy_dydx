import base64
import csv
import hashlib
import hmac
import json
from copy import copy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from threading import Lock
from time import time
from typing import Any, Dict, List, Sequence
from urllib.parse import urlencode

from peewee import chunked
from requests.exceptions import SSLError
from vnpy.api.rest import Request, Response, RestClient
from vnpy.api.websocket import WebsocketClient
from vnpy.event import Event, EventEngine
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Interval,
    Offset,
    OrderType,
    Product,
    Status,
)
from vnpy.trader.database import database_manager
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    AccountData,
    BarData,
    CancelRequest,
    ContractData,
    HistoryRequest,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy.trader.setting import dydx_account
from vnpy.trader.utility import (
    TZ_INFO,
    GetFilePath,
    extract_vt_symbol,
    get_local_datetime,
    is_target_contract,
    str_to_number
)

from .dydx_tool import generate_hash_number, order_to_sign

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
    "UNTRIGGERED": Status.NOTTRADED,
}

# 委托类型映射
ORDERTYPE_VT2DYDX: Dict[OrderType, str] = {OrderType.LIMIT: "LIMIT", OrderType.MARKET: "MARKET"}
ORDERTYPE_DYDX2VT: Dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2DYDX.items()}

# 买卖方向映射
DIRECTION_VT2DYDX: Dict[Direction, str] = {Direction.LONG: "BUY", Direction.SHORT: "SELL"}
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


# ----------------------------------------------------------------------------------------------------
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
        "server": ["REAL", "TESTNET"],
        "host": "",
        "port": 0,
        "limitFee": 0.0,
        "accountNumber": "0",
    }

    exchanges: Exchange = [Exchange.DYDX]
    # 所有合约列表
    recording_list = GetFilePath.recording_list
    # ----------------------------------------------------------------------------------------------------
    def __init__(self, event_engine: EventEngine, gateway_name: str = "DYDX") -> None:
        """
        构造函数
        """
        super().__init__(event_engine, gateway_name)

        self.rest_api: "DydxRestApi" = DydxRestApi(self)
        self.ws_api: "DydxWebsocketApi" = DydxWebsocketApi(self)
        self.account_file_name: str = ""  # 记录账户数据文件名
        self.pos_id: str = ""  # pos_id用于生成交易哈希值
        self.id: str = ""
        self.sys_local_map: Dict[str, str] = {}
        self.local_sys_map: Dict[str, str] = {}
        self.orders: Dict[str, OrderData] = {}
        # 下载历史数据状态
        self.history_status: bool = True
        self.recording_list = [vt_symbol for vt_symbol in self.recording_list if is_target_contract(vt_symbol, self.gateway_name)]
        # 历史数据合约列表
        self.history_contracts = copy(self.recording_list)
        # rest查询合约列表
        self.query_contracts = [vt_symbol for vt_symbol in GetFilePath.all_trading_vt_symbols if is_target_contract(vt_symbol, self.gateway_name)]
    # ----------------------------------------------------------------------------------------------------
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
        api_key_credentials_map["wallet_address"] = log_account["wallet_address"]
        server: str = log_account["server"]
        proxy_host: str = log_account["host"]
        proxy_port: int = log_account["port"]
        limitFee: float = log_account["limitFee"]
        accountNumber: str = log_account["accountNumber"]
        self.account_file_name = log_account["account_file_name"]
        self.rest_api.connect(server, proxy_host, proxy_port, limitFee, self.gateway_name)
        self.ws_api.connect(proxy_host, proxy_port, server, accountNumber, self.gateway_name)
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅行情
        """
        self.ws_api.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单
        """
        return self.rest_api.send_order(req)
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        """
        self.rest_api.cancel_order(req)
    # ----------------------------------------------------------------------------------------------------
    def query_account(self) -> None:
        """
        查询资金
        """
        self.rest_api.query_account()
    # ----------------------------------------------------------------------------------------------------
    def query_position(self) -> None:
        pass
    # ----------------------------------------------------------------------------------------------------
    def query_active_orders(self, symbol: str) -> None:
        """
        查询活动委托单
        """
        self.rest_api.query_active_orders(symbol)
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, event):
        """
        查询合约历史数据
        """
        if self.history_contracts:
            symbol, exchange, gateway_name = extract_vt_symbol(self.history_contracts.pop(0))
            req = HistoryRequest(
                symbol=symbol,
                exchange=Exchange(exchange),
                interval=Interval.MINUTE,
                start=datetime.now(TZ_INFO) - timedelta(minutes=1440),
                end=datetime.now(TZ_INFO),
                gateway_name=self.gateway_name,
            )
            self.rest_api.query_history(req)
    # ----------------------------------------------------------------------------------------------------
    def process_timer_event(self, event):
        """
        处理定时任务
        """
        self.query_account()
        # 查询活动委托单
        if self.query_contracts:
            vt_symbol = self.query_contracts.pop(0)
            symbol, exchange, gateway_name = extract_vt_symbol(vt_symbol)
            self.query_active_orders(symbol)
            self.query_contracts.append(vt_symbol)
    # ----------------------------------------------------------------------------------------------------
    def close(self) -> None:
        """
        关闭连接
        """
        self.rest_api.stop()
        self.ws_api.stop()
    # ----------------------------------------------------------------------------------------------------
    def on_order(self, order: OrderData) -> None:
        """
        推送委托数据
        """
        self.orders[order.orderid] = copy(order)
        super().on_order(order)
    # ----------------------------------------------------------------------------------------------------
    def get_order(self, orderid: str) -> OrderData:
        """
        查询委托数据
        """
        return self.orders.get(orderid, None)
    # ----------------------------------------------------------------------------------------------------
    def init_query(self) -> None:
        """
        初始化查询任务
        """
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        if self.history_status:
            self.event_engine.register(EVENT_TIMER, self.query_history)
# ----------------------------------------------------------------------------------------------------
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
        self.account_date = None  # 账户日期
        self.accounts_info: Dict[str, dict] = {}

        self.order_count: int = 10000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0
    # ----------------------------------------------------------------------------------------------------
    def sign(self, request: Request) -> Request:
        """
        生成dYdX签名
        """
        security: Security = request.data.pop("security")
        now_iso_string = generate_now_iso()
        if security == Security.PUBLIC:
            request.data = {}
            return request
        else:
            request_path = ""
            if request.method in ["GET", "DELETE"]:
                api_params = request.params
                if api_params:
                    request_path = request.path + "?" + urlencode(api_params)
                    api_params = {}
            else:
                api_params = request.data
                if not api_params:
                    api_params = request.data = {}
                request.data = json.dumps(api_params)
            if not request_path:
                request_path = request.path
            signature: str = sign(request_path=request_path, method=request.method, iso_timestamp=now_iso_string, data=api_params)

        headers = {
            "DYDX-SIGNATURE": signature,
            "DYDX-API-KEY": api_key_credentials_map["key"],
            "DYDX-TIMESTAMP": now_iso_string,
            "DYDX-PASSPHRASE": api_key_credentials_map["passphrase"],
        }
        request.headers = headers

        return request

    # ----------------------------------------------------------------------------------------------------

    def connect(self, server: str, proxy_host: str, proxy_port: int, limitFee: float, gateway_name: str) -> None:
        """
        连接REST服务器
        """
        self.connect_time = int(datetime.now(TZ_INFO).strftime("%y%m%d%H%M%S"))

        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server
        self.limitFee = limitFee
        self.gateway_name = gateway_name
        if self.server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)

        self.start()
        self.get_sever_time()
        self.query_contract()

        self.gateway.write_log(f"交易接口：{self.gateway_name}，REST API启动成功")
    # ----------------------------------------------------------------------------------------------------
    def get_sever_time(self):
        """
        获取服务器时间
        """
        data: dict = {"security": Security.PUBLIC}
        self.add_request(method="GET", path="/v3/time", callback=self.on_sever_time, data=data)
    # ----------------------------------------------------------------------------------------------------
    def on_sever_time(self, data: dict, request: Request):
        """
        收到服务器时间回报
        """
        server_time = get_local_datetime(data["epoch"])
        self.gateway.write_log(f"交易所时间：{server_time}，本地时间：{datetime.now(TZ_INFO)}")
    # ----------------------------------------------------------------------------------------------------
    def query_contract(self) -> None:
        """
        查询合约信息
        """
        data: dict = {"security": Security.PUBLIC}

        self.add_request(method="GET", path="/v3/markets", callback=self.on_query_contract, data=data)
    # ----------------------------------------------------------------------------------------------------
    def query_account(self) -> None:
        """
        查询资金
        """
        data: dict = {
            "security": Security.PRIVATE,
        }

        self.add_request(method="GET", path=f"/v3/accounts/{self.gateway.id}", callback=self.on_query_account, data=data)
    # ----------------------------------------------------------------------------------------------------
    def query_active_orders(self, symbol: str) -> None:
        """
        查询活动委托单
        """
        params = {
            "market": symbol,
        }
        for status in ["OPEN", "PENDING"]:
            data: dict = {
                "security": Security.PRIVATE,
            }
            params.update({"status": status})
            self.add_request(
                method="GET",
                path="/v3/orders",
                callback=self.on_active_orders,
                data=data,
                params=params,
            )
    # ----------------------------------------------------------------------------------------------------
    def on_active_orders(self, data: dict, request: Request) -> None:
        """
        收到活动委托单回报
        """
        for order_data in data["orders"]:
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
                gateway_name=self.gateway_name,
            )
            if 0 < order.traded < order.volume:
                order.status = Status.PARTTRADED
            if order.orderid in list(self.gateway.orders):
                order.offset = self.gateway.orders[order.orderid].offset
            self.gateway.on_order(order)
    # ----------------------------------------------------------------------------------------------------
    def new_local_orderid(self) -> str:
        """
        生成local_orderid
        """
        with self.order_count_lock:
            self.order_count += 1
            local_orderid = str(self.connect_time + self.order_count)
            return local_orderid
    # ----------------------------------------------------------------------------------------------------
    def send_order(self, req: OrderRequest) -> str:
        """
        委托下单
        """
        # 生成本地委托号
        orderid: str = self.new_local_orderid()

        # 推送提交中事件
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
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
            expiration_epoch_seconds=expiration_epoch_seconds,
        )

        signature: str = order_to_sign(hash_number, api_key_credentials_map["stark_private_key"])

        # 生成委托请求
        data: dict = {
            "security": Security.PRIVATE,
            "market": req.symbol,
            "side": DIRECTION_VT2DYDX[req.direction],
            "type": ORDERTYPE_VT2DYDX[req.type],
            "size": str(req.volume),
            "price": str(req.price),
            "limitFee": str(self.limitFee),
            "expiration": epoch_seconds_to_iso(expiration_epoch_seconds),
            "postOnly": False,
            "clientId": orderid,
            "signature": signature,
        }
        # 限价单使用GTT(等待直到成交)
        if req.type == OrderType.LIMIT:
            data["timeInForce"] = "GTT"
        else:
            # 市价单使用IOC(无法立即成交的部分撤单)
            data["timeInForce"] = "IOC"
            if order.offset == Offset.CLOSE:
                data["reduceOnly"] = True

        self.add_request(
            method="POST",
            path="/v3/orders",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed,
        )

        return order.vt_orderid
    # ----------------------------------------------------------------------------------------------------
    def cancel_order(self, req: CancelRequest) -> None:
        """
        委托撤单
        """
        system_id: str = self.gateway.local_sys_map.get(req.orderid, "")
        if not system_id:
            self.gateway.write_log(f"撤单失败，找不到{req.orderid}对应的系统委托号")
            return

        data: dict = {"security": Security.PRIVATE}

        order: OrderData = self.gateway.get_order(req.orderid)

        self.add_request(method="DELETE", path=f"/v3/orders/{system_id}", callback=self.on_cancel_order, data=data, on_failed=self.on_cancel_failed, extra=order)
    # ----------------------------------------------------------------------------------------------------
    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """
        查询历史数据
        """
        history: List[BarData] = []
        limit = 100  # 最大获取K线数量
        time_consuming_start = time()
        start_time = req.start
        while True:
            end_time = start_time + timedelta(minutes=limit)
            params: dict = {
                "resolution": INTERVAL_VT2DYDX[req.interval],
                "limit": limit,
                "fromISO": start_time.isoformat("T", "minutes"),
                "toISO": end_time.isoformat("T", "minutes"),
            }
            resp: Response = self.request(method="GET", path=f"/v3/candles/{req.symbol}", data={"security": Security.PUBLIC}, params=params)
            if resp.status_code // 100 != 2:
                msg = f"合约：{req.vt_symbol}获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(msg)
                break
            else:
                data: dict = resp.json()
                if not data:
                    self.gateway.write_log(f"合约：{req.vt_symbol}获取历史数据为空")
                    break
                buf = []
                for data in data["candles"]:
                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=get_local_datetime(data["startedAt"]),
                        interval=req.interval,
                        volume=float(data["baseTokenVolume"]),      # 币的成交量
                        open_price=float(data["open"]),
                        high_price=float(data["high"]),
                        low_price=float(data["low"]),
                        close_price=float(data["close"]),
                        open_interest=float(data["startingOpenInterest"]),
                        gateway_name=self.gateway_name,
                    )
                    buf.append(bar)
                history.extend(buf)
                start_time = end_time - timedelta(minutes=1)
            if start_time >= req.end:
                break
        history.sort(key=lambda x: x.datetime)
        if not history:
            msg = f"未获取到合约：{req.vt_symbol}历史数据"
            self.gateway.write_log(msg)
            return
        for bar_data in chunked(history, 10000):  # 分批保存数据
            try:
                database_manager.save_bar_data(bar_data, False)  # 保存数据到数据库
            except Exception as err:
                self.gateway.write_log(f"{err}")
                return
        time_consuming_end = time()
        query_time = round(time_consuming_end - time_consuming_start, 3)
        msg = f"载入{req.vt_symbol}:bar数据，开始时间：{history[0].datetime} ，结束时间： {history[-1].datetime}，数据量：{len(history)}，耗时:{query_time}秒"
        self.gateway.write_log(msg)
        return history
    # ----------------------------------------------------------------------------------------------------
    def on_query_contract(self, data: dict, request: Request) -> None:
        """
        合约信息查询回报
        """
        for raw_data in data["markets"]:
            contract: ContractData = ContractData(
                symbol=raw_data,
                exchange=Exchange.DYDX,
                name=raw_data,
                price_tick=str_to_number(data["markets"][raw_data]["tickSize"]),
                size=1,
                min_volume=str_to_number(data["markets"][raw_data]["minOrderSize"]),
                max_volume=str_to_number(data["markets"][raw_data]["maxPositionSize"]),
                product=Product.FUTURES,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_contract(contract)

        self.gateway.write_log(f"交易接口：{self.gateway_name}，合约信息查询成功")
    # ----------------------------------------------------------------------------------------------------
    def on_query_account(self, data: dict, request: Request) -> None:
        """
        收到账户资金和持仓回报
        """
        data: dict = data["account"]
        balance: float = float(data["equity"])
        available: float = float(data["freeCollateral"])
        account: AccountData = AccountData(
            accountid=f"USDC_{self.gateway_name}",
            balance=balance,
            available=available,
            frozen=balance - available,
            file_name=self.gateway.account_file_name,
            gateway_name=self.gateway_name,
        )

        if account.balance:
            self.gateway.on_account(account)
            # 保存账户资金信息
            self.accounts_info[account.accountid] = account.__dict__
        if not self.accounts_info:
            return
        accounts_info = list(self.accounts_info.values())
        account_date = accounts_info[-1]["datetime"].date()
        account_path = str(GetFilePath.ctp_account_path).replace("ctp_account_main", self.gateway.account_file_name)
        write_header = not Path(account_path).exists()
        additional_writing = self.account_date and self.account_date != account_date
        self.account_date = account_date
        # 文件不存在则写入文件头，否则只在日期变更后追加写入文件
        if not write_header and not additional_writing:
            return
        write_mode = "w" if write_header else "a"
        for account_data in accounts_info:
            with open(account_path, write_mode, newline="") as f1:
                w1 = csv.DictWriter(f1, list(account_data))
                if write_header:
                    w1.writeheader()
                w1.writerow(account_data)

        for keys in data["openPositions"]:
            if data["openPositions"][keys]["side"] == "SHORT":
                direction = Direction.SHORT
                position.volume = -position.volume
            elif data["openPositions"][keys]["side"] == "LONG":
                direction = Direction.LONG
            position: PositionData = PositionData(
                symbol=keys,
                exchange=Exchange.DYDX,
                direction=direction,
                volume=float(data["openPositions"][keys]["size"]),
                price=float(data["openPositions"][keys]["entryPrice"]),
                pnl=float(data["openPositions"][keys]["unrealizedPnl"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)
    # ----------------------------------------------------------------------------------------------------
    def on_send_order(self, data: dict, request: Request) -> None:
        """
        委托下单回报
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """
        委托下单回报函数报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if not issubclass(exception_type, (ConnectionError, SSLError)):
            self.on_error(exception_type, exception_value, tb, request)
    # ----------------------------------------------------------------------------------------------------
    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """
        委托下单失败服务器报错回报
        """
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_order(self, data: dict, request: Request) -> None:
        """
        委托撤单回报
        """
        pass
    # ----------------------------------------------------------------------------------------------------
    def on_cancel_failed(self, status_code: str, request: Request) -> None:
        """
        撤单回报函数报错回报
        """
        if request.extra:
            order: OrderData = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)

        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)
# ----------------------------------------------------------------------------------------------------
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
        # 订阅主题数据推送到指定函数
        self.topic_map = {
            "v3_orderbook":self.on_orderbook,
            "v3_trades":self.on_orderbook,
            "v3_accounts":self.position_and_order,
            "v3_markets":self.on_tick,
        }
    # ----------------------------------------------------------------------------------------------------
    def connect(self, proxy_host: str, proxy_port: int, server: str, accountNumber: str, gateway_name: str) -> None:
        """
        连接Websocket行情频道
        """
        self.accountNumber = accountNumber
        self.gateway_name = gateway_name
        if server == "REAL":
            self.init(WEBSOCKET_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
        else:
            self.init(TESTNET_WEBSOCKET_HOST, proxy_host, proxy_port, gateway_name=self.gateway_name)
        self.start()
    # ----------------------------------------------------------------------------------------------------
    def on_connected(self) -> None:
        """
        连接成功回报
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket API连接成功")
        self.subscribe_topic()

        for req in list(self.subscribed.values()):
            self.subscribe(req)
    # ----------------------------------------------------------------------------------------------------
    def on_disconnected(self) -> None:
        """
        连接断开回报
        """
        self.gateway.write_log(f"交易接口：{self.gateway_name}，Websocket API连接断开")
    # ----------------------------------------------------------------------------------------------------
    def subscribe(self, req: SubscribeRequest) -> None:
        """
        订阅委托簿，逐笔成交和tick行情
        """

        # 缓存订阅记录
        self.subscribed[req.vt_symbol] = req
        symbol = req.symbol

        orderbook = OrderBook(symbol, req.exchange, self.gateway)
        self.order_books[symbol] = orderbook

        channels = ["v3_orderbook", "v3_trades", "v3_markets"]
        for channel in channels:
            req: dict = {"type": "subscribe", "channel": channel, "id": symbol}
            self.send_packet(req)
    # ----------------------------------------------------------------------------------------------------
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
            "passphrase": api_key_credentials_map["passphrase"],
        }
        self.send_packet(req)
    # ----------------------------------------------------------------------------------------------------
    def on_packet(self, packet: dict) -> None:
        """
        推送数据回报
        """
        type = packet.get("type", None)
        if type == "error":
            msg = packet["message"]
            # 过滤重复订阅错误
            if "already subscribed" not in msg:
                self.gateway.write_log(f"交易接口websocket收到错误回报：{msg}")
            return

        channel: str = packet.get("channel", None)
        if not channel:
            return
        self.topic_map[channel](packet)
    # ----------------------------------------------------------------------------------------------------
    def on_tick(self, packet: dict) -> None:
        """
        收到tick行情推送
        """
        raw = packet["contents"]
        for symbol, data in raw.items():
            order_book = self.order_books.get(symbol, None)
            if not order_book:
                continue
            # USD成交量转为币的成交量
            last_price = order_book.tick.last_price
            if "volume24H" in data and last_price:
                order_book.tick.volume = float(data["volume24H"]) / last_price
            if "openInterest" in data:
                order_book.tick.open_interest = float(data["openInterest"])
    # ----------------------------------------------------------------------------------------------------
    def on_orderbook(self, packet: dict) -> None:
        """
        订单簿更新推送
        """
        orderbook = self.order_books[packet["id"]]
        orderbook.on_message(packet)
    # ----------------------------------------------------------------------------------------------------
    def position_and_order(self, packet: dict) -> None:
        """
        收到持仓，委托，成交回报
        """
        data = packet["contents"]
        # 持仓推送
        for keys in data["account"]["openPositions"]:
            if data["openPositions"][keys]["side"] == "SHORT":
                direction = Direction.SHORT
                position.volume = -position.volume
            elif data["openPositions"][keys]["side"] == "LONG":
                direction = Direction.LONG
            position: PositionData = PositionData(
                symbol=keys,
                exchange=Exchange.DYDX,
                direction=direction,
                volume=float(data["openPositions"][keys]["size"]),
                price=float(data["openPositions"][keys]["entryPrice"]),
                pnl=float(data["openPositions"][keys]["unrealizedPnl"]),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_position(position)

        if packet["type"] == "subscribed":
            self.gateway.pos_id = data["account"]["positionId"]
            self.gateway.id = packet["id"]
            self.gateway.init_query()
        else:
            # 成交推送
            fills = data.get("fills", None)
            if not fills:
                return
            for fill_data in data["fills"]:
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
                    gateway_name=self.gateway_name,
                )
                self.gateway.on_trade(trade)
        # 委托单推送
        for order_data in data["orders"]:
            # 绑定本地和系统委托号映射
            local_orderid, gateway_id = order_data["clientId"], order_data["id"]
            local_sys_map, sys_local_map = self.gateway.local_sys_map, self.gateway.sys_local_map
            local_sys_map[local_orderid] = gateway_id
            sys_local_map[gateway_id] = local_orderid

            order: OrderData = OrderData(
                symbol=order_data["market"],
                exchange=Exchange.DYDX,
                orderid=local_orderid,
                type=ORDERTYPE_DYDX2VT[order_data["type"]],
                direction=DIRECTION_DYDX2VT[order_data["side"]],
                price=float(order_data["price"]),
                volume=float(order_data["size"]),
                traded=float(order_data["size"]) - float(order_data["remainingSize"]),
                status=STATUS_DYDX2VT.get(order_data["status"], Status.SUBMITTING),
                datetime=get_local_datetime(order_data["createdAt"]),
                gateway_name=self.gateway_name,
            )
            if 0 < order.traded < order.volume:
                order.status = Status.PARTTRADED
            if order.orderid in list(self.gateway.orders):
                order.offset = self.gateway.orders[order.orderid].offset
            self.gateway.on_order(order)
            # 委托单非活动状态删除本地委托单号与系统委托单号键值
            if not order.is_active():
                if local_orderid in local_sys_map:
                    system_id = local_sys_map[local_orderid]
                    local_sys_map.pop(local_orderid)
                    sys_local_map.pop(system_id)
# ----------------------------------------------------------------------------------------------------
class OrderBook:
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
    # ----------------------------------------------------------------------------------------------------
    def on_message(self, data: dict) -> None:
        """
        Websocket订单簿更新推送
        """
        type_: str = data["type"]
        channel: str = data["channel"]
        if type_ == "subscribed" and channel == "v3_orderbook":
            self.on_snapshot(data["contents"]["asks"], data["contents"]["bids"])
        elif type_ == "channel_data" and channel == "v3_orderbook":
            self.on_update(data["contents"])
        elif channel == "v3_trades":
            self.on_public_trade(data["contents"]["trades"])
    # ----------------------------------------------------------------------------------------------------
    def on_public_trade(self, data: list) -> None:
        """
        逐笔成交推送
        """
        tick: TickData = self.tick
        tick.last_price = float(data[0]["price"])
        #tick.datetime = get_local_datetime(data[0]["createdAt"])
        tick.datetime = datetime.now(TZ_INFO)
        self.gateway.on_tick(copy(tick))
    # ----------------------------------------------------------------------------------------------------
    def on_update(self, data: dict) -> None:
        """
        深度行情更新推送
        """
        # 定义一个通用的更新函数
        def update_order_book(order_book, updates):
            for price, volume in updates:
                price = float(price)
                volume = float(volume)
                if volume:
                    order_book[price] = volume
                else:
                    order_book.pop(price, None)

        # 更新asks和bids
        if data["asks"]:
            update_order_book(self.asks, data["asks"])
        if data["bids"]:
            update_order_book(self.bids, data["bids"])

        # 触发tick生成
        self.generate_tick()
    # ----------------------------------------------------------------------------------------------------
    def on_snapshot(self, asks: Sequence[List], bids: Sequence[List]) -> None:
        """
        深度行情全量推送回报
        """
        # 直接使用字典推导，清晰地更新asks和bids
        self.asks = {float(ask["price"]): float(ask["size"]) for ask in asks}
        self.bids = {float(bid["price"]): float(bid["size"]) for bid in bids}

        # 触发tick生成
        self.generate_tick()
    # ----------------------------------------------------------------------------------------------------
    def generate_tick(self) -> None:
        """
        合成tick
        """
        tick: TickData = self.tick
        if not tick.last_price:
            return

        # 对bids和asks进行排序和裁剪
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:10]
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])[:10]

        # 校验并清理不合逻辑的价格
        if sorted_bids and sorted_asks:
            bid_price_1, ask_price_1 = sorted_bids[0][0], sorted_asks[0][0]
            if bid_price_1 >= ask_price_1:
                if tick.last_price > ask_price_1:
                    sorted_asks.pop(0)
                if tick.last_price < bid_price_1:
                    sorted_bids.pop(0)

        # 重置并更新bids和asks
        self.bids, self.asks = {}, {}
        def update_order_book(order_book, sorted_prices, prefix):
            for index, (price, volume) in enumerate(sorted_prices):
                setattr(tick, f"{prefix}_price_{index + 1}", price)
                setattr(tick, f"{prefix}_volume_{index + 1}", volume)
                order_book[price] = volume

        update_order_book(self.bids, sorted_bids, "bid")
        update_order_book(self.asks, sorted_asks, "ask")
        if tick.last_price:
            tick.datetime = datetime.now(TZ_INFO)
            self.gateway.on_tick(copy(tick))
# ----------------------------------------------------------------------------------------------------
def generate_now_iso() -> str:
    """
    生成ISO时间
    """
    return datetime.now(UTC).replace(tzinfo=None).isoformat("T", "milliseconds") + "Z"
# ----------------------------------------------------------------------------------------------------
def epoch_seconds_to_iso(epoch: float) -> str:
    """
    时间格式转换
    """
    return datetime.utcfromtimestamp(epoch).isoformat("T", "milliseconds") + "Z"
# ----------------------------------------------------------------------------------------------------
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
        body = json.dumps(data, separators=(",", ":"))
    message_string = "".join([iso_timestamp, method, request_path, body])
    hashed = hmac.new(
        base64.urlsafe_b64decode(
            (api_key_credentials_map["secret"]).encode("utf-8"),
        ),
        msg=message_string.encode("utf-8"),
        digestmod=hashlib.sha256,
    )
    return base64.urlsafe_b64encode(hashed.digest()).decode()
