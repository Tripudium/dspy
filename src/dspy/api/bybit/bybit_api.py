"""
This module provides a simple interface to the pybit library functions.
"""

import time
import logging
import numpy as np
import pybit.unified_trading as bb

# Local imports
from dspy.api.bybit.config import Config
from dspy.api.api_registry import register_api
from dspy.api.base import Exchange

logger = logging.getLogger("DS.exchanges")


@register_api("bybit")
class ByBitManager(Exchange):
    """
    Simple interface for interacting with ByBit.
    """

    def __init__(self, config: Config):
        """
        Set up a HTTP connector.

        Arguments:
            config.api_key -- the public API key
            config.api_secret -- the private API key
        """
        super().__init__()
        self.s = bb.HTTP(
            api_key=config.api_key,
            api_secret=config.api_secret,
            logging_level=logging.INFO,
        )

    # Market
    def get_mid(self, symbol: str) -> float:
        """
        Return best mid price.

        Arguments:
            symbol -- the product symbol
        """
        last_price = self.s.get_tickers(category="linear", symbol=symbol)["result"][
            "list"
        ][0]["lastPrice"]
        return float(last_price)

    def get_orderbook(self, symbol: str, depth: int = 25) -> list[float]:
        """
        Return orderbook for product.

        Arguments:
            symbol -- the product symbol
            depth -- the depth of the orderbook
        """
        orderbook = self.s.get_orderbook(category="linear", symbol=symbol, limit=depth)[
            "result"
        ]
        bids = np.array(orderbook["b"], dtype=float)
        asks = np.array(orderbook["a"], dtype=float)
        orderbook = {
            "b": bids,
            "a": asks,
            "ts": orderbook["ts"],
            "cts": orderbook["cts"],
        }
        return orderbook

    def get_ask(self, symbol: str, depth: int = 1) -> list[float]:
        """
        Return best ask price and volume.

        Arguments:
            symbol -- the product symbol
        """
        ask = self.get_orderbook(symbol, depth=1)["a"][0]
        return [float(ask[0]), float(ask[1])]

    def get_bid(self, symbol: str, depth: int = 1) -> list[float]:
        """
        Return best bid price and volume.

        Arguments:
            symbol -- the product symbol
        """
        bid = self.get_orderbook(symbol, depth=1)["b"][0]
        return [float(bid[0]), float(bid[1])]

    def get_trades(self, symbol: str, limit: int = 100) -> list[float]:
        """
        Return trades for product.

        Arguments:
            symbol -- the product symbol
            limit -- the number of trades to return
        """
        trades = self.s.get_public_trade_history(
            category="linear", symbol=symbol, limit=limit
        )["result"]["list"]
        trades = [
            {
                "ts": t["time"],
                "price": float(t["price"]),
                "qty": float(t["size"]),
                "side": 1 if t["side"] == "Buy" else -1,
            }
            for t in trades
        ]
        return trades

    def get_latency(self, symbol: str, depth: int = 1) -> float:
        """
        Return latency of exchange.

        Arguments:
            symbol -- the product symbol
            depth -- the depth of the orderbook
        """
        orderbook = self.s.get_orderbook(
            category="linear",
            symbol=symbol,
            limit=depth,
        )["result"]
        latency = orderbook["ts"] - orderbook["cts"]
        return latency

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        """
        Set leverage for product.
        """
        resp = self.s.set_leverage(
            category="linear",
            symbol=symbol,
            buyLeverage=leverage,
            sellLeverage=leverage,
        )
        return resp

    # Account info
    def get_wallet_balance(self) -> float:
        """
        Return wallet balance.

        Arguments:
            symbol -- the product symbol
        """
        wallet_balance = self.s.get_wallet_balance(accountType="UNIFIED")["result"][
            "list"
        ][0]["totalAvailableBalance"]
        return float(wallet_balance)

    def get_fees(self, symbol: str) -> list:
        """Return taker and maker fees for product."""
        fees = self.s.get_fee_rates(symbol=symbol)["result"]["list"][0]
        return [float(fees["takerFeeRate"]), float(fees["makerFeeRate"])]

    # Position info
    def get_positions(self, symbols: list[str]) -> dict:
        """
        Return positions in product specified by symbol.

        Arguments:
            symbol -- the product symbol
        """
        positions = {}
        for s in symbols:
            pos = self.s.get_positions(
                category="linear",
                symbol=s,
            )["result"]["list"][0]
            sign = 1 if pos["side"] == "Buy" else -1
            if pos["size"] != "0":
                p = {
                    "size": sign * float(pos["size"]),
                    "aep": float(pos["avgPrice"]),
                    "mark_price": float(pos["markPrice"]),
                    "value": float(pos["positionValue"]),
                    "leverage": float(pos["leverage"]),
                    "position_balance": float(pos["positionBalance"]),
                    "unrealized_pnl": float(pos["unrealisedPnl"]),
                    "realized_pnl": float(pos["curRealisedPnl"]),
                }
            else:
                p = {
                    "size": 0,
                    "aep": 0,
                    "mark_price": float(pos["markPrice"]),
                    "value": 0,
                    "leverage": float(pos["leverage"]),
                    "position_balance": 0,
                    "unrealized_pnl": 0,
                    "realized_pnl": float(pos["curRealisedPnl"]),
                }
            positions[s] = p

        if len(symbols) == 1:
            positions = positions[symbols[0]]
        return positions

    # Trading
    def place_order(
        self, symbol: str, qty: float, price: float | None = None, type: str = "Market"
    ) -> dict:
        """
        Place limit order at given price or market order.

        Arguments:
            symbol -- the product symbol
            qty -- the quantity to trade
            price -- the price to trade at
            type -- the type of order to place
        """
        if price is None:
            price = 0.0
        response = self.s.place_order(
            category="linear",
            symbol=symbol,
            side="Sell" if qty < 0 else "Buy",
            orderType=type,
            qty=str(abs(qty)),
            price=str(price),
            timeInForce="GTC",
            isLeverage=True,
        )
        order_id = response["result"]["orderId"]
        resp = {
            "order_id": order_id,
            "ret_code": response["retCode"],
            "time": response["time"],
        }
        return resp

    def replace_order(
        self, symbol: str, order_id: float, qty: float, price: float
    ) -> dict:
        """Cancel specified limit order and place a new one."""
        self.s.cancel_order(category="linear", symbol=symbol, orderId=order_id)
        response = self.place_order(
            symbol=symbol, qty=qty, price=price, type="Limit", isLeverage=True
        )
        return response

    def place_batch_order(self, symbol: str, qtys: list, prices: list) -> dict:
        """
        Place a collection of orders for a given product.

        Arguments:
            symbol -- the product symbol
            qtys -- the quantities to trade
            prices -- the prices to trade at
        """
        orders = [
            {
                "category": "linear",
                "symbol": symbol,
                "orderType": "Limit",
                "side": "Buy" if qtys[k] > 0 else "Sell",
                "qty": str(abs(qtys[k])),
                "price": str(int(prices[k])),
                "timeInForce": "GTC",
            }
            for k in range(len(qtys))
        ]
        response = self.s.place_batch_order(category="linear", request=orders)
        resp = [i["orderId"] for i in response["result"]["list"]]
        return resp

    def cancel_order(self, symbol: str, order_id: str):
        """
        Cancel specific limit order based on order id.

        Arguments:
            symbol -- the product symbol
            order_id -- the order id to cancel
        """
        resp = self.s.cancel_order(category="linear", symbol=symbol, orderId=order_id)
        return resp["retCode"]

    def cancel_batch_order(self, symbol: str, order_ids: list) -> dict:
        """
        Cancel a list of orders for one product based on order ids.

        Arguments:
            symbol -- the product symbol
            order_ids -- the order ids to cancel
        """
        to_cancel = [
            {"category": "linear", "symbol": symbol, "order_id": i} for i in order_ids
        ]
        resp = self.s.cancel_batch_order(category="linear", request=to_cancel)
        return resp["retCode"]

    def cancel_all_orders(self, symbol: str) -> dict:
        """
        Cancel all outstanding orders for a set of products.

        Arguments:
            symbol -- the product symbol
        """
        resp = self.s.cancel_all_orders(category="linear", symbol=symbol)["retCode"]
        return resp

    def close_positions(self, symbols: list[str]) -> dict:
        """
        Close positions for a list of products.

        Arguments:
            symbols -- the product symbols
        """
        positions = self.get_positions(symbols)
        responses = {}
        for s in symbols:
            p = positions[s]
            if p["size"] != 0:
                resp = self.s.place_order(
                    category="linear",
                    symbol=s,
                    side="Sell" if p["size"] > 0 else "Buy",
                    orderType="Market",
                    qty=str(abs(p["size"])),
                )["retCode"]
            else:
                resp = None
            responses[s] = resp
        return responses

    def set_trading_stop(self, symbol: str, stop_price: int) -> dict:
        """
        Set stop loss.

        Arguments:
            symbol -- the product symbol
            stop_price -- the stop loss price
        """
        resp = self.s.set_trading_stop(
            category="linear", symbol=symbol, stopLoss=str(stop_price)
        )
        return resp

    # Trade History and PnL
    def get_trade_history(
        self,
        symbol: str = None,
        limit: int = 50,
        start_time: int = None,
        end_time: int = None,
    ) -> list[dict]:
        """
        Get user's trade execution history with filled prices.

        Arguments:
            symbol -- the product symbol (optional, returns all if not specified)
            limit -- number of records to return (default: 50, max: 100)
            start_time -- start timestamp in milliseconds (optional)
            end_time -- end timestamp in milliseconds (optional)
        """
        params = {"category": "linear", "limit": limit}
        if symbol:
            params["symbol"] = symbol
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        response = self.s.get_executions(**params)
        executions = response["result"]["list"]

        trades = []
        for exec in executions:
            trade = {
                "symbol": exec["symbol"],
                "side": exec["side"],
                "price": float(exec["execPrice"]),
                "qty": float(exec["execQty"]),
                "exec_type": exec["execType"],
                "exec_value": float(exec["execValue"]),
                "exec_fee": float(exec["execFee"]),
                "fee_rate": float(exec["feeRate"]) if "feeRate" in exec else 0,
                "exec_time": int(exec["execTime"]),
                "order_type": exec.get("orderType", ""),
                "order_price": float(exec.get("orderPrice", 0)),
            }
            trades.append(trade)

        return trades

    def get_filled_orders(
        self, symbol: str = None, limit: int = 50, order_filter: str = "Filled"
    ) -> list[dict]:
        """
        Get filled orders with execution prices.

        Arguments:
            symbol -- the product symbol (optional, returns all if not specified)
            limit -- number of records to return (default: 50)
            order_filter -- filter by order status (default: 'Filled')
        """
        params = {"category": "linear", "limit": limit, "orderFilter": order_filter}
        if symbol:
            params["symbol"] = symbol

        response = self.s.get_order_history(**params)
        orders = response["result"]["list"]

        filled_orders = []
        for order in orders:
            if order["orderStatus"] == "Filled" or (
                order_filter != "Filled" and float(order.get("cumExecQty", 0)) > 0
            ):
                filled_order = {
                    "order_id": order["orderId"],
                    "order_link_id": order.get("orderLinkId", ""),
                    "symbol": order["symbol"],
                    "side": order["side"],
                    "order_type": order["orderType"],
                    "price": float(order["price"]),
                    "qty": float(order["qty"]),
                    "avg_price": float(order.get("avgPrice", 0)),
                    "cum_exec_qty": float(order.get("cumExecQty", 0)),
                    "cum_exec_value": float(order.get("cumExecValue", 0)),
                    "cum_exec_fee": float(order.get("cumExecFee", 0)),
                    "order_status": order["orderStatus"],
                    "created_time": int(order["createdTime"]),
                    "updated_time": int(order["updatedTime"]),
                }
                filled_orders.append(filled_order)

        return filled_orders

    def get_pnl(
        self,
        symbol: str = None,
        limit: int = 50,
        start_time: int = None,
        end_time: int = None,
    ) -> list[dict]:
        """
        Get closed PnL records for positions.

        Arguments:
            symbol -- the product symbol (optional, returns all if not specified)
            limit -- number of records to return (default: 50)
            start_time -- start timestamp in milliseconds (optional)
            end_time -- end timestamp in milliseconds (optional)
        """
        params = {"category": "linear", "limit": limit}
        if symbol:
            params["symbol"] = symbol
        if start_time:
            params["startTime"] = start_time
        if end_time:
            params["endTime"] = end_time

        response = self.s.get_closed_pnl(**params)
        pnl_records = response["result"]["list"]

        pnl_list = []
        for record in pnl_records:
            pnl = {
                "symbol": record["symbol"],
                "side": record["side"],
                "qty": float(record["qty"]),
                "order_price": float(record.get("orderPrice", 0)),
                "order_type": record.get("orderType", ""),
                "exec_type": record.get("execType", ""),
                "closed_size": float(record.get("closedSize", 0)),
                "cum_entry_value": float(record.get("cumEntryValue", 0)),
                "avg_entry_price": float(record.get("avgEntryPrice", 0)),
                "cum_exit_value": float(record.get("cumExitValue", 0)),
                "avg_exit_price": float(record.get("avgExitPrice", 0)),
                "closed_pnl": float(record.get("closedPnl", 0)),
                "fill_count": int(record.get("fillCount", 0)),
                "leverage": float(record.get("leverage", 0)),
                "created_time": int(record["createdTime"]),
                "updated_time": int(record["updatedTime"]),
            }
            pnl_list.append(pnl)

        return pnl_list

    # Various helper and dummy methods
    def wait(self, timeout: float):
        """
        Wait for an indicated amount of time.

        Arguments:
            timeout -- the amount of time to wait
        """
        time.sleep(timeout)

    def next(self):
        """
        For compatibility with simulator.
        """
        return True
