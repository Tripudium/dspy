"""
Bybit WebSocket streaming interface for real-time market data.

This module provides a WebSocket client for streaming real-time market data
from Bybit, including order book updates, trades, and ticker information.
It uses the official pybit WebSocket implementation with custom handlers.

Key Features:
    - Real-time order book streaming (depth updates)
    - Trade stream with buy/sell side classification
    - Ticker updates with best bid/ask prices
    - Automatic reconnection and error handling
    - Callback-based architecture for data processing

Example:
    Basic usage with callbacks:
    
    >>> from dspy.api.bybit.websocket_stream import BybitWebSocketStream
    >>> 
    >>> def on_orderbook(data):
    ...     print(f"Order book update: {data}")
    >>> 
    >>> stream = BybitWebSocketStream()
    >>> stream.subscribe_orderbook("BTCUSDT", callback=on_orderbook)
    >>> stream.start()

Notes:
    The WebSocket uses Bybit's public streams which don't require authentication
    for market data. Private streams (orders, positions) would require auth.

See Also:
    dspy.api.bybit.bybit_api: REST API implementation
    dspy.api.bybit.config: Configuration and API credentials
"""

import asyncio
import json
import logging
from typing import Callable, Dict, List, Optional

from pybit import WebSocket

from dspy.api.bybit.config import API_KEY, API_SECRET

logger = logging.getLogger(__name__)


class BybitWebSocketStream:
    """
    WebSocket client for streaming real-time Bybit market data.
    
    This class provides methods to subscribe to various data streams and
    handle incoming messages through callbacks.
    """
    
    def __init__(self, testnet: bool = False, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """
        Initialize the WebSocket stream client.
        
        Args:
            testnet: Whether to connect to testnet (default: False for mainnet)
            api_key: API key for private streams (default: from environment)
            api_secret: API secret for private streams (default: from environment)
        """
        self.testnet = testnet
        self.api_key = api_key or API_KEY
        self.api_secret = api_secret or API_SECRET
        self.ws_public = None
        self.ws_private = None
        self.callbacks: Dict[str, List[Callable]] = {
            "orderbook": [],
            "trade": [],
            "ticker": [],
            "kline": [],
            "position": [],
            "order": [],
            "execution": [],
            "wallet": [],
        }
        self.subscriptions = []
        self.private_subscriptions = []
        
    def _create_public_websocket(self):
        """Create public WebSocket instance for market data."""
        self.ws_public = WebSocket(
            testnet=self.testnet,
            channel_type="linear",  # For USDT perpetual contracts
        )
        
    def _create_private_websocket(self):
        """Create private WebSocket instance for account data."""
        if not self.api_key or not self.api_secret:
            raise ValueError("API key and secret required for private streams")
            
        self.ws_private = WebSocket(
            testnet=self.testnet,
            channel_type="private",
            api_key=self.api_key,
            api_secret=self.api_secret,
        )
        
    def subscribe_orderbook(self, symbol: str, depth: int = 25, callback: Optional[Callable] = None):
        """
        Subscribe to order book updates.
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            depth: Order book depth (1, 25, 50, 100, 200)
            callback: Function to call with order book data
        """
        if callback:
            self.callbacks["orderbook"].append(callback)
            
        channel = f"orderbook.{depth}.{symbol}"
        self.subscriptions.append(("orderbook", channel))
        
        if self.ws_public:
            self.ws_public.orderbook_stream(
                depth=depth,
                symbol=symbol,
                callback=self._handle_orderbook
            )
            
    def subscribe_trades(self, symbol: str, callback: Optional[Callable] = None):
        """
        Subscribe to trade updates.
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            callback: Function to call with trade data
        """
        if callback:
            self.callbacks["trade"].append(callback)
            
        channel = f"publicTrade.{symbol}"
        self.subscriptions.append(("trade", channel))
        
        if self.ws_public:
            self.ws_public.trade_stream(
                symbol=symbol,
                callback=self._handle_trade
            )
            
    def subscribe_ticker(self, symbol: str, callback: Optional[Callable] = None):
        """
        Subscribe to ticker updates (best bid/ask).
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            callback: Function to call with ticker data
        """
        if callback:
            self.callbacks["ticker"].append(callback)
            
        channel = f"tickers.{symbol}"
        self.subscriptions.append(("ticker", channel))
        
        if self.ws_public:
            self.ws_public.ticker_stream(
                symbol=symbol,
                callback=self._handle_ticker
            )
            
    def subscribe_kline(self, symbol: str, interval: str = "1", callback: Optional[Callable] = None):
        """
        Subscribe to kline/candlestick updates.
        
        Args:
            symbol: Trading symbol (e.g., "BTCUSDT")
            interval: Kline interval (1, 3, 5, 15, 30, 60, 120, 240, 360, 720, D, W, M)
            callback: Function to call with kline data
        """
        if callback:
            self.callbacks["kline"].append(callback)
            
        channel = f"kline.{interval}.{symbol}"
        self.subscriptions.append(("kline", channel))
        
        if self.ws_public:
            self.ws_public.kline_stream(
                interval=interval,
                symbol=symbol,
                callback=self._handle_kline
            )
            
    def _handle_orderbook(self, message):
        """Process order book update messages."""
        try:
            data = self._parse_message(message)
            if data:
                # Convert to standardized format
                formatted_data = {
                    "symbol": data.get("s"),
                    "timestamp": data.get("ts"),
                    "update_id": data.get("u"),
                    "bids": [(float(price), float(qty)) for price, qty in data.get("b", [])],
                    "asks": [(float(price), float(qty)) for price, qty in data.get("a", [])],
                }
                
                # Call all registered callbacks
                for callback in self.callbacks["orderbook"]:
                    callback(formatted_data)
                    
        except Exception as e:
            logger.error(f"Error handling orderbook message: {e}")
            
    def _handle_trade(self, message):
        """Process trade update messages."""
        try:
            data = self._parse_message(message)
            if data and "data" in data:
                for trade in data["data"]:
                    # Convert to standardized format with integer side
                    formatted_trade = {
                        "symbol": trade.get("s"),
                        "timestamp": trade.get("T"),
                        "trade_id": trade.get("i"),
                        "price": float(trade.get("p", 0)),
                        "vol": float(trade.get("v", 0)),  # Using 'vol' as per your convention
                        "side": 1 if trade.get("S") == "Buy" else -1,  # 1 for buy, -1 for sell
                        "is_block_trade": trade.get("BT", False),
                    }
                    
                    # Call all registered callbacks
                    for callback in self.callbacks["trade"]:
                        callback(formatted_trade)
                        
        except Exception as e:
            logger.error(f"Error handling trade message: {e}")
            
    def _handle_ticker(self, message):
        """Process ticker update messages."""
        try:
            data = self._parse_message(message)
            if data and "data" in data:
                ticker_data = data["data"]
                
                # Convert to standardized format
                formatted_ticker = {
                    "symbol": ticker_data.get("symbol"),
                    "timestamp": data.get("ts"),
                    "last_price": float(ticker_data.get("lastPrice", 0)),
                    "bid_price": float(ticker_data.get("bid1Price", 0)),
                    "bid_size": float(ticker_data.get("bid1Size", 0)),
                    "ask_price": float(ticker_data.get("ask1Price", 0)),
                    "ask_size": float(ticker_data.get("ask1Size", 0)),
                    "volume_24h": float(ticker_data.get("volume24h", 0)),
                    "turnover_24h": float(ticker_data.get("turnover24h", 0)),
                    "price_24h_change": float(ticker_data.get("price24hPcnt", 0)),
                }
                
                # Call all registered callbacks
                for callback in self.callbacks["ticker"]:
                    callback(formatted_ticker)
                    
        except Exception as e:
            logger.error(f"Error handling ticker message: {e}")
            
    def _handle_kline(self, message):
        """Process kline update messages."""
        try:
            data = self._parse_message(message)
            if data and "data" in data:
                for kline in data["data"]:
                    # Convert to standardized format
                    formatted_kline = {
                        "symbol": kline.get("symbol"),
                        "timestamp": kline.get("timestamp"),
                        "start_time": kline.get("start"),
                        "end_time": kline.get("end"),
                        "interval": kline.get("interval"),
                        "open": float(kline.get("open", 0)),
                        "high": float(kline.get("high", 0)),
                        "low": float(kline.get("low", 0)),
                        "close": float(kline.get("close", 0)),
                        "volume": float(kline.get("volume", 0)),
                        "turnover": float(kline.get("turnover", 0)),
                        "confirm": kline.get("confirm", False),
                    }
                    
                    # Call all registered callbacks
                    for callback in self.callbacks["kline"]:
                        callback(formatted_kline)
                        
        except Exception as e:
            logger.error(f"Error handling kline message: {e}")
            
    def _parse_message(self, message):
        """Parse WebSocket message to extract data."""
        if isinstance(message, str):
            return json.loads(message)
        return message
        
    # Private stream methods
    def subscribe_positions(self, callback: Optional[Callable] = None):
        """
        Subscribe to position updates (requires authentication).
        
        Args:
            callback: Function to call with position data
        """
        if callback:
            self.callbacks["position"].append(callback)
            
        self.private_subscriptions.append(("position", "position"))
        
        if self.ws_private:
            self.ws_private.position_stream(callback=self._handle_position)
            
    def subscribe_orders(self, callback: Optional[Callable] = None):
        """
        Subscribe to order updates (requires authentication).
        
        Args:
            callback: Function to call with order data
        """
        if callback:
            self.callbacks["order"].append(callback)
            
        self.private_subscriptions.append(("order", "order"))
        
        if self.ws_private:
            self.ws_private.order_stream(callback=self._handle_order)
            
    def subscribe_executions(self, callback: Optional[Callable] = None):
        """
        Subscribe to execution/fill updates (requires authentication).
        
        Args:
            callback: Function to call with execution data
        """
        if callback:
            self.callbacks["execution"].append(callback)
            
        self.private_subscriptions.append(("execution", "execution"))
        
        if self.ws_private:
            self.ws_private.execution_stream(callback=self._handle_execution)
            
    def subscribe_wallet(self, callback: Optional[Callable] = None):
        """
        Subscribe to wallet balance updates (requires authentication).
        
        Args:
            callback: Function to call with wallet data
        """
        if callback:
            self.callbacks["wallet"].append(callback)
            
        self.private_subscriptions.append(("wallet", "wallet"))
        
        if self.ws_private:
            self.ws_private.wallet_stream(callback=self._handle_wallet)
            
    def _handle_position(self, message):
        """Process position update messages."""
        try:
            data = self._parse_message(message)
            if data and "data" in data:
                for position in data["data"]:
                    # Convert to standardized format
                    formatted_position = {
                        "symbol": position.get("symbol"),
                        "side": 1 if position.get("side") == "Buy" else -1,
                        "size": float(position.get("size", 0)),
                        "position_value": float(position.get("positionValue", 0)),
                        "entry_price": float(position.get("avgPrice", 0)),
                        "mark_price": float(position.get("markPrice", 0)),
                        "liq_price": float(position.get("liqPrice", 0)) if position.get("liqPrice") else None,
                        "unrealized_pnl": float(position.get("unrealisedPnl", 0)),
                        "realized_pnl": float(position.get("realisedPnl", 0)),
                        "position_margin": float(position.get("positionMM", 0)),
                        "leverage": float(position.get("leverage", 0)),
                        "position_status": position.get("positionStatus"),
                        "adl_rank_indicator": position.get("adlRankIndicator"),
                        "updated_time": position.get("updatedTime"),
                    }
                    
                    # Call all registered callbacks
                    for callback in self.callbacks["position"]:
                        callback(formatted_position)
                        
        except Exception as e:
            logger.error(f"Error handling position message: {e}")
            
    def _handle_order(self, message):
        """Process order update messages."""
        try:
            data = self._parse_message(message)
            if data and "data" in data:
                for order in data["data"]:
                    # Convert to standardized format
                    formatted_order = {
                        "order_id": order.get("orderId"),
                        "order_link_id": order.get("orderLinkId"),
                        "symbol": order.get("symbol"),
                        "side": 1 if order.get("side") == "Buy" else -1,
                        "order_type": order.get("orderType"),
                        "price": float(order.get("price", 0)),
                        "qty": float(order.get("qty", 0)),
                        "leaves_qty": float(order.get("leavesQty", 0)),
                        "leaves_value": float(order.get("leavesValue", 0)),
                        "cum_exec_qty": float(order.get("cumExecQty", 0)),
                        "cum_exec_value": float(order.get("cumExecValue", 0)),
                        "cum_exec_fee": float(order.get("cumExecFee", 0)),
                        "order_status": order.get("orderStatus"),
                        "time_in_force": order.get("timeInForce"),
                        "reduce_only": order.get("reduceOnly", False),
                        "close_on_trigger": order.get("closeOnTrigger", False),
                        "created_time": order.get("createdTime"),
                        "updated_time": order.get("updatedTime"),
                        "trigger_price": float(order.get("triggerPrice", 0)) if order.get("triggerPrice") else None,
                        "trigger_by": order.get("triggerBy"),
                        "stop_loss": float(order.get("stopLoss", 0)) if order.get("stopLoss") else None,
                        "take_profit": float(order.get("takeProfit", 0)) if order.get("takeProfit") else None,
                    }
                    
                    # Call all registered callbacks
                    for callback in self.callbacks["order"]:
                        callback(formatted_order)
                        
        except Exception as e:
            logger.error(f"Error handling order message: {e}")
            
    def _handle_execution(self, message):
        """Process execution/fill messages."""
        try:
            data = self._parse_message(message)
            if data and "data" in data:
                for execution in data["data"]:
                    # Convert to standardized format
                    formatted_execution = {
                        "exec_id": execution.get("execId"),
                        "order_id": execution.get("orderId"),
                        "order_link_id": execution.get("orderLinkId"),
                        "symbol": execution.get("symbol"),
                        "side": 1 if execution.get("side") == "Buy" else -1,
                        "price": float(execution.get("execPrice", 0)),
                        "qty": float(execution.get("execQty", 0)),
                        "exec_type": execution.get("execType"),
                        "exec_value": float(execution.get("execValue", 0)),
                        "exec_fee": float(execution.get("execFee", 0)),
                        "exec_time": execution.get("execTime"),
                        "is_maker": execution.get("isMaker", False),
                        "fee_rate": float(execution.get("feeRate", 0)),
                        "trade_iv": float(execution.get("tradeIv", 0)) if execution.get("tradeIv") else None,
                        "mark_price": float(execution.get("markPrice", 0)) if execution.get("markPrice") else None,
                        "index_price": float(execution.get("indexPrice", 0)) if execution.get("indexPrice") else None,
                        "underlying_price": float(execution.get("underlyingPrice", 0)) if execution.get("underlyingPrice") else None,
                        "block_trade_id": execution.get("blockTradeId"),
                    }
                    
                    # Call all registered callbacks
                    for callback in self.callbacks["execution"]:
                        callback(formatted_execution)
                        
        except Exception as e:
            logger.error(f"Error handling execution message: {e}")
            
    def _handle_wallet(self, message):
        """Process wallet balance update messages."""
        try:
            data = self._parse_message(message)
            if data and "data" in data:
                for wallet in data["data"]:
                    # Convert to standardized format
                    formatted_wallet = {
                        "account_type": wallet.get("accountType"),
                        "coin": wallet.get("coin"),
                        "wallet_balance": float(wallet.get("walletBalance", 0)),
                        "available_balance": float(wallet.get("availableBalance", 0)),
                        "total_order_margin": float(wallet.get("totalOrderIM", 0)),
                        "total_position_margin": float(wallet.get("totalPositionIM", 0)),
                        "total_position_mm": float(wallet.get("totalPositionMM", 0)),
                        "unrealized_pnl": float(wallet.get("unrealisedPnl", 0)),
                        "cum_realized_pnl": float(wallet.get("cumRealisedPnl", 0)),
                        "given_cash": float(wallet.get("givenCash", 0)),
                        "service_cash": float(wallet.get("serviceCash", 0)),
                    }
                    
                    # Call all registered callbacks
                    for callback in self.callbacks["wallet"]:
                        callback(formatted_wallet)
                        
        except Exception as e:
            logger.error(f"Error handling wallet message: {e}")
        
    def start(self):
        """Start the WebSocket connection and begin streaming."""
        # Start public streams if there are any subscriptions
        if self.subscriptions and not self.ws_public:
            self._create_public_websocket()
            
        # Start private streams if there are any private subscriptions
        if self.private_subscriptions and not self.ws_private:
            self._create_private_websocket()
            
        logger.info("Starting Bybit WebSocket streams...")
        
        # Re-subscribe to all public channels
        if self.ws_public:
            for stream_type, channel in self.subscriptions:
                if stream_type == "orderbook":
                    # Extract depth and symbol from channel
                    parts = channel.split(".")
                    depth = int(parts[1])
                    symbol = parts[2]
                    self.ws_public.orderbook_stream(
                        depth=depth,
                        symbol=symbol,
                        callback=self._handle_orderbook
                    )
                elif stream_type == "trade":
                    symbol = channel.split(".")[-1]
                    self.ws_public.trade_stream(
                        symbol=symbol,
                        callback=self._handle_trade
                    )
                elif stream_type == "ticker":
                    symbol = channel.split(".")[-1]
                    self.ws_public.ticker_stream(
                        symbol=symbol,
                        callback=self._handle_ticker
                    )
                elif stream_type == "kline":
                    parts = channel.split(".")
                    interval = parts[1]
                    symbol = parts[2]
                    self.ws_public.kline_stream(
                        interval=interval,
                        symbol=symbol,
                        callback=self._handle_kline
                    )
                    
        # Re-subscribe to all private channels
        if self.ws_private:
            for stream_type, channel in self.private_subscriptions:
                if stream_type == "position":
                    self.ws_private.position_stream(callback=self._handle_position)
                elif stream_type == "order":
                    self.ws_private.order_stream(callback=self._handle_order)
                elif stream_type == "execution":
                    self.ws_private.execution_stream(callback=self._handle_execution)
                elif stream_type == "wallet":
                    self.ws_private.wallet_stream(callback=self._handle_wallet)
                    
        logger.info("WebSocket streams started")
        
    def stop(self):
        """Stop the WebSocket connections."""
        logger.info("Stopping Bybit WebSocket streams...")
        
        if self.ws_public:
            self.ws_public.exit()
            self.ws_public = None
            
        if self.ws_private:
            self.ws_private.exit()
            self.ws_private = None
            
        logger.info("WebSocket streams stopped")
            
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
        
    def __exit__(self, *args):
        """Context manager exit."""
        self.stop()


# Example usage functions
def example_orderbook_handler(data):
    """Example handler for order book updates."""
    print(f"Order book update for {data['symbol']}:")
    print(f"  Best bid: {data['bids'][0][0] if data['bids'] else 'N/A'}")
    print(f"  Best ask: {data['asks'][0][0] if data['asks'] else 'N/A'}")
    

def example_trade_handler(data):
    """Example handler for trade updates."""
    side = "BUY" if data['side'] == 1 else "SELL"
    print(f"Trade: {data['symbol']} {side} {data['vol']} @ {data['price']}")


if __name__ == "__main__":
    # Example usage
    stream = BybitWebSocketStream()
    
    # Subscribe to multiple data streams
    stream.subscribe_orderbook("BTCUSDT", depth=25, callback=example_orderbook_handler)
    stream.subscribe_trades("BTCUSDT", callback=example_trade_handler)
    
    try:
        # Start streaming
        stream.start()
        
        # Keep the connection alive
        while True:
            asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping stream...")
        stream.stop()