"""
Simulation engine that mimics Bybit API functionality using historical data.
"""

import logging
from typing import Dict, List, Optional, Literal
import uuid
import numpy as np
from dataclasses import dataclass
import random
import polars as pl

from dspy.api.base import Exchange
from dspy.api.api_registry import register_api
from dspy.hdb.registry import get_dataset

logger = logging.getLogger(__name__)


@dataclass
class LatencyConfig:
    """Configuration for simulating realistic latency and slippage."""

    # Order latency in milliseconds
    order_latency_ms: float = 50.0
    order_latency_std_ms: float = 10.0

    # Market data latency in milliseconds
    data_latency_ms: float = 10.0
    data_latency_std_ms: float = 5.0

    # Slippage configuration
    market_order_slippage_bps: float = 1.0  # basis points
    limit_order_fill_probability: float = (
        0.95  # probability of limit order filling at touch
    )

    # Time simulation mode (kept for compatibility, but no real-time waiting)
    time_mode: Literal["realtime", "fast", "instant"] = "instant"
    time_acceleration: float = 1.0  # For "fast" mode, how much faster than real-time


class SimulationOrder:
    """Represents a simulated order."""

    def __init__(
        self,
        order_id: str,
        symbol: str,
        side: str,
        qty: float,
        price: float,
        order_type: str,
        timestamp: int,
        submission_time: int,
        execution_time: Optional[int] = None,
    ):
        self.order_id = order_id
        self.symbol = symbol
        self.side = side
        self.qty = qty
        self.price = price
        self.order_type = order_type
        self.timestamp = timestamp
        self.submission_time = submission_time  # When order was submitted (with latency)
        self.execution_time = execution_time  # When order should be processed
        self.status = "New"
        self.filled_qty = 0.0
        self.avg_price = 0.0
        self.executions = []


class SimulationPosition:
    """Represents a simulated position."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.size = 0.0
        self.avg_price = 0.0
        self.mark_price = 0.0
        self.unrealized_pnl = 0.0
        self.realized_pnl = 0.0
        self.leverage = 1.0

    def update_mark_price(self, price: float):
        """Update mark price and unrealized PnL."""
        self.mark_price = price
        if self.size != 0:
            self.unrealized_pnl = self.size * (price - self.avg_price)

    def add_trade(self, qty: float, price: float, fee: float = 0.0):
        """Add a trade to the position."""
        if self.size == 0:
            # Opening position
            self.size = qty
            self.avg_price = price
        elif (self.size > 0 and qty > 0) or (self.size < 0 and qty < 0):
            # Increasing position
            total_value = self.size * self.avg_price + qty * price
            self.size += qty
            self.avg_price = total_value / self.size
        else:
            # Reducing or closing position
            if abs(qty) >= abs(self.size):
                # Closing position
                realized_pnl = self.size * (price - self.avg_price) - fee
                self.realized_pnl += realized_pnl
                self.size = qty + self.size if abs(qty) > abs(self.size) else 0.0
                if self.size != 0:
                    self.avg_price = price
            else:
                # Reducing position
                realized_pnl = (-qty) * (price - self.avg_price) - fee
                self.realized_pnl += realized_pnl
                self.size += qty


@register_api("simulation")
class SimulationEngine(Exchange):
    """
    Simulation engine that mimics Bybit API functionality using historical data.
    """

    def __init__(
        self,
        symbols: List[str],
        times: List[str],
        initial_balance: float = 10000.0,
        data_source: str = "tardis",
        maker_fee: float = 0.0001,
        taker_fee: float = 0.0006,
        market: str = "binance-futures",
        latency_config: Optional[LatencyConfig] = None,
    ):
        """
        Initialize the simulation engine.

        Args:
            symbols: List of symbols to simulate
            times: Time range [start, end] in format '%y%m%d.%H%M%S'
            initial_balance: Initial wallet balance
            data_source: Data source name (default: "tardis")
            maker_fee: Maker fee rate (default: 0.01%)
            taker_fee: Taker fee rate (default: 0.06%)
            market: Market name for data source
            latency_config: Configuration for latency and slippage simulation
        """
        super().__init__()

        self.symbols = symbols
        self.times = times
        self.initial_balance = initial_balance
        self.wallet_balance = initial_balance
        self.maker_fee = maker_fee
        self.taker_fee = taker_fee
        self.latency_config = latency_config or LatencyConfig()

        # Initialize data source
        self.data_source = get_dataset(data_source, market=market)

        # Current simulation state
        self.current_time = 0
        self.current_data = {}
        self.positions = {symbol: SimulationPosition(symbol) for symbol in symbols}
        self.orders = {}
        self.pending_orders = []  # Orders waiting for latency
        self.order_history = []
        self.trade_history = []
        self.execution_history = []

        # Data storage for efficient jumping
        self.data_frames = {}  # Store full DataFrames for each symbol
        self.current_indices = {}  # Current position in each DataFrame
        
        # Initialize data
        self._load_all_data()

        # Advance to first data point
        self.next()

    def _load_all_data(self):
        """Load all data for efficient jumping."""
        logger.info("Loading all data for simulation...")
        
        for symbol in self.symbols:
            # Load full dataset for this symbol
            df = self.data_source.load_book(symbol, self.times, depth=25)
            
            if df is not None and len(df) > 0:
                # Sort by timestamp to ensure proper ordering
                df = df.sort('ts')
                self.data_frames[symbol] = df
                self.current_indices[symbol] = 0
                logger.info(f"Loaded {len(df)} data points for {symbol}")
            else:
                logger.warning(f"No data loaded for {symbol}")
                self.data_frames[symbol] = None
                self.current_indices[symbol] = 0

    def _simulate_latency(self, base_ms: float, std_ms: float) -> int:
        """Generate realistic latency in nanoseconds."""
        latency_ms = max(0, np.random.normal(base_ms, std_ms))
        return int(latency_ms * 1_000_000)  # Convert to nanoseconds

    def _apply_slippage(self, price: float, side: str, symbol: str) -> float:
        """Apply realistic slippage to execution price."""
        # Symbol parameter kept for future extensibility (symbol-specific slippage)
        slippage_factor = self.latency_config.market_order_slippage_bps / 10_000

        if side == "Buy":
            # Buying - price moves against us (higher)
            return price * (1 + slippage_factor)
        else:
            # Selling - price moves against us (lower)
            return price * (1 - slippage_factor)

    def _should_fill_limit_order(self) -> bool:
        """Determine if a limit order should fill when price is touched."""
        return random.random() < self.latency_config.limit_order_fill_probability

    def _get_next_data_point(self, symbol: str) -> Optional[Dict]:
        """Get next data point for a symbol."""
        if symbol not in self.data_frames or self.data_frames[symbol] is None:
            return None
        
        df = self.data_frames[symbol]
        current_idx = self.current_indices[symbol]
        
        if current_idx >= len(df):
            return None
        
        # Get current row as dict
        row = df.row(current_idx, named=True)
        self.current_indices[symbol] += 1
        
        return row

    def next(self, target_time: Optional[int] = None) -> bool:
        """
        Advance simulation to next data point or specified time.

        Args:
            target_time: Optional target time in nanoseconds to advance to

        Returns:
            True if successful, False if no more data
        """
        if target_time is not None:
            # Jump to specific time using efficient data filtering
            return self._jump_to_time(target_time)
        
        # Regular single-step advancement
        min_time = None
        next_data = {}

        # Get next data point for each symbol
        for symbol in self.symbols:
            data = self._get_next_data_point(symbol)
            if data is not None:
                next_data[symbol] = data
                if min_time is None or data["ts"] < min_time:
                    min_time = data["ts"]

        # If no data available, simulation is complete
        if not next_data:
            return False

        # Update current state
        self._update_state(min_time, next_data)
        return True

    def _jump_to_time(self, target_time: int) -> bool:
        """
        Jump to a specific time using efficient Polars operations.
        
        Args:
            target_time: Target time in nanoseconds
            
        Returns:
            True if successful, False if no more data
        """
        # For each symbol, advance to the target time
        advanced_any = False
        
        for symbol in self.symbols:
            if symbol not in self.data_frames or self.data_frames[symbol] is None:
                continue
                
            df = self.data_frames[symbol]
            current_idx = self.current_indices[symbol]
            
            if current_idx >= len(df):
                continue
            
            # Use Polars to efficiently find the next data point at or after target_time
            remaining_df = df.slice(current_idx)
            
            # Find the first row with timestamp >= target_time
            mask = remaining_df['ts'] >= target_time
            if mask.any():
                # Get the index of the first matching row
                match_idx = mask.arg_max()  # arg_max gives first True index
                
                # Update the current index
                self.current_indices[symbol] = current_idx + match_idx
                advanced_any = True
        
        if not advanced_any:
            return False
        
        # Now get the next data point normally
        return self.next()

    def _update_state(self, timestamp: int, data: Dict):
        """Update simulation state with new data."""
        # Update current time and data
        self.current_time = timestamp
        self.current_data = data

        # Update mark prices for all positions
        for symbol, symbol_data in data.items():
            if symbol in self.positions:
                mid_price = (
                    symbol_data["bids[0].price"] + symbol_data["asks[0].price"]
                ) / 2
                self.positions[symbol].update_mark_price(mid_price)

        # Process pending orders that are ready
        self._process_pending_orders()

        # Process any active orders
        self._process_orders()

    def _process_pending_orders(self):
        """Process orders that were submitted with latency."""
        ready_orders = []

        for order in self.pending_orders:
            if order.execution_time <= self.current_time:
                # Order is ready to be processed
                self.orders[order.order_id] = order
                ready_orders.append(order)

        # Remove processed orders from pending list
        for order in ready_orders:
            self.pending_orders.remove(order)

    def _process_orders(self):
        """Process pending orders against current market data."""
        executed_orders = []

        for order_id, order in self.orders.items():
            if order.symbol not in self.current_data:
                continue

            data = self.current_data[order.symbol]
            executed = False

            if order.order_type == "Market":
                # Market order - execute immediately with slippage
                if order.side == "Buy":
                    base_price = data["asks[0].price"]
                else:
                    base_price = data["bids[0].price"]

                # Apply slippage
                fill_price = self._apply_slippage(base_price, order.side, order.symbol)
                self._execute_order(order, order.qty, fill_price)
                executed = True

            elif order.order_type == "Limit":
                # Limit order - check if price is touched and should fill
                if order.side == "Buy" and data["asks[0].price"] <= order.price:
                    if self._should_fill_limit_order():
                        fill_price = min(order.price, data["asks[0].price"])
                        self._execute_order(order, order.qty, fill_price)
                        executed = True
                elif order.side == "Sell" and data["bids[0].price"] >= order.price:
                    if self._should_fill_limit_order():
                        fill_price = max(order.price, data["bids[0].price"])
                        self._execute_order(order, order.qty, fill_price)
                        executed = True

            if executed:
                executed_orders.append(order_id)

        # Remove executed orders
        for order_id in executed_orders:
            del self.orders[order_id]

    def _execute_order(self, order: SimulationOrder, qty: float, price: float):
        """Execute an order."""
        # Calculate fee
        fee_rate = self.maker_fee if order.order_type == "Limit" else self.taker_fee
        fee = abs(qty) * price * fee_rate

        # Update wallet balance
        self.wallet_balance -= fee

        # Update position
        signed_qty = qty if order.side == "Buy" else -qty
        self.positions[order.symbol].add_trade(signed_qty, price, fee)

        # Update order
        order.status = "Filled"
        order.filled_qty = qty
        order.avg_price = price

        # Record execution
        execution = {
            "order_id": order.order_id,
            "symbol": order.symbol,
            "side": order.side,
            "price": price,
            "qty": qty,
            "exec_type": "Trade",
            "exec_value": abs(qty) * price,
            "exec_fee": fee,
            "fee_rate": fee_rate,
            "exec_time": self.current_time,
            "order_type": order.order_type,
            "order_price": order.price,
        }

        self.execution_history.append(execution)
        self.order_history.append(order)

    # Market data methods
    def get_mid(self, symbol: str) -> float:
        """Return best mid price."""
        if symbol not in self.current_data:
            raise ValueError(f"No data available for symbol {symbol}")

        data = self.current_data[symbol]
        return (data["bids[0].price"] + data["asks[0].price"]) / 2

    def get_orderbook(self, symbol: str, depth: int = 25) -> dict:
        """Return orderbook for product."""
        if symbol not in self.current_data:
            raise ValueError(f"No data available for symbol {symbol}")

        data = self.current_data[symbol]

        # Extract bids and asks up to specified depth
        bids = []
        asks = []

        for i in range(min(depth, 25)):
            bid_price_key = f"bids[{i}].price"
            bid_amount_key = f"bids[{i}].amount"
            ask_price_key = f"asks[{i}].price"
            ask_amount_key = f"asks[{i}].amount"

            if bid_price_key in data and data[bid_price_key] is not None:
                bids.append([data[bid_price_key], data[bid_amount_key]])
            if ask_price_key in data and data[ask_price_key] is not None:
                asks.append([data[ask_price_key], data[ask_amount_key]])

        return {
            "b": np.array(bids, dtype=float),
            "a": np.array(asks, dtype=float),
            "ts": data["ts"],
            "cts": data.get("ts_local", data["ts"]),
        }

    def get_ask(self, symbol: str, depth: int = 1) -> List[float]:
        """Return best ask price and volume."""
        # Parameters kept for API compatibility
        orderbook = self.get_orderbook(symbol, depth=depth)
        if len(orderbook["a"]) > 0:
            return [float(orderbook["a"][0][0]), float(orderbook["a"][0][1])]
        return [0.0, 0.0]

    def get_bid(self, symbol: str, depth: int = 1) -> List[float]:
        """Return best bid price and volume."""
        # Parameters kept for API compatibility
        orderbook = self.get_orderbook(symbol, depth=depth)
        if len(orderbook["b"]) > 0:
            return [float(orderbook["b"][0][0]), float(orderbook["b"][0][1])]
        return [0.0, 0.0]

    def get_trades(self, symbol: str, limit: int = 100) -> List[dict]:
        """Return recent trades (simulated from executions)."""
        trades = []
        for exec in self.execution_history[-limit:]:
            if exec["symbol"] == symbol:
                trades.append(
                    {
                        "ts": exec["exec_time"],
                        "price": exec["price"],
                        "qty": exec["qty"],
                        "side": 1 if exec["side"] == "Buy" else -1,
                    }
                )
        return trades

    def get_latency(self, symbol: str, depth: int = 1) -> float:
        """Return simulated latency (always 0)."""
        # Parameters are kept for API compatibility but not used in simulation
        return 0.0

    # Account info methods
    def get_wallet_balance(self) -> float:
        """Return current wallet balance."""
        return self.wallet_balance

    def get_fees(self, symbol: str) -> List[float]:
        """Return taker and maker fees."""
        # Symbol parameter kept for API compatibility but not used in simulation
        return [self.taker_fee, self.maker_fee]

    # Position methods
    def get_positions(self, symbols: List[str]) -> dict:
        """Return positions for specified symbols."""
        positions = {}

        for symbol in symbols:
            if symbol not in self.positions:
                continue

            pos = self.positions[symbol]
            positions[symbol] = {
                "size": pos.size,
                "aep": pos.avg_price,
                "mark_price": pos.mark_price,
                "value": abs(pos.size) * pos.mark_price,
                "leverage": pos.leverage,
                "position_balance": abs(pos.size) * pos.avg_price,
                "unrealized_pnl": pos.unrealized_pnl,
                "realized_pnl": pos.realized_pnl,
            }

        if len(symbols) == 1:
            return positions[symbols[0]]
        return positions

    # Trading methods
    def place_order(
        self,
        symbol: str,
        qty: float,
        price: Optional[float] = None,
        type: str = "Market",
    ) -> dict:
        """Place a simulated order."""
        if symbol not in self.symbols:
            raise ValueError(f"Symbol {symbol} not supported in simulation")

        order_id = str(uuid.uuid4())
        side = "Buy" if qty > 0 else "Sell"

        if price is None:
            price = 0.0

        # Simulate order submission latency
        order_latency = self._simulate_latency(
            self.latency_config.order_latency_ms,
            self.latency_config.order_latency_std_ms,
        )

        submission_time = self.current_time
        execution_time = self.current_time + order_latency

        order = SimulationOrder(
            order_id=order_id,
            symbol=symbol,
            side=side,
            qty=abs(qty),
            price=price,
            order_type=type,
            timestamp=submission_time,
            submission_time=submission_time,
            execution_time=execution_time,
        )

        # Add to pending orders to simulate latency
        self.pending_orders.append(order)

        return {
            "order_id": order_id,
            "ret_code": 0,
            "time": self.current_time,
        }

    def cancel_order(self, symbol: str, order_id: str) -> int:
        """Cancel a specific order."""
        # Symbol parameter kept for API compatibility but not used in simulation
        if order_id in self.orders:
            del self.orders[order_id]
            return 0
        return 1

    def cancel_all_orders(self, symbol: str) -> int:
        """Cancel all orders for a symbol."""
        orders_to_cancel = [
            oid for oid, order in self.orders.items() if order.symbol == symbol
        ]

        for order_id in orders_to_cancel:
            del self.orders[order_id]

        return 0

    def close_positions(self, symbols: List[str]) -> dict:
        """Close positions for specified symbols."""
        responses = {}

        for symbol in symbols:
            if symbol in self.positions and self.positions[symbol].size != 0:
                pos = self.positions[symbol]
                qty = -pos.size  # Opposite sign to close

                response = self.place_order(symbol, qty, type="Market")
                responses[symbol] = response["ret_code"]
            else:
                responses[symbol] = None

        return responses

    # History methods
    def get_trade_history(
        self,
        symbol: str = None,
        limit: int = 50,
        start_time: int = None,
        end_time: int = None,
    ) -> List[dict]:
        """Get simulated trade execution history."""
        # start_time and end_time parameters kept for API compatibility but not used in simulation
        trades = []

        for exec in self.execution_history[-limit:]:
            if symbol and exec["symbol"] != symbol:
                continue
            if start_time and exec["exec_time"] < start_time:
                continue
            if end_time and exec["exec_time"] > end_time:
                continue

            trades.append(exec)

        return trades

    def get_filled_orders(
        self, symbol: str = None, limit: int = 50, order_filter: str = "Filled"
    ) -> List[dict]:
        """Get filled orders."""
        # order_filter parameter kept for API compatibility but not used in simulation
        orders = []

        for order in self.order_history[-limit:]:
            if symbol and order.symbol != symbol:
                continue
            if order.status != "Filled":
                continue

            orders.append(
                {
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "side": order.side,
                    "order_type": order.order_type,
                    "price": order.price,
                    "qty": order.qty,
                    "avg_price": order.avg_price,
                    "cum_exec_qty": order.filled_qty,
                    "cum_exec_value": order.filled_qty * order.avg_price,
                    "order_status": order.status,
                    "created_time": order.timestamp,
                    "updated_time": order.timestamp,
                }
            )

        return orders

    def get_pnl(
        self,
        symbol: str = None,
        limit: int = 50,
        start_time: int = None,
        end_time: int = None,
    ) -> List[dict]:
        """Get P&L records (simplified)."""
        # start_time and end_time parameters kept for API compatibility but not used in simulation
        pnl_records = []

        for symbol_name, pos in self.positions.items():
            if symbol and symbol_name != symbol:
                continue

            pnl_records.append(
                {
                    "symbol": symbol_name,
                    "closed_pnl": pos.realized_pnl,
                    "unrealized_pnl": pos.unrealized_pnl,
                    "created_time": self.current_time,
                    "updated_time": self.current_time,
                }
            )

        return pnl_records[-limit:]

    # Utility methods
    def set_leverage(self, symbol: str, leverage: int) -> dict:
        """Set leverage (simulated)."""
        if symbol in self.positions:
            self.positions[symbol].leverage = leverage
        return {"ret_code": 0}

    def wait(self, timeout: float):
        """Wait for specified time in simulation by jumping forward in data."""
        # Convert timeout from seconds to nanoseconds
        wait_ns = int(timeout * 1_000_000_000)
        target_time = self.current_time + wait_ns
        
        # Jump to target time using efficient data operations
        self.next(target_time)

    def wait_seconds(self, seconds: float) -> bool:
        """
        Wait for specified number of seconds in simulation time by jumping forward.
        
        Args:
            seconds: Number of seconds to wait
            
        Returns:
            True if successful, False if simulation ended
        """
        # Convert to nanoseconds and jump forward
        wait_ns = int(seconds * 1_000_000_000)
        target_time = self.current_time + wait_ns
        
        return self.next(target_time)

    def wait_minutes(self, minutes: float) -> bool:
        """
        Wait for specified number of minutes in simulation time by jumping forward.
        
        Args:
            minutes: Number of minutes to wait
            
        Returns:
            True if successful, False if simulation ended
        """
        return self.wait_seconds(minutes * 60)

    def get_current_time(self) -> int:
        """Get current simulation time."""
        return self.current_time

    def get_simulation_stats(self) -> dict:
        """Get simulation statistics."""
        total_pnl = sum(
            pos.realized_pnl + pos.unrealized_pnl for pos in self.positions.values()
        )

        return {
            "current_time": self.current_time,
            "wallet_balance": self.wallet_balance,
            "total_pnl": total_pnl,
            "total_trades": len(self.execution_history),
            "open_orders": len(self.orders),
            "positions": {
                symbol: {
                    "size": pos.size,
                    "unrealized_pnl": pos.unrealized_pnl,
                    "realized_pnl": pos.realized_pnl,
                }
                for symbol, pos in self.positions.items()
                if pos.size != 0
            },
        }
