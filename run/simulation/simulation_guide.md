# DSPy Simulation Framework Guide

The DSPy simulation framework provides a comprehensive backtesting environment that mimics the Bybit API functionality using historical data. This allows you to test trading strategies against real market data without risking capital.

## Overview

The simulation framework (`dspy.sim.simulation_engine`) implements the same interface as the Bybit API (`dspy.api.bybit.bybit_api`) but uses historical data from the Tardis data loader instead of live market data.

## Key Features

- **Identical API Interface**: Same methods as the real Bybit API
- **Historical Data Streaming**: Uses real market data from Tardis
- **Position Tracking**: Simulates positions, P&L, and account balance
- **Order Processing**: Supports market and limit orders
- **Fee Simulation**: Configurable maker/taker fees
- **Performance Metrics**: Track trades, P&L, and other statistics

## Quick Start

```python
from dspy.api.api_registry import get_api
from dspy.sim.simulation_engine import SimulationEngine  # Import to register

# Initialize simulation
sim = get_api(
    "simulation",
    symbols=["BTCUSDT"],
    times=["250120.000000", "250120.010000"],  # 10 minutes of data
    initial_balance=10000.0,
    maker_fee=0.0001,  # 0.01%
    taker_fee=0.0006   # 0.06%
)

# Use exactly like the real Bybit API
mid_price = sim.get_mid("BTCUSDT")
sim.place_order("BTCUSDT", 0.01, type="Market")
position = sim.get_positions(["BTCUSDT"])
```

## Initialization Parameters

- `symbols`: List of symbols to simulate (e.g., ["BTCUSDT", "ETHUSDT"])
- `times`: Time range in format `[start, end]` using `'YYMMDD.HHMMSS'`
- `initial_balance`: Starting wallet balance (default: 10000.0)
- `data_source`: Data source name (default: "tardis")
- `maker_fee`: Maker fee rate (default: 0.0001 = 0.01%)
- `taker_fee`: Taker fee rate (default: 0.0006 = 0.06%)
- `market`: Market name for data source (default: "binance-futures")
- `latency_config`: Configuration for realistic latency and slippage simulation

## Core Methods

### Market Data
- `get_mid(symbol)`: Get mid price
- `get_bid(symbol)`: Get best bid price and volume
- `get_ask(symbol)`: Get best ask price and volume
- `get_orderbook(symbol, depth)`: Get full orderbook
- `get_trades(symbol, limit)`: Get recent trades

### Trading
- `place_order(symbol, qty, price, type)`: Place market or limit order
- `cancel_order(symbol, order_id)`: Cancel specific order
- `cancel_all_orders(symbol)`: Cancel all orders for symbol
- `close_positions(symbols)`: Close all positions

### Account & Positions
- `get_wallet_balance()`: Get current balance
- `get_positions(symbols)`: Get position details
- `get_fees(symbol)`: Get trading fees
- `set_leverage(symbol, leverage)`: Set leverage

### Time Management
- `wait(seconds)`: Wait for specified time in simulation
- `wait_seconds(seconds)`: Wait for specified seconds (returns success status)
- `wait_minutes(minutes)`: Wait for specified minutes (returns success status)
- `get_current_time()`: Get current simulation timestamp

### History & Analysis
- `get_trade_history()`: Get execution history
- `get_filled_orders()`: Get filled orders
- `get_pnl()`: Get P&L records
- `get_simulation_stats()`: Get simulation statistics

## Latency Configuration

The simulation supports realistic latency and slippage simulation through the `LatencyConfig` class:

```python
from dspy.sim.simulation_engine import LatencyConfig

latency_config = LatencyConfig(
    # Order latency in milliseconds
    order_latency_ms=50.0,
    order_latency_std_ms=10.0,
    
    # Market data latency in milliseconds
    data_latency_ms=10.0,
    data_latency_std_ms=5.0,
    
    # Slippage configuration
    market_order_slippage_bps=1.0,  # 1 basis point slippage
    limit_order_fill_probability=0.95,  # 95% fill rate when price touches
    
    # Time simulation modes
    time_mode="instant",  # "instant", "realtime", or "fast"
    time_acceleration=10.0,  # For "fast" mode
)

sim = get_api("simulation", ..., latency_config=latency_config)
```

### Configuration Parameters

- `order_latency_ms`: Average order submission latency (default: 50ms)
- `order_latency_std_ms`: Standard deviation of order latency (default: 10ms)
- `data_latency_ms`: Market data latency (default: 10ms)
- `data_latency_std_ms`: Standard deviation of data latency (default: 5ms)
- `market_order_slippage_bps`: Market order slippage in basis points (default: 1.0)
- `limit_order_fill_probability`: Probability of limit order filling when price touches (default: 0.95)
- `time_mode`: Time simulation mode (default: "instant")
- `time_acceleration`: Speed multiplier for "fast" mode (default: 1.0)

### Time Simulation Modes

1. **"instant"**: No time delays, process data as fast as possible
2. **"realtime"**: Match real-time progression (1:1 time scaling)
3. **"fast"**: Accelerated time progression (configurable speed)

## Simulation Control

### Advancing Time
The simulation processes data sequentially. Use `next()` to advance to the next data point:

```python
# Process next data point
success = sim.next()
if not success:
    print("Simulation complete - no more data")
```

### Time Management
- `get_current_time()`: Get current simulation timestamp
- `wait_seconds(seconds)`: Wait for specific time period
- `wait_minutes(minutes)`: Wait for specific time period
- Orders are processed when `next()` is called
- Market orders execute immediately with realistic slippage
- Limit orders execute probabilistically when price conditions are met

## Example Strategy

```python
from dspy.api.api_registry import get_api
from dspy.sim.simulation_engine import SimulationEngine

# Initialize simulation
sim = get_api(
    "simulation",
    symbols=["BTCUSDT"],
    times=["250120.000000", "250120.010000"],
    initial_balance=10000.0
)

# Simple momentum strategy
previous_price = None
position_size = 0.01  # 0.01 BTC

for i in range(1000):  # Process 1000 data points
    if not sim.next():
        break
    
    current_price = sim.get_mid("BTCUSDT")
    
    if previous_price is not None:
        # Buy if price increased by 0.05%
        if current_price > previous_price * 1.0005:
            sim.place_order("BTCUSDT", position_size, type="Market")
            print(f"BUY at {current_price:.2f}")
        
        # Sell if price decreased by 0.05%
        elif current_price < previous_price * 0.9995:
            sim.place_order("BTCUSDT", -position_size, type="Market")
            print(f"SELL at {current_price:.2f}")
    
    previous_price = current_price

# Get results
stats = sim.get_simulation_stats()
print(f"Final P&L: ${stats['total_pnl']:.2f}")
print(f"Total trades: {stats['total_trades']}")
```

## Position Management

The simulation tracks positions with the same metrics as the real API:

```python
position = sim.get_positions(["BTCUSDT"])
print(f"Size: {position['size']:.6f} BTC")
print(f"Average entry price: ${position['aep']:.2f}")
print(f"Mark price: ${position['mark_price']:.2f}")
print(f"Unrealized P&L: ${position['unrealized_pnl']:.2f}")
print(f"Realized P&L: ${position['realized_pnl']:.2f}")
```

## Order Types

### Market Orders
Execute immediately at current market price:
```python
sim.place_order("BTCUSDT", 0.01, type="Market")  # Buy
sim.place_order("BTCUSDT", -0.01, type="Market")  # Sell
```

### Limit Orders
Execute when price reaches specified level:
```python
current_price = sim.get_mid("BTCUSDT")
sim.place_order("BTCUSDT", 0.01, price=current_price * 0.999, type="Limit")
```

## Fee Structure

The simulation applies realistic trading fees:
- **Maker Fee**: Applied to limit orders that add liquidity
- **Taker Fee**: Applied to market orders that remove liquidity
- Fees are deducted from wallet balance
- Fees are included in P&L calculations

## Performance Metrics

Get comprehensive simulation statistics:

```python
stats = sim.get_simulation_stats()
print(f"Current time: {stats['current_time']}")
print(f"Wallet balance: ${stats['wallet_balance']:.2f}")
print(f"Total P&L: ${stats['total_pnl']:.2f}")
print(f"Total trades: {stats['total_trades']}")
print(f"Open orders: {stats['open_orders']}")
```

## Data Requirements

The simulation requires historical data to be available:
- Data is loaded from the Tardis data source
- Ensure you have the required data files for your time range
- The simulation will attempt to download missing data automatically

## Best Practices

1. **Import Registration**: Always import `SimulationEngine` to register the API
2. **Time Management**: Use `next()` appropriately to advance simulation time
3. **Error Handling**: Handle cases where data is not available
4. **Resource Management**: Use appropriate time ranges to avoid memory issues
5. **Validation**: Test strategies on multiple time periods and symbols

## Limitations

- **Data Availability**: Limited by historical data availability
- **Execution Model**: Simplified execution model (no partial fills, slippage)
- **Latency**: No realistic latency simulation
- **Market Impact**: No market impact modeling
- **Funding Rates**: No funding rate simulation for perpetual futures

## Integration with DSPy

The simulation framework integrates seamlessly with other DSPy components:
- Use with feature engineering functions from `dspy.features`
- Compatible with position tracking from `dspy.positions`
- Works with data streaming from `dspy.hdb`

This simulation framework provides a powerful tool for backtesting trading strategies with realistic market conditions and fee structures.