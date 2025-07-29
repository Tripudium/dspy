"""
Realistic trading simulation with latency, slippage, and time delays.
"""

from dspy.api.api_registry import get_api
from dspy.sim.simulation_engine import SimulationEngine, LatencyConfig  # noqa: F401
import numpy as np

# Configure realistic trading conditions
latency_config = LatencyConfig(
    order_latency_ms=30.0,  # 30ms order latency
    order_latency_std_ms=15.0,  # High variability
    data_latency_ms=5.0,  # 5ms data latency
    data_latency_std_ms=2.0,
    market_order_slippage_bps=2.0,  # 2 bps slippage
    limit_order_fill_probability=0.8,  # 80% fill rate at touch
    time_mode="fast",  # 10x faster than real-time
    time_acceleration=10.0,
)

# Initialize simulation
sim = get_api(
    "simulation",
    symbols=["BTCUSDT", "ETHUSDT"],
    times=["250120.000000", "250120.010000"],  # 10 minutes
    initial_balance=50000.0,
    latency_config=latency_config,
)

print("Realistic Multi-Asset Trading Simulation")
print("=" * 50)
print(f"Initial balance: ${sim.get_wallet_balance():,.2f}")
print(f"Simulating with {latency_config.order_latency_ms}ms order latency")
print(f"Market order slippage: {latency_config.market_order_slippage_bps} bps")
print()

# Trading parameters
position_size_btc = 0.1
position_size_eth = 1.0
stop_loss_pct = 0.002  # 0.2% stop loss
take_profit_pct = 0.003  # 0.3% take profit

# Track trades
trades = []
pending_stops = {}
pending_profits = {}

# Strategy: Mean reversion with protective stops
print("Strategy: Mean reversion with stop-loss and take-profit orders")
print("-" * 50)

# Price tracking
price_history = {"BTCUSDT": [], "ETHUSDT": []}
ma_period = 20

for i in range(300):  # Process 300 ticks
    if not sim.next():
        break

    # Update price history
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        price = sim.get_mid(symbol)
        price_history[symbol].append(price)

    # Skip until we have enough data for MA
    if len(price_history["BTCUSDT"]) < ma_period:
        continue

    # Calculate signals every 10 ticks
    if i % 10 == 0:
        for symbol in ["BTCUSDT", "ETHUSDT"]:
            prices = np.array(price_history[symbol][-ma_period:])
            ma = np.mean(prices)
            current_price = prices[-1]
            deviation = (current_price - ma) / ma

            position = sim.get_positions([symbol])
            pos_size = position_size_btc if symbol == "BTCUSDT" else position_size_eth

            # Entry signals
            if abs(position["size"]) < 0.0001:  # No position
                if deviation < -0.001:  # Price 0.1% below MA - buy signal
                    # Place market buy with simulated latency
                    order = sim.place_order(symbol, pos_size, type="Market")

                    print(f"\n[{i}] {symbol} BUY SIGNAL")
                    print(f"  Price: ${current_price:.2f} (MA: ${ma:.2f})")
                    print(f"  Deviation: {deviation * 100:.2f}%")
                    print(f"  Order ID: {order['order_id'][:8]}...")

                    # Wait for execution before placing stops
                    sim.wait_seconds(0.1)  # 100ms wait

                    # Check if filled
                    position = sim.get_positions([symbol])
                    if position["size"] > 0:
                        entry_price = position["aep"]
                        actual_slippage = (
                            (entry_price - current_price) / current_price * 10000
                        )
                        print(
                            f"  Filled at: ${entry_price:.2f} (slippage: {actual_slippage:.1f} bps)"
                        )

                        # Place stop loss and take profit
                        stop_price = entry_price * (1 - stop_loss_pct)
                        profit_price = entry_price * (1 + take_profit_pct)

                        stop_order = sim.place_order(
                            symbol, -pos_size, price=stop_price, type="Limit"
                        )
                        profit_order = sim.place_order(
                            symbol, -pos_size, price=profit_price, type="Limit"
                        )

                        pending_stops[symbol] = stop_order["order_id"]
                        pending_profits[symbol] = profit_order["order_id"]

                        print(f"  Stop Loss at: ${stop_price:.2f}")
                        print(f"  Take Profit at: ${profit_price:.2f}")

                        trades.append(
                            {
                                "symbol": symbol,
                                "side": "BUY",
                                "price": entry_price,
                                "size": pos_size,
                                "time": i,
                            }
                        )

                elif deviation > 0.001:  # Price 0.1% above MA - sell signal
                    # Place market sell
                    order = sim.place_order(symbol, -pos_size, type="Market")

                    print(f"\n[{i}] {symbol} SELL SIGNAL")
                    print(f"  Price: ${current_price:.2f} (MA: ${ma:.2f})")
                    print(f"  Deviation: {deviation * 100:.2f}%")
                    print(f"  Order ID: {order['order_id'][:8]}...")

                    # Wait and check execution
                    sim.wait_seconds(0.1)

                    position = sim.get_positions([symbol])
                    if position["size"] < 0:
                        entry_price = position["aep"]
                        actual_slippage = (
                            (current_price - entry_price) / current_price * 10000
                        )
                        print(
                            f"  Filled at: ${entry_price:.2f} (slippage: {actual_slippage:.1f} bps)"
                        )

                        # Place protective orders
                        stop_price = entry_price * (1 + stop_loss_pct)
                        profit_price = entry_price * (1 - take_profit_pct)

                        stop_order = sim.place_order(
                            symbol, pos_size, price=stop_price, type="Limit"
                        )
                        profit_order = sim.place_order(
                            symbol, pos_size, price=profit_price, type="Limit"
                        )

                        pending_stops[symbol] = stop_order["order_id"]
                        pending_profits[symbol] = profit_order["order_id"]

                        print(f"  Stop Loss at: ${stop_price:.2f}")
                        print(f"  Take Profit at: ${profit_price:.2f}")

                        trades.append(
                            {
                                "symbol": symbol,
                                "side": "SELL",
                                "price": entry_price,
                                "size": pos_size,
                                "time": i,
                            }
                        )

    # Simulate waiting between checks (1 second simulation time)
    if i % 10 == 9:
        sim.wait_seconds(1)

# Close any remaining positions
print("\n\nClosing remaining positions...")
for symbol in ["BTCUSDT", "ETHUSDT"]:
    position = sim.get_positions([symbol])
    if abs(position["size"]) > 0.0001:
        # Cancel pending orders
        if symbol in pending_stops:
            sim.cancel_order(symbol, pending_stops[symbol])
        if symbol in pending_profits:
            sim.cancel_order(symbol, pending_profits[symbol])

        # Close position
        sim.close_positions([symbol])
        print(f"Closed {symbol} position")

# Wait for final executions
sim.wait_seconds(0.2)

# Calculate results
print("\n" + "=" * 50)
print("TRADING RESULTS")
print("=" * 50)

# Get final statistics
stats = sim.get_simulation_stats()
final_balance = sim.get_wallet_balance()

print("\nAccount Summary:")
print(f"Initial balance: ${50000.00:,.2f}")
print(f"Final balance: ${final_balance:,.2f}")
print(
    f"Net P&L: ${final_balance - 50000.00:+,.2f} ({(final_balance / 50000 - 1) * 100:+.2f}%)"
)
print(f"Total trades: {len(trades)}")

# Trade analysis
if trades:
    print("\nTrade Analysis:")
    btc_trades = [t for t in trades if t["symbol"] == "BTCUSDT"]
    eth_trades = [t for t in trades if t["symbol"] == "ETHUSDT"]
    print(f"BTC trades: {len(btc_trades)}")
    print(f"ETH trades: {len(eth_trades)}")

    # Get execution history for slippage analysis
    executions = sim.get_trade_history(limit=100)
    if executions:
        slippages = []
        for exec in executions:
            if exec["order_type"] == "Market":
                # Calculate slippage (this is simplified)
                slippages.append(abs(exec["fee_rate"]) * 10000)

        if slippages:
            print("\nExecution Quality:")
            print(f"Average slippage: {np.mean(slippages):.1f} bps")
            print(f"Max slippage: {np.max(slippages):.1f} bps")

print("\nSimulation complete!")
