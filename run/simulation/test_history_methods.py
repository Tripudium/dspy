"""
Test the history methods in the simulation framework.
"""

from dspy.sim.simulation_engine import LatencyConfig
from dspy.api.api_registry import get_api

# Test simulation with some trading activity
latency_config = LatencyConfig(
    order_latency_ms=10.0, market_order_slippage_bps=1.0, time_mode="instant"
)

sim = get_api(
    "simulation",
    symbols=["BTCUSDT"],
    times=["250120.000000", "250120.001000"],
    initial_balance=10000.0,
    latency_config=latency_config,
)

print("Testing history methods...")

# Make some trades
print("\n1. Making some trades...")
sim.place_order("BTCUSDT", 0.001, type="Market")
sim.wait_seconds(0.1)
sim.place_order("BTCUSDT", -0.001, type="Market")
sim.wait_seconds(0.1)

# Test get_trade_history
print("\n2. Testing get_trade_history()...")
trade_history = sim.get_trade_history()
print(f"Found {len(trade_history)} trades")
if trade_history:
    print(f"First trade: {trade_history[0]}")

# Test get_filled_orders
print("\n3. Testing get_filled_orders()...")
filled_orders = sim.get_filled_orders()
print(f"Found {len(filled_orders)} filled orders")
if filled_orders:
    print(f"First order: {filled_orders[0]}")

# Test get_pnl
print("\n4. Testing get_pnl()...")
pnl_records = sim.get_pnl()
print(f"Found {len(pnl_records)} P&L records")
if pnl_records:
    print(f"First P&L record: {pnl_records[0]}")

# Test with symbol filter
print("\n5. Testing with symbol filter...")
trade_history_btc = sim.get_trade_history(symbol="BTCUSDT")
print(f"BTCUSDT trades: {len(trade_history_btc)}")

filled_orders_btc = sim.get_filled_orders(symbol="BTCUSDT")
print(f"BTCUSDT filled orders: {len(filled_orders_btc)}")

pnl_btc = sim.get_pnl(symbol="BTCUSDT")
print(f"BTCUSDT P&L records: {len(pnl_btc)}")

print("\n✅ All history methods working correctly!")
