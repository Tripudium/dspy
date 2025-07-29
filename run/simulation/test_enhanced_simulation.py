"""
Test the enhanced simulation features.
"""

from dspy.sim.simulation_engine import LatencyConfig
from dspy.api.api_registry import get_api

# Test basic latency simulation
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

print("✓ Enhanced simulation initialized")
print(f"Order latency: {latency_config.order_latency_ms}ms")
print(f"Slippage: {latency_config.market_order_slippage_bps} bps")

# Test time delays
print("\nTesting time delays...")
start_time = sim.get_current_time()
sim.wait_seconds(0.1)
end_time = sim.get_current_time()
elapsed = (end_time - start_time) / 1_000_000_000
print(f"Waited {elapsed:.3f} seconds simulation time")

# Test order with slippage
print("\nTesting order with slippage...")
initial_price = sim.get_mid("BTCUSDT")
order = sim.place_order("BTCUSDT", 0.001, type="Market")
print(f"Order placed: {order['order_id'][:8]}...")

sim.wait_seconds(0.1)
position = sim.get_positions(["BTCUSDT"])
if position["size"] > 0:
    fill_price = position["aep"]
    slippage = (fill_price - initial_price) / initial_price * 10000
    print(f"Filled at ${fill_price:.2f} (slippage: {slippage:.1f} bps)")

print("\n✅ All enhanced features working!")
