"""
Example demonstrating time delays and realistic order execution in simulation.
"""

from dspy.api.api_registry import get_api
from dspy.sim.simulation_engine import SimulationEngine, LatencyConfig  # noqa: F401
import time

# Configure realistic latency and slippage
latency_config = LatencyConfig(
    # Order latency: 50ms average with 10ms standard deviation
    order_latency_ms=50.0,
    order_latency_std_ms=10.0,
    # Market data latency: 10ms average with 5ms standard deviation
    data_latency_ms=10.0,
    data_latency_std_ms=5.0,
    # Slippage: 1 basis point for market orders
    market_order_slippage_bps=1.0,
    # Limit orders: 95% chance of filling when price is touched
    limit_order_fill_probability=0.95,
    # Time mode: "instant" (default), "realtime", or "fast"
    time_mode="instant",  # Change to "realtime" for real-time simulation
    time_acceleration=10.0,  # For "fast" mode: 10x faster than real-time
)

# Initialize simulation with latency configuration
sim = get_api(
    "simulation",
    symbols=["BTCUSDT"],
    times=["250120.000000", "250120.001000"],  # 1 minute of data
    initial_balance=10000.0,
    latency_config=latency_config,
)

print("Simulation with Realistic Latency and Slippage")
print("=" * 50)
print(f"Initial balance: ${sim.get_wallet_balance():.2f}")
print(
    f"Order latency: {latency_config.order_latency_ms}ms ± {latency_config.order_latency_std_ms}ms"
)
print(f"Market order slippage: {latency_config.market_order_slippage_bps} bps")
print()

# Example 1: Market order with slippage
print("Example 1: Market Order with Slippage")
print("-" * 40)

# Get current price
initial_price = sim.get_mid("BTCUSDT")
print(f"Current mid price: ${initial_price:.2f}")

# Place market buy order
order = sim.place_order("BTCUSDT", 0.001, type="Market")
print(f"Placed market buy order: {order['order_id']}")

# Wait for order to execute (simulating latency)
sim.wait_seconds(0.1)  # Wait 100ms

# Check execution
position = sim.get_positions(["BTCUSDT"])
if position["size"] > 0:
    print("Order executed!")
    print(f"Fill price: ${position['aep']:.2f}")
    print(
        f"Slippage: ${position['aep'] - initial_price:.2f} ({(position['aep'] / initial_price - 1) * 10000:.1f} bps)"
    )
else:
    print("Order still pending...")

print()

# Example 2: Limit order with probabilistic fill
print("Example 2: Limit Order with Probabilistic Fill")
print("-" * 40)

# Place limit sell order at current price
current_price = sim.get_mid("BTCUSDT")
limit_price = current_price * 1.0001  # 1 bps above mid

print(f"Current mid price: ${current_price:.2f}")
print(f"Placing limit sell at: ${limit_price:.2f}")

order = sim.place_order("BTCUSDT", -0.001, price=limit_price, type="Limit")
print(f"Placed limit sell order: {order['order_id']}")

# Simulate price movements
fills = 0
attempts = 0

for i in range(20):
    sim.next()
    current_price = sim.get_mid("BTCUSDT")

    if current_price >= limit_price:
        attempts += 1
        print(
            f"Step {i}: Price touched limit (${current_price:.2f} >= ${limit_price:.2f})"
        )

        # Check if order filled
        position = sim.get_positions(["BTCUSDT"])
        if position["size"] == 0:
            fills += 1
            print(
                f"  -> Order filled! (Fill probability: {latency_config.limit_order_fill_probability * 100}%)"
            )
            break
        else:
            print("  -> Order not filled (probabilistic rejection)")

print(f"\nLimit order fill rate: {fills}/{attempts if attempts > 0 else 1} attempts")
print()

# Example 3: Time-based waiting
print("Example 3: Time-based Waiting")
print("-" * 40)

start_time = sim.get_current_time()
print(f"Starting at simulation time: {start_time}")

# Wait for 5 seconds of simulation time
print("Waiting for 5 seconds of simulation time...")
sim.wait_seconds(5)

end_time = sim.get_current_time()
elapsed_ns = end_time - start_time
elapsed_s = elapsed_ns / 1_000_000_000

print(f"Ending at simulation time: {end_time}")
print(f"Elapsed simulation time: {elapsed_s:.3f} seconds")
print()

# Example 4: Real-time simulation mode
print("Example 4: Real-time Simulation Mode")
print("-" * 40)

# Create a new simulation with real-time mode
realtime_config = LatencyConfig(
    order_latency_ms=50.0,
    order_latency_std_ms=10.0,
    market_order_slippage_bps=1.0,
    time_mode="realtime",  # Real-time simulation
)

rt_sim = get_api(
    "simulation",
    symbols=["BTCUSDT"],
    times=["250120.000000", "250120.000010"],  # 10 seconds of data
    initial_balance=10000.0,
    latency_config=realtime_config,
)

print("Running in real-time mode (1:1 time scaling)")
print("Processing 3 data points with actual time delays...")

start_real_time = time.time()

for i in range(3):
    rt_sim.next()
    current_price = rt_sim.get_mid("BTCUSDT")
    print(f"Step {i}: Price = ${current_price:.2f}")

end_real_time = time.time()
real_elapsed = end_real_time - start_real_time

print(f"\nReal time elapsed: {real_elapsed:.3f} seconds")
print("(In real-time mode, simulation time matches wall clock time)")

print("\n" + "=" * 50)
print("SIMULATION COMPLETE")
