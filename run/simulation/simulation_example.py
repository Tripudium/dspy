"""
Example usage of the simulation framework.
"""

from dspy.api.api_registry import get_api
from dspy.sim.simulation_engine import SimulationEngine  # noqa: F401 # Import to register

# Initialize simulation
sim = get_api(
    "simulation",
    symbols=["BTCUSDT"],
    times=["250120.000000", "250120.010000"],  # 10 minutes of data
    initial_balance=10000.0,
    maker_fee=0.0001,
    taker_fee=0.0006,
)

print("Simulation initialized")
print(f"Initial balance: ${sim.get_wallet_balance():.2f}")
print()

# Run a simple strategy
previous_price = None
for i in range(100):  # Process 100 data points
    if not sim.next():
        break

    # Get current market data
    mid_price = sim.get_mid("BTCUSDT")

    # Simple strategy: buy when price drops, sell when price rises
    if previous_price is not None:
        if mid_price < previous_price * 0.999:  # Price dropped by 0.1%
            sim.place_order("BTCUSDT", 0.01, type="Market")  # Buy 0.01 BTC
            print(f"Step {i}: BUY at {mid_price:.2f}")
        elif mid_price > previous_price * 1.001:  # Price rose by 0.1%
            sim.place_order("BTCUSDT", -0.01, type="Market")  # Sell 0.01 BTC
            print(f"Step {i}: SELL at {mid_price:.2f}")

    previous_price = mid_price

# Get final results
stats = sim.get_simulation_stats()
positions = sim.get_positions(["BTCUSDT"])

print("\n" + "=" * 50)
print("SIMULATION RESULTS")
print("=" * 50)
print(f"Final balance: ${sim.get_wallet_balance():.2f}")
print(f"Total P&L: ${stats['total_pnl']:.2f}")
print(f"Total trades: {stats['total_trades']}")
print(f"Position size: {positions['size']:.4f} BTC")
print(f"Unrealized P&L: ${positions['unrealized_pnl']:.2f}")
print(f"Realized P&L: ${positions['realized_pnl']:.2f}")
print(f"Mark price: ${positions['mark_price']:.2f}")
