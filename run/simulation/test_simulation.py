"""
Test script for the simulation framework.
"""

from dspy.api.api_registry import get_api
from dspy.sim.simulation_engine import SimulationEngine  # noqa: F401 # Import to register


def test_simulation():
    """Test basic simulation functionality."""
    print("Testing simulation framework...")

    # Initialize simulation
    sim = get_api(
        "simulation",
        symbols=["BTCUSDT"],
        times=["250120.000000", "250120.001000"],  # 1 minute of data
        initial_balance=10000.0,
        maker_fee=0.0001,
        taker_fee=0.0006,
    )

    print(f"✓ Simulation initialized with balance: ${sim.get_wallet_balance():.2f}")

    # Test market data
    try:
        mid_price = sim.get_mid("BTCUSDT")
        print(f"✓ Current mid price: ${mid_price:.2f}")

        bid = sim.get_bid("BTCUSDT")
        ask = sim.get_ask("BTCUSDT")
        print(f"✓ Bid: ${bid[0]:.2f} (size: {bid[1]:.4f})")
        print(f"✓ Ask: ${ask[0]:.2f} (size: {ask[1]:.4f})")

        orderbook = sim.get_orderbook("BTCUSDT", depth=5)
        print(
            f"✓ Orderbook has {len(orderbook['b'])} bids and {len(orderbook['a'])} asks"
        )

    except Exception as e:
        print(f"✗ Market data error: {e}")
        return False

    # Test trading
    try:
        # Place a buy order
        response = sim.place_order("BTCUSDT", 0.001, type="Market")
        print(f"✓ Buy order placed: {response['order_id']}")

        # Advance simulation to process the order
        sim.next()

        # Check position
        pos = sim.get_positions(["BTCUSDT"])
        print(f"✓ Position size: {pos['size']:.6f} BTC")
        print(f"✓ Average entry price: ${pos['aep']:.2f}")
        print(f"✓ Unrealized P&L: ${pos['unrealized_pnl']:.2f}")

        # Place a sell order to close
        response = sim.place_order("BTCUSDT", -0.001, type="Market")
        print(f"✓ Sell order placed: {response['order_id']}")

        # Advance simulation to process the order
        sim.next()

        # Check final position
        pos = sim.get_positions(["BTCUSDT"])
        print(f"✓ Final position size: {pos['size']:.6f} BTC")
        print(f"✓ Realized P&L: ${pos['realized_pnl']:.2f}")

    except Exception as e:
        print(f"✗ Trading error: {e}")
        return False

    # Test simulation stats
    try:
        stats = sim.get_simulation_stats()
        print(f"✓ Total trades: {stats['total_trades']}")
        print(f"✓ Final balance: ${stats['wallet_balance']:.2f}")
        print(f"✓ Total P&L: ${stats['total_pnl']:.2f}")

    except Exception as e:
        print(f"✗ Stats error: {e}")
        return False

    print("\n✅ All tests passed!")
    return True


if __name__ == "__main__":
    test_simulation()
