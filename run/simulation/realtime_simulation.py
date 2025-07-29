"""
Example demonstrating real-time data simulation using batch_size=1
"""

import polars as pl
import time
from dspy.hdb.registry import get_dataset


def simulate_realtime_feed():
    """Simulate real-time data feed with batch_size=1"""

    # Initialize the Tardis data loader
    tardis = get_dataset("tardis")

    # Define time range and product
    times = ["241201.000000", "241201.000500"]  # 5 seconds of data
    product = "BTCUSDT"

    print("=== Real-time Data Simulation ===")
    print(f"Simulating live feed for {product}")
    print("Press Ctrl+C to stop\n")

    tick_count = 0

    try:
        for batch in tardis.stream_book(product, times, depth=1, batch_size=1):
            tick_count += 1

            # Extract tick data
            if batch.height > 0:
                row = batch.row(0)
                ts = row[0]
                bid_price = row[4]  # bids[0].price
                ask_price = row[2]  # asks[0].price
                bid_size = row[5]  # bids[0].amount
                ask_size = row[3]  # asks[0].amount

                # Calculate mid price and spread
                mid_price = (bid_price + ask_price) / 2
                spread = ask_price - bid_price
                spread_bps = (spread / mid_price) * 10000

                # Format timestamp for display
                ts_ms = ts // 1_000_000
                ts_readable = (
                    pl.from_epoch([ts_ms], time_unit="ms")
                    .dt.strftime("%H:%M:%S.%3f")
                    .item()
                )

                # Print tick data (like a real-time feed)
                print(
                    f"[{ts_readable}] {product}: "
                    f"Bid={bid_price:.2f}({bid_size:.3f}) "
                    f"Ask={ask_price:.2f}({ask_size:.3f}) "
                    f"Mid={mid_price:.2f} "
                    f"Spread={spread_bps:.1f}bps"
                )

                # Simulate processing time (remove for max speed)
                time.sleep(0.01)  # 10ms delay

                # Limit output for demo
                if tick_count >= 50:
                    break

    except KeyboardInterrupt:
        print(f"\nStopped after {tick_count} ticks")
    except Exception as e:
        print(f"Error: {e}")

    print(f"\nProcessed {tick_count} ticks")


def simulate_with_feature_engineering():
    """Simulate real-time feed with feature engineering"""

    tardis = get_dataset("tardis")
    times = ["241201.000000", "241201.001000"]  # 10 seconds
    product = "BTCUSDT"

    print("\n=== Real-time with Feature Engineering ===")

    # Rolling window for features
    price_window = []
    window_size = 10

    tick_count = 0

    try:
        for batch in tardis.stream_book(product, times, depth=3, batch_size=1):
            tick_count += 1

            if batch.height > 0:
                # Calculate mid price
                mid_price = (
                    (
                        batch.select(pl.col("bids[0].price"))
                        + batch.select(pl.col("asks[0].price"))
                    )
                    / 2
                ).item()

                # Update rolling window
                price_window.append(mid_price)
                if len(price_window) > window_size:
                    price_window.pop(0)

                # Calculate features
                if len(price_window) >= 2:
                    price_change = price_window[-1] - price_window[-2]
                    price_change_bps = (price_change / price_window[-2]) * 10000

                    if len(price_window) >= window_size:
                        sma = sum(price_window) / len(price_window)
                        deviation = mid_price - sma

                        print(
                            f"Tick {tick_count:3d}: "
                            f"Price={mid_price:.2f} "
                            f"Change={price_change_bps:+.1f}bps "
                            f"SMA={sma:.2f} "
                            f"Dev={deviation:+.2f}"
                        )
                    else:
                        print(
                            f"Tick {tick_count:3d}: "
                            f"Price={mid_price:.2f} "
                            f"Change={price_change_bps:+.1f}bps "
                            f"(building window...)"
                        )

                # Limit output for demo
                if tick_count >= 30:
                    break

    except Exception as e:
        print(f"Error: {e}")

    print(f"\nProcessed {tick_count} ticks with features")


if __name__ == "__main__":
    simulate_realtime_feed()
    simulate_with_feature_engineering()
