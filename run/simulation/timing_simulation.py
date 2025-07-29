"""
Examples of timing control in streaming simulations
"""

import polars as pl
import time
from dspy.hdb.registry import get_dataset


def simulate_with_processing_delay():
    """Simulate real-time with processing delays using time.sleep()"""

    tardis = get_dataset("tardis")
    times = ["241201.000000", "241201.001000"]  # 10 seconds of data
    product = "BTCUSDT"

    print("=== Processing Delay Simulation ===")
    print("Simulating 1ms processing time per tick\n")

    start_time = time.time()
    tick_count = 0

    for batch in tardis.stream_book(product, times, depth=1, batch_size=1):
        tick_count += 1

        if batch.height > 0:
            # Extract data
            mid_price = (
                (
                    batch.select(pl.col("bids[0].price"))
                    + batch.select(pl.col("asks[0].price"))
                )
                / 2
            ).item()

            # Simulate processing time (e.g., strategy computation)
            time.sleep(0.001)  # 1ms processing delay

            if tick_count % 10 == 0:
                elapsed = time.time() - start_time
                print(
                    f"Tick {tick_count}: Price={mid_price:.2f} "
                    f"(Elapsed: {elapsed:.2f}s)"
                )

            if tick_count >= 50:
                break

    total_time = time.time() - start_time
    print(f"\nProcessed {tick_count} ticks in {total_time:.2f}s")
    print(f"Average time per tick: {total_time / tick_count * 1000:.1f}ms")


def simulate_replay_at_original_speed():
    """Replay historical data at original timestamp intervals"""

    tardis = get_dataset("tardis")
    times = ["241201.000000", "241201.001000"]  # 10 seconds of data
    product = "BTCUSDT"

    print("\n=== Original Speed Replay ===")
    print("Replaying data at original timestamp intervals\n")

    prev_timestamp = None
    tick_count = 0
    start_time = time.time()

    for batch in tardis.stream_book(product, times, depth=1, batch_size=1):
        tick_count += 1

        if batch.height > 0:
            # Get current timestamp (nanoseconds)
            current_ts = batch.select(pl.col("ts")).item()

            if prev_timestamp is not None:
                # Calculate time difference in seconds
                time_diff = (current_ts - prev_timestamp) / 1_000_000_000

                # Sleep for the actual time difference (scaled down for demo)
                scaled_sleep = min(time_diff * 0.001, 0.1)  # Scale by 0.1% for demo
                time.sleep(scaled_sleep)

            prev_timestamp = current_ts

            mid_price = (
                (
                    batch.select(pl.col("bids[0].price"))
                    + batch.select(pl.col("asks[0].price"))
                )
                / 2
            ).item()

            # Show timestamp
            ts_ms = current_ts // 1_000_000
            ts_readable = (
                pl.from_epoch([ts_ms], time_unit="ms")
                .dt.strftime("%H:%M:%S.%3f")
                .item()
            )

            print(f"[{ts_readable}] Tick {tick_count}: Price={mid_price:.2f}")

            if tick_count >= 20:
                break

    total_time = time.time() - start_time
    print(f"\nReplayed {tick_count} ticks in {total_time:.2f}s")


def simulate_fixed_frequency_sampling():
    """Sample data at fixed intervals (every second/minute)"""

    tardis = get_dataset("tardis")
    times = ["241201.000000", "241201.003000"]  # 30 seconds of data
    product = "BTCUSDT"

    print("\n=== Fixed Frequency Sampling ===")
    print("Sampling every 1 second from streaming data\n")

    sample_interval = 1.0  # seconds
    last_sample_time = time.time()
    current_batch = None
    sample_count = 0

    for batch in tardis.stream_book(product, times, depth=1, batch_size=1):
        # Always keep the latest batch
        current_batch = batch

        # Check if it's time to sample
        current_time = time.time()
        if current_time - last_sample_time >= sample_interval:
            sample_count += 1

            if current_batch is not None and current_batch.height > 0:
                mid_price = (
                    (
                        current_batch.select(pl.col("bids[0].price"))
                        + current_batch.select(pl.col("asks[0].price"))
                    )
                    / 2
                ).item()

                bid_size = current_batch.select(pl.col("bids[0].amount")).item()
                ask_size = current_batch.select(pl.col("asks[0].amount")).item()

                print(
                    f"Sample {sample_count}: "
                    f"Price={mid_price:.2f} "
                    f"BidSize={bid_size:.2f} "
                    f"AskSize={ask_size:.2f}"
                )

                last_sample_time = current_time

            if sample_count >= 10:
                break

        # Small sleep to avoid busy waiting
        time.sleep(0.001)

    print(f"\nCollected {sample_count} samples")


def simulate_burst_then_wait():
    """Process data in bursts with waiting periods"""

    tardis = get_dataset("tardis")
    times = ["241201.000000", "241201.002000"]  # 20 seconds of data
    product = "BTCUSDT"

    print("\n=== Burst Processing ===")
    print("Processing 10 ticks, then waiting 2 seconds\n")

    batch_size = 10
    wait_time = 2.0  # seconds
    tick_count = 0
    burst_count = 0

    batch_buffer = []

    for batch in tardis.stream_book(product, times, depth=1, batch_size=1):
        tick_count += 1
        batch_buffer.append(batch)

        # Process in bursts
        if len(batch_buffer) >= batch_size:
            burst_count += 1
            print(f"Burst {burst_count}: Processing {len(batch_buffer)} ticks...")

            # Process the batch
            for i, tick_batch in enumerate(batch_buffer):
                if tick_batch.height > 0:
                    mid_price = (
                        (
                            tick_batch.select(pl.col("bids[0].price"))
                            + tick_batch.select(pl.col("asks[0].price"))
                        )
                        / 2
                    ).item()
                    print(
                        f"  Tick {tick_count - batch_size + i + 1}: Price={mid_price:.2f}"
                    )

            # Clear buffer and wait
            batch_buffer = []
            print(f"  Waiting {wait_time} seconds...")
            time.sleep(wait_time)

            if burst_count >= 3:
                break

    print(f"\nProcessed {tick_count} ticks in {burst_count} bursts")


if __name__ == "__main__":
    simulate_with_processing_delay()
    simulate_replay_at_original_speed()
    simulate_fixed_frequency_sampling()
    simulate_burst_then_wait()
