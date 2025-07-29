"""
Example demonstrating streaming functionality for Tardis data.
"""

import polars as pl
from dspy.hdb.registry import get_dataset


def stream_book_example():
    """Example of streaming book data in batches."""

    # Initialize the Tardis data loader
    tardis = get_dataset("tardis")

    # Define time range and product
    times = ["241201.000000", "241201.010000"]  # 1 minute of data
    product = "BTCUSDT"

    print("Streaming book data in batches...")

    # Stream book data with small batch size for demonstration
    batch_count = 0
    total_rows = 0

    try:
        for batch in tardis.stream_book(product, times, depth=5, batch_size=1000):
            batch_count += 1
            batch_rows = batch.height
            total_rows += batch_rows

            print(f"Batch {batch_count}: {batch_rows} rows")

            # Show first few rows of first batch
            if batch_count == 1:
                print("First batch preview:")
                print(batch.head(3))
                print("Columns:", batch.columns)

            # Demonstrate feature engineering on streaming data
            if batch_rows > 0:
                # Calculate mid price for the batch
                mid_price = (
                    batch.select(pl.col("bids[0].price"))
                    + batch.select(pl.col("asks[0].price"))
                ) / 2
                print(
                    f"  Mid price range: {mid_price.min().item():.2f} - {mid_price.max().item():.2f}"
                )

            # Limit output for demo
            if batch_count >= 5:
                break

    except Exception as e:
        print(f"Error during streaming: {e}")
        return

    print(f"\nTotal: {batch_count} batches, {total_rows} rows processed")


def stream_trades_example():
    """Example of streaming trade data in batches."""

    # Initialize the Tardis data loader
    tardis = get_dataset("tardis")

    # Define time range and product
    times = ["241201.000000", "241201.010000"]  # 1 minute of data
    product = "BTCUSDT"

    print("\nStreaming trade data in batches...")

    batch_count = 0
    total_rows = 0

    try:
        for batch in tardis.stream_trades(product, times, batch_size=500):
            batch_count += 1
            batch_rows = batch.height
            total_rows += batch_rows

            print(f"Batch {batch_count}: {batch_rows} rows")

            # Show first few rows of first batch
            if batch_count == 1:
                print("First batch preview:")
                print(batch.head(3))
                print("Columns:", batch.columns)

            # Limit output for demo
            if batch_count >= 3:
                break

    except Exception as e:
        print(f"Error during streaming: {e}")
        return

    print(f"\nTotal: {batch_count} batches, {total_rows} rows processed")


if __name__ == "__main__":
    print("=== Streaming Data Example ===")
    stream_book_example()
    stream_trades_example()
    print("\n=== Streaming Complete ===")
