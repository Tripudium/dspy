"""
Example of using Bybit WebSocket streaming for real-time market data.

This script demonstrates how to:
1. Subscribe to multiple data streams (orderbook, trades, ticker)
2. Process real-time updates with custom handlers
3. Store streaming data in Polars DataFrames
4. Calculate derived metrics from streaming data

Note: For USDT perpetuals (like BTCUSDT), supported orderbook depths are: 1, 50, 200, 500
      Depth 25 is only available for options markets.
"""

import asyncio
import time
from collections import deque

import polars as pl

from dspy.api.bybit.websocket_stream import BybitWebSocketStream


class StreamDataCollector:
    """Collects streaming data into Polars DataFrames for analysis."""
    
    def __init__(self, max_rows=10000):
        """
        Initialize data collector.
        
        Args:
            max_rows: Maximum rows to keep in memory (FIFO)
        """
        self.max_rows = max_rows
        
        # Use deques for efficient append/pop operations
        self.orderbook_updates = deque(maxlen=max_rows)
        self.trades = deque(maxlen=max_rows)
        self.tickers = deque(maxlen=max_rows)
        
        # Track statistics
        self.stats = {
            "orderbook_count": 0,
            "trade_count": 0,
            "ticker_count": 0,
            "start_time": time.time(),
        }
        
    def on_orderbook(self, data):
        """Handle order book updates."""
        # Calculate mid price and spread
        best_bid = data['bids'][0][0] if data['bids'] else None
        best_ask = data['asks'][0][0] if data['asks'] else None
        
        if best_bid and best_ask:
            mid_price = (best_bid + best_ask) / 2
            spread = best_ask - best_bid
            spread_bps = (spread / mid_price) * 10000  # Basis points
        else:
            mid_price = spread = spread_bps = None
            
        # Add derived metrics to the update
        update = {
            "ts": data['timestamp'],
            "symbol": data['symbol'],
            "mid": mid_price,
            "spread": spread,
            "spread_bps": spread_bps,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bid_size": data['bids'][0][1] if data['bids'] else None,
            "ask_size": data['asks'][0][1] if data['asks'] else None,
            "update_id": data['update_id'],
        }
        
        self.orderbook_updates.append(update)
        self.stats["orderbook_count"] += 1
        
        # Print periodic updates
        if self.stats["orderbook_count"] % 100 == 0:
            self._print_stats()
            
    def on_trade(self, data):
        """Handle trade updates."""
        # Add timestamp in nanoseconds for consistency with historical data
        trade = {
            "ts": data['timestamp'],
            "symbol": data['symbol'],
            "price": data['price'],
            "vol": data['vol'],
            "side": data['side'],  # Already 1 or -1
            "trade_id": data['trade_id'],
        }
        
        self.trades.append(trade)
        self.stats["trade_count"] += 1
        
    def on_ticker(self, data):
        """Handle ticker updates."""
        ticker = {
            "ts": data['timestamp'],
            "symbol": data['symbol'],
            "last": data['last_price'],
            "bid": data['bid_price'],
            "ask": data['ask_price'],
            "bid_size": data['bid_size'],
            "ask_size": data['ask_size'],
            "volume_24h": data['volume_24h'],
            "change_24h_pct": data['price_24h_change'],
        }
        
        self.tickers.append(ticker)
        self.stats["ticker_count"] += 1
        
    def get_orderbook_df(self) -> pl.DataFrame:
        """Get order book updates as Polars DataFrame."""
        if not self.orderbook_updates:
            return pl.DataFrame()
        return pl.DataFrame(list(self.orderbook_updates))
        
    def get_trades_df(self) -> pl.DataFrame:
        """Get trades as Polars DataFrame."""
        if not self.trades:
            return pl.DataFrame()
        return pl.DataFrame(list(self.trades))
        
    def get_ticker_df(self) -> pl.DataFrame:
        """Get ticker updates as Polars DataFrame."""
        if not self.tickers:
            return pl.DataFrame()
        return pl.DataFrame(list(self.tickers))
        
    def _print_stats(self):
        """Print collection statistics."""
        elapsed = time.time() - self.stats["start_time"]
        print(f"\n--- Stream Statistics ({elapsed:.1f}s) ---")
        print(f"Order book updates: {self.stats['orderbook_count']}")
        print(f"Trades: {self.stats['trade_count']}")
        print(f"Ticker updates: {self.stats['ticker_count']}")
        
        # Show latest data if available
        if self.orderbook_updates:
            latest = self.orderbook_updates[-1]
            print(f"\nLatest {latest['symbol']} orderbook:")
            print(f"  Mid: {latest['mid']:.2f}")
            print(f"  Spread: {latest['spread_bps']:.2f} bps")
            
        if self.trades:
            # Calculate trade statistics
            trades_df = self.get_trades_df()
            if len(trades_df) > 0:
                buy_vol = trades_df.filter(pl.col("side") == 1)["vol"].sum()
                sell_vol = trades_df.filter(pl.col("side") == -1)["vol"].sum()
                print("\nRecent trade flow:")
                print(f"  Buy volume: {buy_vol:.4f}")
                print(f"  Sell volume: {sell_vol:.4f}")
                print(f"  Net flow: {buy_vol - sell_vol:.4f}")


async def main():
    """Main function to run the streaming example."""
    # Initialize data collector
    collector = StreamDataCollector(max_rows=10000)
    
    # Create WebSocket stream
    stream = BybitWebSocketStream()
    
    # Subscribe to multiple symbols and data types
    symbols = ["BTCUSDT", "ETHUSDT"]
    
    for symbol in symbols:
        print(f"Subscribing to {symbol} streams...")
        stream.subscribe_orderbook(symbol, depth=50, callback=collector.on_orderbook)
        stream.subscribe_trades(symbol, callback=collector.on_trade)
        stream.subscribe_ticker(symbol, callback=collector.on_ticker)
    
    # Start streaming
    stream.start()
    print("\nStreaming started. Press Ctrl+C to stop.\n")
    
    try:
        # Run for a while
        await asyncio.sleep(60)  # Stream for 60 seconds
        
        # Analyze collected data
        print("\n\n=== Final Analysis ===")
        
        # Order book analysis
        orderbook_df = collector.get_orderbook_df()
        if len(orderbook_df) > 0:
            print(f"\nOrder book data: {len(orderbook_df)} updates")
            
            # Group by symbol
            for symbol in symbols:
                symbol_data = orderbook_df.filter(pl.col("symbol") == symbol)
                if len(symbol_data) > 0:
                    print(f"\n{symbol} statistics:")
                    print(f"  Average mid price: {symbol_data['mid'].mean():.2f}")
                    print(f"  Average spread: {symbol_data['spread_bps'].mean():.2f} bps")
                    print(f"  Min spread: {symbol_data['spread_bps'].min():.2f} bps")
                    print(f"  Max spread: {symbol_data['spread_bps'].max():.2f} bps")
        
        # Trade analysis
        trades_df = collector.get_trades_df()
        if len(trades_df) > 0:
            print(f"\nTrade data: {len(trades_df)} trades")
            
            # VWAP calculation
            for symbol in symbols:
                symbol_trades = trades_df.filter(pl.col("symbol") == symbol)
                if len(symbol_trades) > 0:
                    vwap = (symbol_trades["price"] * symbol_trades["vol"]).sum() / symbol_trades["vol"].sum()
                    total_vol = symbol_trades["vol"].sum()
                    buy_ratio = symbol_trades.filter(pl.col("side") == 1)["vol"].sum() / total_vol
                    
                    print(f"\n{symbol} trade statistics:")
                    print(f"  VWAP: {vwap:.2f}")
                    print(f"  Total volume: {total_vol:.4f}")
                    print(f"  Buy ratio: {buy_ratio:.2%}")
        
    except KeyboardInterrupt:
        print("\n\nStopping stream...")
    finally:
        # Clean up
        stream.stop()
        
        # Save data to files for further analysis
        if len(collector.get_orderbook_df()) > 0:
            collector.get_orderbook_df().write_parquet("orderbook_stream.parquet")
            print("Order book data saved to orderbook_stream.parquet")
            
        if len(collector.get_trades_df()) > 0:
            collector.get_trades_df().write_parquet("trades_stream.parquet")
            print("Trade data saved to trades_stream.parquet")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())