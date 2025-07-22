"""
Example of using Bybit WebSocket private streams for account monitoring.

This script demonstrates how to:
1. Subscribe to private account streams (positions, orders, executions, wallet)
2. Monitor real-time account updates
3. Track position P&L and order status changes
4. Handle authentication for private streams

Note: Requires valid BYBIT_API_KEY and BYBIT_API_SECRET in environment variables.
"""

import asyncio
import os
import time
from collections import defaultdict
from datetime import datetime

import polars as pl

from dspy.api.bybit.websocket_stream import BybitWebSocketStream


class AccountMonitor:
    """Monitors account activity through private WebSocket streams."""
    
    def __init__(self):
        """Initialize account monitor."""
        self.positions = {}  # Current positions by symbol
        self.active_orders = {}  # Active orders by order_id
        self.recent_executions = []  # Recent fills
        self.wallet_balances = {}  # Wallet balances by coin
        
        # Statistics
        self.stats = defaultdict(int)
        self.start_time = time.time()
        
    def on_position(self, data):
        """Handle position updates."""
        symbol = data['symbol']
        self.positions[symbol] = data
        self.stats['position_updates'] += 1
        
        # Log significant position changes
        if data['size'] > 0:
            side = "LONG" if data['side'] == 1 else "SHORT"
            print(f"\n📊 Position Update: {symbol}")
            print(f"   Side: {side}, Size: {data['size']}")
            print(f"   Entry: ${data['entry_price']:.2f}, Mark: ${data['mark_price']:.2f}")
            print(f"   Unrealized P&L: ${data['unrealized_pnl']:.2f}")
            if data['liq_price']:
                print(f"   Liquidation Price: ${data['liq_price']:.2f}")
                
    def on_order(self, data):
        """Handle order updates."""
        order_id = data['order_id']
        status = data['order_status']
        
        # Track active orders
        if status in ['New', 'PartiallyFilled']:
            self.active_orders[order_id] = data
        elif order_id in self.active_orders:
            del self.active_orders[order_id]
            
        self.stats['order_updates'] += 1
        
        # Log order status changes
        side = "BUY" if data['side'] == 1 else "SELL"
        symbol = data['symbol']
        qty = data['qty']
        price = data['price']
        
        if status == 'New':
            print(f"\n📋 New Order: {symbol} {side} {qty} @ ${price:.2f}")
            print(f"   Order ID: {order_id}")
            print(f"   Type: {data['order_type']}")
            
        elif status == 'Filled':
            print(f"\n✅ Order Filled: {symbol} {side} {qty}")
            print(f"   Avg Price: ${data['cum_exec_value'] / data['cum_exec_qty']:.2f}")
            print(f"   Fees: ${data['cum_exec_fee']:.4f}")
            
        elif status == 'Cancelled':
            print(f"\n❌ Order Cancelled: {symbol} {side} {qty} @ ${price:.2f}")
            
        elif status == 'Rejected':
            print(f"\n🚫 Order Rejected: {symbol} {side} {qty} @ ${price:.2f}")
            
    def on_execution(self, data):
        """Handle execution/fill updates."""
        self.recent_executions.append(data)
        if len(self.recent_executions) > 100:
            self.recent_executions.pop(0)
            
        self.stats['executions'] += 1
        
        # Log executions
        side = "BUY" if data['side'] == 1 else "SELL"
        symbol = data['symbol']
        qty = data['qty']
        price = data['price']
        fee = data['exec_fee']
        
        print(f"\n💹 Execution: {symbol} {side} {qty} @ ${price:.2f}")
        print(f"   Exec ID: {data['exec_id']}")
        print(f"   Fee: ${fee:.4f} ({'Maker' if data['is_maker'] else 'Taker'})")
        print(f"   Time: {data['exec_time']}")
        
    def on_wallet(self, data):
        """Handle wallet balance updates."""
        coin = data['coin']
        self.wallet_balances[coin] = data
        self.stats['wallet_updates'] += 1
        
        # Log significant balance changes
        print(f"\n💰 Wallet Update: {coin}")
        print(f"   Balance: {data['wallet_balance']:.4f}")
        print(f"   Available: {data['available_balance']:.4f}")
        print(f"   Unrealized P&L: ${data['unrealized_pnl']:.2f}")
        print(f"   Total Realized P&L: ${data['cum_realized_pnl']:.2f}")
        
    def print_summary(self):
        """Print account summary."""
        elapsed = time.time() - self.start_time
        
        print("\n" + "="*60)
        print("ACCOUNT SUMMARY")
        print("="*60)
        
        # Active positions
        print("\n📊 Active Positions:")
        total_unrealized_pnl = 0
        for symbol, pos in self.positions.items():
            if pos['size'] > 0:
                side = "LONG" if pos['side'] == 1 else "SHORT"
                print(f"   {symbol}: {side} {pos['size']} @ ${pos['entry_price']:.2f}")
                print(f"      Unrealized P&L: ${pos['unrealized_pnl']:.2f}")
                total_unrealized_pnl += pos['unrealized_pnl']
        
        if not any(p['size'] > 0 for p in self.positions.values()):
            print("   No active positions")
        else:
            print(f"\n   Total Unrealized P&L: ${total_unrealized_pnl:.2f}")
            
        # Active orders
        print(f"\n📋 Active Orders: {len(self.active_orders)}")
        for order_id, order in list(self.active_orders.items())[:5]:
            side = "BUY" if order['side'] == 1 else "SELL"
            print(f"   {order['symbol']}: {side} {order['qty']} @ ${order['price']:.2f}")
            
        # Wallet balances
        print("\n💰 Wallet Balances:")
        for coin, wallet in self.wallet_balances.items():
            if wallet['wallet_balance'] > 0:
                print(f"   {coin}: {wallet['wallet_balance']:.4f}")
                print(f"      Available: {wallet['available_balance']:.4f}")
                
        # Statistics
        print(f"\n📈 Stream Statistics ({elapsed:.1f}s):")
        print(f"   Position updates: {self.stats['position_updates']}")
        print(f"   Order updates: {self.stats['order_updates']}")
        print(f"   Executions: {self.stats['executions']}")
        print(f"   Wallet updates: {self.stats['wallet_updates']}")
        
        # Recent executions summary
        if self.recent_executions:
            print(f"\n💹 Recent Executions: {len(self.recent_executions)}")
            
            # Calculate volume by side
            buy_volume = sum(e['qty'] for e in self.recent_executions if e['side'] == 1)
            sell_volume = sum(e['qty'] for e in self.recent_executions if e['side'] == -1)
            total_fees = sum(e['exec_fee'] for e in self.recent_executions)
            
            print(f"   Buy volume: {buy_volume:.4f}")
            print(f"   Sell volume: {sell_volume:.4f}")
            print(f"   Total fees: ${total_fees:.4f}")


async def main():
    """Main function to run the private stream monitoring."""
    # Check for API credentials
    if not os.getenv("BYBIT_API_KEY") or not os.getenv("BYBIT_API_SECRET"):
        print("❌ Error: BYBIT_API_KEY and BYBIT_API_SECRET environment variables required")
        print("Please set your API credentials to use private streams")
        return
        
    # Initialize account monitor
    monitor = AccountMonitor()
    
    # Create WebSocket stream with credentials
    stream = BybitWebSocketStream()
    
    print("🔐 Subscribing to private account streams...")
    
    # Subscribe to all private streams
    stream.subscribe_positions(callback=monitor.on_position)
    stream.subscribe_orders(callback=monitor.on_order)
    stream.subscribe_executions(callback=monitor.on_execution)
    stream.subscribe_wallet(callback=monitor.on_wallet)
    
    # Optional: Also subscribe to some public data for context
    # stream.subscribe_ticker("BTCUSDT")
    
    # Start streaming
    stream.start()
    print("\n✅ Private streams connected. Monitoring account activity...\n")
    print("Press Ctrl+C to stop and see summary.\n")
    
    try:
        # Monitor for a while
        while True:
            await asyncio.sleep(30)  # Print summary every 30 seconds
            monitor.print_summary()
            
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping monitor...")
    finally:
        # Final summary
        monitor.print_summary()
        
        # Clean up
        stream.stop()
        
        # Save recent executions for analysis
        if monitor.recent_executions:
            df = pl.DataFrame(monitor.recent_executions)
            df.write_parquet("recent_executions.parquet")
            print("\n💾 Recent executions saved to recent_executions.parquet")


if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())