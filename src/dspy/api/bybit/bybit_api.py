"""
This module provides a simple interface to the pybit library functions.
"""
import time
import logging
import pybit.unified_trading as bb

# Local imports
from dspy.api.bybit.config import Config
from dspy.api.api_registry import register_api
from dspy.api.base import Exchange

logger = logging.getLogger('DS.exchanges')

@register_api('bybit')
class ByBitManager(Exchange):
    """
    Simple interface for interacting with ByBit.
    """
    
    def __init__(self, config: Config):
        """
        Set up a HTTP connector.

        Arguments:
            config.api_key -- the public API key
            config.api_secret -- the private API key
        """
        self.s = bb.HTTP(
            api_key=config.api_key,
            api_secret=config.api_secret,
            logging_level=logging.INFO
            )

    # Market
    def get_mid(self, symbol: str) -> float:
        """
        Return best mid price.

        Arguments:
            symbol -- the product symbol
        """
        last_price = self.s.get_tickers(
            category='linear', 
            symbol=symbol
            )['result']['list'][0]['lastPrice']
        return float(last_price)
    
    def get_ask(self, symbol: str) -> list[float]:
        """
        Return best ask price and volume.

        Arguments:
            symbol -- the product symbol
        """
        ask = self.s.get_orderbook(
            category="linear",
            symbol=symbol,
            limit=1
            )['result']['a'][0]
        return [float(ask[0]), float(ask[1])]
    
    def get_bid(self, symbol: str) -> list[float]:
        """
        Return best bid price and volume.

        Arguments:
            symbol -- the product symbol
        """
        bid = self.s.get_orderbook(
            category="linear",
            symbol=symbol,
            limit=1
            )['result']['b'][0]
        return [float(bid[0]), float(bid[1])]
    
    def get_orderbook(self, symbol: str, depth: int = 25) -> list[float]:
        """
        Return orderbook for product.

        Arguments:
            symbol -- the product symbol
            depth -- the depth of the orderbook
        """
        orderbook = self.s.get_orderbook(
            category="linear",
            symbol=symbol,
            limit=depth
            )['result']
        
        return orderbook

    # Account info
    def get_wallet_balance(self) -> float:
        """
        Return wallet balance.

        Arguments:
            symbol -- the product symbol
        """
        wallet_balance = self.s.get_wallet_balance(
            accountType="UNIFIED"
            )['result']['list'][0]['totalAvailableBalance']
        return float(wallet_balance)
    
    def get_fees(self, symbol: str) -> list:
        """Return taker and maker fees for product."""
        fees = self.s.get_fee_rates(
            symbol=symbol
        )['result']['list'][0]
        return [float(fees['takerFeeRate']), float(fees['makerFeeRate'])]

    # Position info
    def get_position(self, symbol: str) -> dict:
        """Return positions in products specified by symbol."""
        p = {}
        pos = self.s.get_positions(
            category='linear', 
            symbol=symbol,
            )['result']['list'][0]
        sign = 1 if pos['side'] == 'Buy' else -1
        if pos['size'] != '0':
            p = {
                'size': sign*float(pos['size']),
                'aep': float(pos['avgPrice']),
                'mark_price': float(pos['markPrice']),
                'value': float(pos['positionValue']),
                'leverage': float(pos['leverage']),
                'position_balance': float(pos['positionBalance']),
                'unrealized_pnl': float(pos['unrealisedPnl']),
                'realized_pnl': float(pos['curRealisedPnl'])
            }
        else:
            p = {
                'size': 0,
                'aep': 0,
                'mark_price': float(pos['markPrice']),
                'value': 0,
                'leverage': float(pos['leverage']),
                'position_balance': 0,
                'unrealized_pnl': 0,
                'realized_pnl': float(pos['curRealisedPnl'])
            }
        return p

    # Trading
    def place_order(
            self, 
            symbol: str, 
            qty: float, 
            price: float | None = None, 
            type: str = 'Market'
            ) -> dict:
        """Place limit order at given price or market order."""
        if price is None:
            price = 0.
        response = self.s.place_order(
            category = 'linear',
            symbol = symbol,
            side = 'Sell' if qty < 0 else 'Buy',
            orderType = type,
            qty = str(abs(qty)),
            price = str(price), 
            timeInForce ='GTC'
            )
        order_id = response['result']['orderId']
        resp = {
            'order_id': order_id,
            'ret_code': response['retCode'],
            'time': response['time']
            }
        return resp

    def replace_order(
            self, 
            symbol: str, 
            order_id: float, 
            qty: float, 
            price: float
            ) -> dict:
        """Cancel specified limit order and place a new one."""
        self.s.cancel_order(
            category = 'linear',
            symbol = symbol,
            orderId = order_id
            )
        response = self.place_order(
            symbol = symbol,
            qty = qty,
            price = price,
            type ='Limit'
            )
        return response
    
    def place_batch_order(self, symbol: str, qtys: list, prices: list) -> dict:
        """Place a collection of orders for a given product."""
        assert len(qtys) == len(prices)
        orders = [
            {'category': 'linear',
             'symbol': symbol,
             'orderType': 'Limit',
             'side': 'Buy' if qtys[k] > 0 else 'Sell',
             'qty': str(abs(qtys[k])),
             'price': str(int(prices[k])),
             'timeInForce': 'GTC'} 
             for k in range(len(qtys))
            ]
        response = self.s.place_batch_order(category='linear', request=orders)
        resp = [i['orderId'] for i in response['result']['list']]
        return resp
    
    def cancel_order(self, symbol: str, order_id: str):
        """Cancel specific limit order based on order id."""
        resp = self.s.cancel_order(
            category = 'linear',
            symbol = symbol,
            orderId = order_id
            )
        return resp['retCode']
    
    def cancel_batch_order(self, symbol: str, order_ids: list) -> dict:
        """Cancel a list of orders for one product based on order ids."""
        to_cancel = [{'category': 'linear', 
                      'symbol': symbol,
                      'order_id': i} 
                      for i in order_ids
                      ]
        resp = self.s.cancel_batch_order(category='linear', request=to_cancel)
        return resp['retCode']
    
    def cancel_all_orders(self, symbol: str) -> dict:
        """Cancel all outstanding orders for a set of products."""
        resp = self.s.cancel_all_orders(
            category='linear', 
            symbol=symbol)['retCode']
        return resp

    def close_positions(self, symbols):
        """Close positions for a list of products."""
        p = self.get_positions(symbols=symbols)
        resp = {}
        for s in symbols:
            resp[s] = self.s.place_order(
                category = 'linear',
                symbol = s,
                side = 'Sell' if p[s]['side'] == 'Buy' else 'Buy',
                orderType ='Market',
                qty = p[s]['size']
                )['retCode']
        return resp

    def set_trading_stop(self, symbol: str, stop_price: int) -> dict:
        """Set stop loss."""
        resp = self.s.set_trading_stop(
            category = 'linear',
            symbol = symbol,
            stopLoss = str(stop_price)
        )
        return resp

    # Various helper and dummy methods
    def wait(self, timeout: float):
        """Wait for an indicated amount of time."""
        time.sleep(timeout)

    def next(self):
        """For compatibility with simulator."""
        return True
