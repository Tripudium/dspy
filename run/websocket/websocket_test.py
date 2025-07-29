from time import sleep

from pybit.unified_trading import WebSocket


def handle_message(message):
    print(message)

def main():
    ws = WebSocket(
        testnet=True,
        channel_type="linear",
    )
    ws.orderbook_stream(50, "BTCUSDT", handle_message)

    while True:
        sleep(1)

if __name__ == "__main__":
    main()