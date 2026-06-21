import json
import logging
import signal
import sys
import time
from urllib.parse import urlencode

import websocket

# websocket.enableTrace(True)
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)

ALL_SYMBOLS = [
    "ETHUSDT",
    "SOLUSDT",
    "TRXUSDT",
    "DOGEUSDT",
    "XRPUSDT",
    "LTCUSDT",
    "SUIUSDT",
    "ZKUSDT",
    "AAVEUSDT",
    "AVAXUSDT",
    "ZECUSDT",
    "1000PEPEUSDT",
    "OPUSDT",
    "ADAUSDT",
    "LINKUSDT",
    "UNIUSDT",
    "TONUSDT",
]


def on_message(ws, message):
    try:
        data = json.loads(message)
        logger.info(json.dumps(data, indent=2))
    except Exception:
        logger.info(message)


def on_error(ws, error):
    logger.error(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    logger.info("Connection closed")


def on_open(ws):
    logger.info("Connected, waiting for messages...")
    logger.info("Press Ctrl+C to exit")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="ws://154.86.24.27:8081")
    args = parser.parse_args()
    period = "1m"

    suffix = f"@volatility_{period}"
    streams = "BTCUSDT" + suffix
    for symbol in ALL_SYMBOLS:
        streams += "/" + symbol + suffix

    url = f"{args.server}/stream?{urlencode({'streams': streams})}"
    logger.info(f"Connecting to {url}")

    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    # 信号处理
    def signal_handler(sig, frame):
        logger.info("\nInterrupt received, closing connection...")
        ws.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    while True:
        ws.run_forever()
        time.sleep(10)
