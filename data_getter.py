import websocket
import _thread
import time
import rel
import json
from kafka import KafkaProducer

DATA = {
    "type": "subscribe",
    "product_ids": [
        "ETH-USD",
        "ETH-EUR"
    ],
    "channels": [
        "level2",
        "heartbeat",
        {
            "name": "ticker",
            "product_ids": [
                "ETH-BTC",
                "ETH-USD"
            ]
        }
    ]
}

#PRODUCER = KafkaProducer(bootstrap_servers='localhost:1234')


def on_message(ws, message):
    print(type(message))
    print(message)


def on_error(ws, error):
    print(error)


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    print("Opened connection")


class DataGetter:
    def __init__(self):
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp("wss://ws-feed.exchange.coinbase.com",
                                         on_open=on_open,
                                         on_message=on_message,
                                         on_error=on_error,
                                         on_close=on_close)

    def start(self):
        self.ws.run_forever(dispatcher=rel, reconnect=4)
        self.ws.send(json.dumps(DATA))
        rel.signal(2, rel.abort)
        rel.dispatch()
