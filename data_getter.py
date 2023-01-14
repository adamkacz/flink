import websocket
import time
import rel
import json
from kafka import KafkaProducer
import json

DATA = {
    "type": "subscribe",
    "product_ids": [
        "ETH-USD",
        "ETH-EUR"
    ],
    "channels": [
        "level2",
        # "heartbeat",
        # {
        #     "name": "ticker",
        #     "product_ids": [
        #         "ETH-BTC",
        #         "ETH-USD"
        #     ]
        # }
    ]
}


class DataGetter:
    topic = 'coinbase'

    def __init__(self, purpose='kafka'):
        if purpose == 'kafka':
            on_message = self.on_message_kafka
        elif purpose == 'classic':
            on_message = self.on_message
        elif purpose == 'file':
            on_message = self.on_message_file
        else:
            raise ValueError

        self.kafka = KafkaProducer(bootstrap_servers='127.0.0.1:9092') if purpose == 'kafka' else None

        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp("wss://ws-feed.exchange.coinbase.com",
                                         on_open=self.on_open,
                                         on_message=on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)

    def start(self):
        self.ws.run_forever(dispatcher=rel, reconnect=4)
        self.ws.send(json.dumps(DATA))
        rel.signal(2, rel.abort)
        rel.dispatch()

    def on_message(self, ws, message):
        print(message)

    def on_message_kafka(self, ws, message):
        message_dict = json.loads(message)
        #print(message_dict)
        if 'type' not in message_dict or message_dict['type'] != 'l2update':
            return

        for change in message_dict["changes"]:
            # new_dict = {
            #     "type": message_dict["type"],
            #     "product_id": message_dict["product_id"],
            #     "rate_type": change[0],
            #     "rate_value": float(change[1]),
            #     "rate_change": float(change[2]),
            #     "time": message_dict['time'].replace('T', " ").replace('Z', "")
            # }
            # # print(json.dumps(new_dict))
            # self.kafka.send(DataGetter.topic, value=json.dumps(new_dict).encode('utf-8'))
            field_list = [
                message_dict["type"],
                message_dict["product_id"],
                change[0],
                change[1],
                change[2],
                message_dict['time'].replace('T', " ").replace('Z', "")
            ]
            self.kafka.send(DataGetter.topic, value=",".join(field_list).encode('utf-8'))

    def on_message_file(self, ws, message):
        with open("coinbase.data", "a+") as file:
            file.write(message + '\n')

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###")

    def on_open(self, ws):
        print("Opened connection")


def main(purpose='kafka'):
    dg = DataGetter(purpose)
    dg.start()


if __name__ == '__main__':
    main()
