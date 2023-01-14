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
        self.line_counter = 0
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
                                         on_open=DataGetter.on_open,
                                         on_message=on_message,
                                         on_error=DataGetter.on_error,
                                         on_close=DataGetter.on_close)

    def start(self):
        self.ws.run_forever(dispatcher=rel, reconnect=4)
        self.ws.send(json.dumps(DATA))
        rel.signal(2, rel.abort)
        rel.dispatch()

    @staticmethod
    def on_message(ws, message):
        print(message)

    def on_message_kafka(self, ws, message):
        if self.line_counter >= 741023:
            print('Data end')
            return

        for field_list in DataGetter.prepare_message(message):
            self.line_counter += 1
            self.kafka.send(DataGetter.topic, value=",".join(field_list).encode('utf-8'))

    @staticmethod
    def prepare_message(message):
        message_dict = json.loads(message)
        if 'type' not in message_dict or message_dict['type'] != 'l2update':
            return []

        changes = []
        for change in message_dict["changes"]:
            changes += [[
                message_dict["type"],
                message_dict["product_id"],
                change[0],
                change[1],
                change[2],
                message_dict['time'].replace('T', " ").replace('Z', "")
            ]]

        return changes

    @staticmethod
    def on_message_file(ws, message):
        with open("coinbase.data", "a+") as file:
            for field_list in DataGetter.prepare_message(message):
                file.write(",".join(field_list) + '\n')

    @staticmethod
    def on_error(ws, error):
        print(error)

    @staticmethod
    def on_close(ws, close_status_code, close_msg):
        print("### closed ###")

    @staticmethod
    def on_open(ws):
        print("Opened connection")


def main(purpose='kafka'):
    dg = DataGetter(purpose)
    dg.start()


if __name__ == '__main__':
    main('file')
    # 741023
    # with open("coinbase.data") as file:
    #     i = 0
    #     for line in file:
    #         i += 1
    #
    #     print(i)
