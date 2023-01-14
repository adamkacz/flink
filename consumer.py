from kafka import KafkaConsumer
import data_getter as dg
import time


class Consumer:

    def __init__(self, topic=dg.DataGetter.topic, bootstrap_servers='127.0.0.1:9092'):
        self.consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers)

    def consume(self):
        while True:
            message = self.consumer.poll(timeout_ms=0)
            #print(message)
            with open("messages.txt", "a+") as file:
                file.write(str(message) + "\n")

            time.sleep(1)


if __name__ == '__main__':
    consumer = Consumer()
    consumer.consume()
