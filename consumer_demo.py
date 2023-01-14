from kafka import KafkaConsumer
import data_getter as dg
import time


def main():
    consumer = KafkaConsumer(dg.DataGetter.topic, bootstrap_servers='127.0.0.1:9092')
    while True:
        try:
            print(consumer.poll(timeout_ms=0))
            time.sleep(1)
        except:
            pass


if __name__ == '__main__':
    main()

