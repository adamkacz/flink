from data_getter import DataGetter
from threading import Thread
from consumer import Consumer
from flink_stream_consumer import FlinkStreamConsumer


def main():
    dg = DataGetter()
    # fc = Consumer()
    # fc_thread = Thread(target=fc.consume)
    # fc_thread.start()
    kafka_consumer = FlinkStreamConsumer()
    # kafka_consumer.execute_counting()
    kc_thread = Thread(target=kafka_consumer.execute_counting)
    kc_thread.start()
    dg.start()


if __name__ == "__main__":
    main()
