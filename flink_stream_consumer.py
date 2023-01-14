from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.csv import CsvRowDeserializationSchema
import data_getter as dg
from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig


class FlinkStreamConsumer:

    def __init__(self, topic=dg.DataGetter.topic, moving_avg_count_limit=10, kind='kafka'):
        self.kind = kind
        self.moving_avg_count_limit = moving_avg_count_limit
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        # write all the data to one file
        self.env.set_parallelism(1)
        # self.env.add_jars(
        #     "file:///home/adamkaczmarczyk/.local/lib/python3.9/site-packages/pyflink/lib/flink-sql-connector-kafka-1.16.0.jar")
        self.env.add_jars("file:///home/adamkaczmarczyk/PycharmProjects/flink/flink-sql-connector-kafka-1.16.0.jar")
        # self.env.add_jars("file://flink-sql-connector-kafka-1.16.0.jar")

        deserialization_schema = CsvRowDeserializationSchema \
            .Builder(
                type_info=Types.ROW([Types.STRING(),
                                     Types.STRING(),
                                     Types.STRING(),
                                     Types.FLOAT(),
                                     Types.FLOAT(),
                                     Types.STRING()])
            ).build()

        kafka_consumer = FlinkKafkaConsumer(
            topics=topic,
            deserialization_schema=deserialization_schema,
            properties={'bootstrap.servers': '127.0.0.1:9092', 'group.id': 'test'})

        kafka_consumer.set_start_from_earliest()

        self.ds = self.env.add_source(kafka_consumer)

    def execute_counting(self):
        moving_avg_count_limit = self.moving_avg_count_limit

        def important_info(line):
            category = f"{line[1]}-{line[2]}"
            moving_avg_init = tuple([-1 for _ in range(moving_avg_count_limit)])
            # category,       rate,      date, idx,   components,  mov_avg
            return category, line[3], line[5], 1, moving_avg_init, -1

        tuple_components = [Types.FLOAT() for _ in range(moving_avg_count_limit)]
        self.ds = self.ds \
            .map(important_info, output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(),
                                                          Types.STRING(), Types.INT(),
                                                          Types.TUPLE(tuple_components), Types.FLOAT()])) \
            .key_by(lambda i: i[0]) \
            .reduce(lambda i, j: current_mean(i[0], i[1], j[1], i[3], j[3], j[2], i[4]))

        output_path = './results'
        file_sink = FileSink \
            .for_row_format(output_path, Encoder.simple_string_encoder()) \
            .with_output_file_config(OutputFileConfig.builder().with_part_prefix(f'pre-{self.kind}')
                                     .with_part_suffix('suf').build()) \
            .build()
        self.ds.sink_to(file_sink)

        self.ds.print()
        self.env.execute('xd')


def current_mean(category, mean, new_obs, indexes_sum, new_ind,
                 date, moving_averages_components):
    moving_averages_components = list(moving_averages_components)
    new_delimiter = indexes_sum + new_ind
    new_mean = (mean * indexes_sum + new_obs) / new_delimiter
    moving_averages_components.append(new_obs)
    moving_averages_components.pop(0)
    moving_average = -1
    if -1 not in moving_averages_components:
        moving_average = sum(moving_averages_components) / len(moving_averages_components)

    # category,       rate,    date,    idx,             components,                    mov_avg
    return category, new_mean, date, new_delimiter, tuple(moving_averages_components), moving_average


if __name__ == '__main__':
    consumer = FlinkStreamConsumer()
    consumer.execute_counting()
