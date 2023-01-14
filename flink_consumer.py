from pyflink.common import Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.csv import CsvRowDeserializationSchema
import data_getter as dg
from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, FileSource, StreamFormat, BulkFormat
import time


class FlinkConsumer:

    def __init__(self, topic=dg.DataGetter.topic, moving_avg_count_limit=10, kind='kafka', file_name=""):
        if kind not in ['kafka', 'file', 'batch']:
            raise ValueError

        if kind != 'kafka' and file_name == "":
            raise ValueError

        self.kind = kind
        self.file_name = file_name
        self.moving_avg_count_limit = moving_avg_count_limit
        self.env = StreamExecutionEnvironment.get_execution_environment()
        if self.kind == 'batch':
            mode = RuntimeExecutionMode.BATCH
        else:
            mode = RuntimeExecutionMode.STREAMING

        self.env.set_runtime_mode(mode)
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

        if self.kind == 'kafka':
            kafka_consumer = FlinkKafkaConsumer(
                topics=topic,
                deserialization_schema=deserialization_schema,
                properties={'bootstrap.servers': '127.0.0.1:9092', 'group.id': 'test'})

            kafka_consumer.set_start_from_earliest()
            self.ds = self.env.add_source(kafka_consumer)
        elif self.kind == 'file' or self.kind == 'batch':
            self.ds = self.env.from_source(
                source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                           self.file_name)
                .process_static_file_set().build(),
                watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
                source_name="file_source"
            )

    def execute_counting(self):
        moving_avg_count_limit = self.moving_avg_count_limit

        def important_info(line):
            category = f"{line[1]}-{line[2]}"
            moving_avg_init = tuple([-1 for _ in range(moving_avg_count_limit)])
            # category,       rate,      date, idx,   components,  mov_avg
            return category, line[3], line[5], 1, moving_avg_init, -1, time.perf_counter()

        def important_info_file(line):
            str_line = important_info(line.split(','))
            return str_line[0], float(str_line[1]), str_line[2], int(str_line[3]), str_line[4],\
                float(str_line[5]), float(str_line[6])

        if self.kind == 'kafka':
            map_function = important_info
        else:
            map_function = important_info_file

        tuple_components = [Types.FLOAT() for _ in range(moving_avg_count_limit)]
        self.ds = self.ds \
            .map(map_function, output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(),
                                                        Types.STRING(), Types.INT(),
                                                        Types.TUPLE(tuple_components), Types.FLOAT(),
                                                        Types.FLOAT()])) \
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
        self.env.execute()


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
    return category, new_mean, date, new_delimiter, \
        tuple(moving_averages_components), moving_average, time.perf_counter()


if __name__ == '__main__':
    consumer = FlinkConsumer(kind='batch', file_name='coinbase.data')
    start = time.perf_counter()
    consumer.execute_counting()
    print(time.perf_counter() - start)
